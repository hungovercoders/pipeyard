"""The control plane: JSON API under /api and the HTMX UI, in one FastAPI app.

Every handler gets ``org_id`` from the ``current_org`` dependency (a verified token) and
nowhere else. No route takes an org in its path, query or body.
"""

import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path as FsPath
from typing import Annotated
from urllib.parse import parse_qs, quote

import httpx
from fastapi import Body, Depends, FastAPI, HTTPException, Path, Request, WebSocket
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import contract, datacontract, proxy, query
from .auth import COOKIE, Auth, AuthError
from .config import Settings
from .contract import ContractError, PipelineSpec, Report, ReportApp, RunRecord
from .runner import TIMEOUT_EXIT_CODE, Job, Runner, make_runner
from .storage import Storage, org_credentials

PipelineName = Annotated[str, Path(pattern=contract.PIPELINE_NAME)]
DatasetName = Annotated[str, Path(pattern=contract.DATASET_NAME)]
RunId = Annotated[str, Path(pattern=contract.RUN_ID_RE.pattern)]
SLUG = r"^[a-z0-9-]{1,80}$"
Slug = Annotated[str, Path(pattern=SLUG)]

STARTER_YAML = """\
name: my-pipeline
image: my-image:latest
cpu: 256
memory: 512
timeout_seconds: 600
outputs:
  - dataset: my_dataset
reports:
  - name: My dashboard
    image: my-dashboard:latest
    dataset: my_dataset
"""

FARGATE_REPORT_PORT = 8080
SWEEP_SECONDS = 15  # how often idle report apps are looked for
log = logging.getLogger("pipeyard")
ROUTE_CACHE_SECONDS = 15


def _answers(address: str, path: str = "/") -> bool:
    """True if an HTTP server answers at the address, with any status.

    A plain TCP connect is not enough: Docker accepts connections on a published port even
    when nothing inside the container is listening on it yet.
    """
    try:
        httpx.get(f"http://{address}{path}", timeout=1.5, trust_env=False)
        return True
    except httpx.HTTPError:
        return False


def create_app(settings: Settings | None = None, runner: Runner | None = None) -> FastAPI:
    settings = settings or Settings()
    runner = runner or make_runner(settings)
    storage = Storage(settings)
    auth = Auth(settings)
    templates = Jinja2Templates(directory=FsPath(__file__).parent / "templates")
    query.install_extensions()
    if not settings.is_aws:
        storage.ensure_bucket()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        async def sweeper() -> None:
            while True:
                await asyncio.sleep(SWEEP_SECONDS)
                try:
                    await run_in_threadpool(sweep_idle)
                except Exception:  # noqa: BLE001 - keep sweeping whatever one pass hits
                    log.exception("idle sweep failed")

        task = asyncio.create_task(sweeper())
        yield
        task.cancel()
        await http.aclose()

    app = FastAPI(
        title="pipeyard", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan
    )
    app.state.storage = storage
    # For the report proxy. trust_env=False: report apps are on private addresses, so
    # never send their traffic through an outbound proxy from the environment.
    http = httpx.AsyncClient(
        follow_redirects=False,
        trust_env=False,
        timeout=httpx.Timeout(60, connect=5),
        limits=httpx.Limits(max_keepalive_connections=0),  # no pooled state between requests
    )

    # --- auth -------------------------------------------------------------------------

    def current_org(request: Request) -> str:
        return org_from(request)

    def org_from(connection: Request | WebSocket) -> str:
        header = connection.headers.get("authorization", "")
        bearer = header.lower().startswith("bearer ")
        token = header[7:] if bearer else connection.cookies.get(COOKIE)
        if not token:
            raise HTTPException(401, "missing token")
        try:
            return auth.org_id(token)
        except AuthError as e:
            raise HTTPException(401, str(e)) from e

    Org = Annotated[str, Depends(current_org)]

    @app.exception_handler(HTTPException)
    def on_http_error(request: Request, exc: HTTPException):
        if request.url.path.startswith("/api/"):
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        if exc.status_code == 401:
            return RedirectResponse(f"/login?next={quote(request.url.path)}", status_code=303)
        return templates.TemplateResponse(
            request,
            "error.html",
            {"status": exc.status_code, "detail": exc.detail},
            status_code=exc.status_code,
        )

    def _signed_in(token: str, next_path: str) -> RedirectResponse:
        target = next_path if next_path.startswith("/") and not next_path.startswith("//") else "/"
        response = RedirectResponse(target, status_code=303)
        response.set_cookie(
            COOKIE,
            token,
            httponly=True,
            samesite="lax",
            secure=settings.secure_cookies,
            max_age=3600 if settings.is_aws else 12 * 3600,
        )
        return response

    @app.get("/login")
    def login_page(request: Request, next: str = "/"):
        if not settings.is_aws:  # dev stub: sign straight in as the dev org
            return _signed_in(auth.dev_token(), next)
        return templates.TemplateResponse(request, "login.html", {"next": next, "error": None})

    @app.post("/login")
    def login(request: Request, body: Annotated[bytes, Body()], next: str = "/"):
        if not settings.is_aws:
            raise HTTPException(404, "not found")
        form = parse_qs(body.decode())
        try:
            token = auth.cognito_login(form.get("username", [""])[0], form.get("password", [""])[0])
            auth.org_id(token)
        except AuthError as e:
            return templates.TemplateResponse(
                request, "login.html", {"next": next, "error": str(e)}, status_code=401
            )
        return _signed_in(token, next)

    @app.get("/logout")
    def logout():
        response = RedirectResponse("/login" if settings.is_aws else "/", status_code=303)
        response.delete_cookie(COOKIE)
        return response

    @app.post("/dev/token")
    def dev_token():
        """Local stub: a token for the dev org. Does not exist in aws mode."""
        if settings.is_aws:
            raise HTTPException(404, "not found")
        return {"token": auth.dev_token(), "org_id": settings.dev_org_id}

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    # --- operations (shared by API and UI) --------------------------------------------

    def save_contract(org_id: str, dataset: str, text: str) -> datacontract.DataContract:
        try:
            dc = datacontract.parse(text, dataset)
        except ContractError as e:
            raise HTTPException(422, e.errors) from e
        storage.put_data_contract(org_id, dataset, text)
        return dc

    def get_pipeline(org_id: str, name: str) -> PipelineSpec:
        spec = storage.get_pipeline(org_id, name)
        if spec is None:
            raise HTTPException(404, f"pipeline '{name}' not found")
        return spec

    def load_data_contract(org_id: str, dataset: str) -> datacontract.DataContract | None:
        text = storage.get_data_contract(org_id, dataset)
        return datacontract.parse(text, dataset) if text is not None else None

    def contract_problem(run: RunRecord) -> str | None:
        """If the run's dataset has a data contract, check what the run wrote against it."""
        dc = load_data_contract(run.org_id, run.dataset)
        if dc is None:
            return None
        run.contract_version = dc.version
        required = [p.column for p in dc.table(run.dataset).properties if p.required]
        try:
            columns, nulls = query.describe_output(
                settings, run.org_id, run.dataset, run.run_id, required
            )
        except query.duckdb.Error as e:
            return f"could not read the run's Parquet files: {str(e).splitlines()[0]}"
        problems = datacontract.violations(dc, run.dataset, columns, nulls)
        if not problems:
            return None
        return f"data contract {dc.label} is not met:\n" + "\n".join(f"- {p}" for p in problems)

    def reconcile(run: RunRecord) -> RunRecord:
        """If a run is still marked running, ask the runner and settle the record."""
        if run.status != "running" or run.handle is None:
            return run
        state = runner.poll(run.handle)
        if state.running:
            return run
        if state.gone:
            # Another request may have settled this run and cleaned up in the meantime.
            latest = storage.get_run(run.org_id, run.run_id)
            if latest is not None and latest.status != "running":
                return latest
        error = None
        if state.exit_code == TIMEOUT_EXIT_CODE:
            error = f"timed out after {run.timeout_seconds}s"
        elif state.gone or state.exit_code == 0:
            # Exit 0, or the runner has forgotten the container (ECS drops stopped tasks
            # after about an hour): either way the output in S3 decides.
            ended = state.detail if state.gone else "container exited 0"
            text = storage.get_manifest_text(run.org_id, run.dataset, run.run_id)
            if text is None:
                error = f"{ended} but wrote no {contract.MANIFEST_FILE}"
            elif not storage.has_parquet(run.org_id, run.dataset, run.run_id):
                error = f"{ended} but wrote no .parquet files"
            else:
                try:
                    manifest = contract.parse_manifest(text, dataset=run.dataset, run_id=run.run_id)
                    run.row_count = manifest.row_count
                    error = contract_problem(run)
                except ContractError as e:
                    error = str(e)
        elif state.exit_code is None:
            error = f"container did not run: {state.detail or 'unknown reason'}"
        else:
            error = f"container exited with code {state.exit_code}"
            if state.detail:
                error += f"\n{state.detail}"
        run.status = "failed" if error else "succeeded"
        run.error = error
        run.finished_at = datetime.now(UTC)
        storage.put_run(run)
        if error:
            # Report apps read the dataset's files directly, so a failed run must not leave
            # any behind: after this, a run= directory with a manifest is always a good one.
            storage.delete_output(run.org_id, run.dataset, run.run_id)
        runner.cleanup(run.handle)  # only after the outcome is saved
        return run

    def contract_view(dataset: str, dc: datacontract.DataContract) -> dict:
        return {
            "dataset": dataset,
            "id": dc.id,
            "name": dc.name or dc.id,
            "version": dc.version,
            "status": dc.status,
            "api_version": dc.apiVersion,
            "purpose": dc.purpose,
            "properties": [
                {
                    "name": p.column,
                    "type": p.physicalType or p.logicalType or "",
                    "logical_type": p.logicalType,
                    "required": p.required,
                    "description": p.description,
                }
                for p in dc.table(dataset).properties
            ],
        }

    def list_datasets(org_id: str) -> list[dict]:
        """Datasets with succeeded runs, plus any that so far only have a contract."""
        listed = {d["dataset"]: d for d in query.list_datasets(settings, org_id, list_runs(org_id))}
        for name in storage.list_data_contracts(org_id):
            entry = listed.setdefault(
                name,
                {"dataset": name, "runs": 0, "row_count": 0, "schema": [], "updated_at": None},
            )
            try:
                entry["contract"] = contract_view(name, load_data_contract(org_id, name))
            except ContractError:
                continue  # stored before a rule tightened: show the dataset without it
        for entry in listed.values():
            entry.setdefault("contract", None)
        return sorted(listed.values(), key=lambda d: d["dataset"])

    def list_runs(org_id: str, pipeline: str | None = None) -> list[dict]:
        runs = query.list_runs(settings, org_id, pipeline)
        for i, r in enumerate(runs):
            if r["status"] == "running":
                record = storage.get_run(org_id, r["run_id"])
                if record is not None:
                    settled = reconcile(record).model_dump()
                    runs[i] = {k: settled[k] for k in r}
        return runs

    def container_env(org_id: str, *, read_only: bool = False) -> dict[str, str]:
        """What every container gets: its org and S3 credentials scoped to that org."""
        creds = org_credentials(settings, org_id, fresh=True, read_only=read_only)
        env = {
            "ORG_ID": org_id,
            "AWS_REGION": settings.region,
            "AWS_DEFAULT_REGION": settings.region,
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
        }
        if creds.session_token:
            env["AWS_SESSION_TOKEN"] = creds.session_token
        if not settings.is_aws:
            env["AWS_ENDPOINT_URL"] = settings.runner_s3_endpoint_url
        return env

    def trigger(org_id: str, spec: PipelineSpec) -> RunRecord:
        run_id = contract.new_run_id()
        dataset = spec.output_dataset
        output_uri = storage.uri(contract.output_prefix(org_id, dataset, run_id))
        env = {"RUN_ID": run_id, "OUTPUT_URI": output_uri, **container_env(org_id)}
        if spec.inputs:
            env["INPUT_URI"] = storage.uri(contract.dataset_prefix(org_id, spec.inputs[0].dataset))
        run = RunRecord(
            run_id=run_id,
            org_id=org_id,
            pipeline=spec.name,
            dataset=dataset,
            status="running",
            started_at=datetime.now(UTC),
            timeout_seconds=spec.timeout_seconds,
            output_uri=output_uri,
        )
        try:
            job = Job(spec.name, spec.image, spec.cpu, spec.memory, spec.timeout_seconds)
            run.handle = runner.start(job, org_id, run_id, env)
        except Exception as e:  # noqa: BLE001 - any start failure becomes a failed run
            run.status, run.error, run.finished_at = (
                "failed",
                f"could not start: {e}",
                run.started_at,
            )
        storage.put_run(run)
        return run

    # --- report apps ------------------------------------------------------------------
    # A report is a web app in a container. One instance per report at a time: started on
    # demand, reached through the proxy below, stopped by its lifetime or the Stop button.

    recent_ports: dict[int, float] = {}  # local ports handed out in the last minute
    routes: dict[tuple[str, str, str], tuple[float, str]] = {}  # proxy targets, briefly cached
    # Scale to zero: when each live app was last used. In memory, so after a restart an app
    # counts as just used, and its lifetime is the backstop if nobody ever comes back.
    usage: dict[tuple[str, str, str], dict] = {}

    def touch(app_: ReportApp) -> dict:
        key = (app_.org_id, app_.pipeline, app_.slug)
        entry = usage.setdefault(key, {"sockets": 0})
        entry["last"], entry["idle"] = time.time(), app_.idle_timeout_seconds
        return entry

    def sweep_idle(now: float | None = None) -> list[tuple[str, str, str]]:
        """Stop every report app nobody has used for its idle timeout. Returns what it stopped."""
        now = now or time.time()
        idle = [k for k, u in usage.items() if not u["sockets"] and now - u["last"] > u["idle"]]
        for key in idle:
            usage.pop(key, None)
            app_ = storage.get_report_app(*key)
            if app_ is not None and app_.status != "stopped":
                stopped(app_)
        return idle

    app.state.sweep_idle = sweep_idle

    def get_report(org_id: str, name: str, slug: str) -> tuple[PipelineSpec, Report]:
        spec = get_pipeline(org_id, name)
        report = spec.report(slug)
        if report is None:
            raise HTTPException(404, f"report '{slug}' not found")
        return spec, report

    def base_path(pipeline: str, slug: str) -> str:
        return f"/apps/{pipeline}/{slug}"

    def pick_port() -> int:
        if settings.is_aws:
            return FARGATE_REPORT_PORT  # each task has its own address
        first, _, last = settings.report_ports.partition("-")
        now = time.time()
        for port in range(int(first), int(last or first) + 1):
            if recent_ports.get(port, 0) < now - 60 and not _answers(f"127.0.0.1:{port}"):
                recent_ports[port] = now
                return port
        raise RuntimeError(
            f"all local report ports ({settings.report_ports}) are in use; stop a report first"
        )

    def stopped(app_: ReportApp, error: str | None = None) -> ReportApp:
        app_.status, app_.error = "stopped", error
        storage.put_report_app(app_)
        routes.pop((app_.org_id, app_.pipeline, app_.slug), None)
        usage.pop((app_.org_id, app_.pipeline, app_.slug), None)
        if app_.handle:
            runner.stop(app_.handle)
        return app_

    def refresh_report(app_: ReportApp | None) -> ReportApp | None:
        """Bring a report app's record up to date with what its container is doing."""
        if app_ is None or app_.status == "stopped" or app_.handle is None:
            return app_
        state = runner.poll(app_.handle)
        if not state.running:
            expired = state.exit_code == TIMEOUT_EXIT_CODE or datetime.now(UTC) >= app_.expires_at
            error = None
            if not expired:
                error = f"the app exited (code {state.exit_code})"
                if state.detail:
                    error += f"\n{state.detail}"
            return stopped(app_, error)
        if datetime.now(UTC) >= app_.expires_at:
            return stopped(app_)
        if (app_.org_id, app_.pipeline, app_.slug) not in usage:
            touch(app_)  # this process has not seen it before (it restarted): count from now
        if app_.status == "starting":
            touch(app_)  # someone is waiting for it
            address = app_.address or runner.address(app_.handle, app_.port)
            ready_path = base_path(app_.pipeline, app_.slug) + "/"
            if address and _answers(address, ready_path):
                app_.status = "running"
            if address != app_.address or app_.status == "running":
                app_.address = address
                storage.put_report_app(app_)
        return app_

    def report_app(org_id: str, pipeline: str, slug: str) -> ReportApp | None:
        return refresh_report(storage.get_report_app(org_id, pipeline, slug))

    def start_report(org_id: str, spec: PipelineSpec, report: Report) -> ReportApp:
        current = report_app(org_id, spec.name, report.slug)
        if current is not None and current.status != "stopped":
            return current
        now = datetime.now(UTC)
        app_ = ReportApp(
            org_id=org_id,
            pipeline=spec.name,
            slug=report.slug,
            status="starting",
            instance_id=contract.new_run_id(),
            started_at=now,
            expires_at=now + timedelta(seconds=report.lifetime_seconds),
            port=0,
            idle_timeout_seconds=report.idle_timeout_seconds,
        )
        try:
            app_.port = pick_port()
            env = {
                "PORT": str(app_.port),
                "BASE_PATH": base_path(spec.name, report.slug),
                "INPUT_URI": storage.uri(contract.dataset_prefix(org_id, report.dataset)),
                **container_env(org_id, read_only=True),
            }
            job = Job(
                name=f"{spec.name}-{report.slug}",
                image=report.image,
                cpu=report.cpu,
                memory=report.memory,
                timeout_seconds=report.lifetime_seconds,
                port=app_.port,
            )
            app_.handle = runner.start(job, org_id, app_.instance_id, env)
        except Exception as e:  # noqa: BLE001 - any start failure is shown on the report page
            app_.status, app_.error = "stopped", f"could not start: {e}"
        storage.put_report_app(app_)
        if app_.status != "stopped":
            touch(app_)
        return app_

    def stop_report(org_id: str, pipeline: str, slug: str) -> ReportApp | None:
        app_ = storage.get_report_app(org_id, pipeline, slug)
        if app_ is not None and app_.status != "stopped":
            stopped(app_)
        return app_

    def report_view(spec: PipelineSpec, report: Report, app_: ReportApp | None) -> dict:
        """What the API returns and the templates render for one report."""
        live = app_ is not None and app_.status != "stopped"
        return {
            "pipeline": spec.name,
            "name": report.name,
            "slug": report.slug,
            "image": report.image,
            "dataset": report.dataset,
            "status": app_.status if app_ else "stopped",
            "url": base_path(spec.name, report.slug) + "/" if live else None,
            "started_at": app_.started_at if live else None,
            "expires_at": app_.expires_at if live else None,
            "idle_timeout_seconds": report.idle_timeout_seconds,
            "error": app_.error if app_ else None,
        }

    def proxy_target(org_id: str, pipeline: str, slug: str) -> str:
        """Address of a running report app, or 404/409. Blocking: call it in a thread."""
        key = (org_id, pipeline, slug)
        cached = routes.get(key)
        if cached and cached[0] > time.time():
            return cached[1]
        get_report(org_id, pipeline, slug)
        app_ = report_app(org_id, pipeline, slug)
        if app_ is None or app_.status != "running" or not app_.address:
            routes.pop(key, None)
            raise HTTPException(409, "this report is not running; start it from its page")
        routes[key] = (time.time() + ROUTE_CACHE_SECONDS, app_.address)
        touch(app_)
        return app_.address

    @app.api_route(
        "/apps/{name}/{slug}/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def proxy_http(request: Request, org_id: Org, name: PipelineName, slug: Slug, path: str):
        address = await run_in_threadpool(proxy_target, org_id, name, slug)
        usage[(org_id, name, slug)]["last"] = time.time()  # in use
        try:
            return await proxy.forward_http(http, request, address)
        except HTTPException:
            routes.pop((org_id, name, slug), None)  # look it up afresh next time
            raise

    @app.get("/apps/{name}/{slug}")
    def proxy_root(org_id: Org, name: PipelineName, slug: Slug):
        return RedirectResponse(base_path(name, slug) + "/", status_code=307)

    @app.websocket("/apps/{name}/{slug}/{path:path}")
    async def proxy_websocket(websocket: WebSocket, name: str, slug: str, path: str):
        try:
            org_id = org_from(websocket)
            if not re.fullmatch(contract.PIPELINE_NAME, name) or not re.fullmatch(SLUG, slug):
                raise HTTPException(404, "not found")
            address = await run_in_threadpool(proxy_target, org_id, name, slug)
        except HTTPException:
            await websocket.close(code=1008)
            return
        entry = usage[(org_id, name, slug)]
        entry["sockets"] += 1  # an open socket means someone has the report open
        try:
            await proxy.forward_websocket(websocket, address)
        finally:
            entry["sockets"] -= 1
            entry["last"] = time.time()

    # --- API --------------------------------------------------------------------------

    @app.post("/api/pipelines", status_code=201)
    def api_register_pipeline(
        org_id: Org, body: Annotated[bytes, Body(media_type="application/yaml")]
    ):
        """Body is the pipeline.yaml text. Re-posting the same name replaces it."""
        try:
            text = body.decode()
            spec = contract.parse_pipeline(text)
        except UnicodeDecodeError as e:
            raise HTTPException(422, ["pipeline.yaml must be UTF-8"]) from e
        except ContractError as e:
            raise HTTPException(422, e.errors) from e
        storage.put_pipeline(org_id, spec.name, text)
        return spec

    @app.get("/api/pipelines")
    def api_pipelines(org_id: Org):
        return storage.list_pipelines(org_id)

    @app.get("/api/pipelines/{name}")
    def api_pipeline(org_id: Org, name: PipelineName):
        return get_pipeline(org_id, name)

    @app.post("/api/pipelines/{name}/runs", status_code=201)
    def api_trigger(org_id: Org, name: PipelineName):
        return trigger(org_id, get_pipeline(org_id, name))

    @app.get("/api/pipelines/{name}/runs")
    def api_pipeline_runs(org_id: Org, name: PipelineName):
        get_pipeline(org_id, name)
        return list_runs(org_id, name)

    @app.get("/api/runs/{run_id}")
    def api_run(org_id: Org, run_id: RunId):
        run = storage.get_run(org_id, run_id)
        if run is None:
            raise HTTPException(404, f"run '{run_id}' not found")
        return reconcile(run).model_dump(exclude={"handle"})

    @app.get("/api/datasets")
    def api_datasets(org_id: Org):
        return list_datasets(org_id)

    @app.get("/api/datasets/{dataset}/contract")
    def api_get_contract(org_id: Org, dataset: DatasetName):
        text = storage.get_data_contract(org_id, dataset)
        if text is None:
            raise HTTPException(404, f"dataset '{dataset}' has no data contract")
        return PlainTextResponse(text, media_type="application/yaml")

    @app.put("/api/datasets/{dataset}/contract")
    def api_put_contract(
        org_id: Org,
        dataset: DatasetName,
        body: Annotated[bytes, Body(media_type="application/yaml")],
    ):
        """Body is an ODCS v3 document. It applies to runs settled from now on."""
        return contract_view(dataset, save_contract(org_id, dataset, body.decode(errors="replace")))

    @app.delete("/api/datasets/{dataset}/contract", status_code=204)
    def api_delete_contract(org_id: Org, dataset: DatasetName):
        storage.delete_data_contract(org_id, dataset)

    @app.get("/api/pipelines/{name}/reports/{slug}")
    def api_report(org_id: Org, name: PipelineName, slug: Slug):
        spec, report = get_report(org_id, name, slug)
        return report_view(spec, report, report_app(org_id, name, slug))

    @app.post("/api/pipelines/{name}/reports/{slug}/start")
    def api_report_start(org_id: Org, name: PipelineName, slug: Slug):
        spec, report = get_report(org_id, name, slug)
        return report_view(spec, report, start_report(org_id, spec, report))

    @app.post("/api/pipelines/{name}/reports/{slug}/stop")
    def api_report_stop(org_id: Org, name: PipelineName, slug: Slug):
        spec, report = get_report(org_id, name, slug)
        return report_view(spec, report, stop_report(org_id, name, slug))

    # --- UI ---------------------------------------------------------------------------

    def page(request: Request, template: str, org_id: str, status_code: int = 200, **context):
        return templates.TemplateResponse(
            request, template, {"org_id": org_id, **context}, status_code=status_code
        )

    @app.get("/", response_class=HTMLResponse)
    def home():
        return RedirectResponse("/pipelines", status_code=303)

    @app.get("/pipelines", response_class=HTMLResponse)
    def ui_pipelines(request: Request, org_id: Org):
        latest: dict[str, dict] = {}
        for run in list_runs(org_id):
            latest.setdefault(run["pipeline"], run)
        return page(
            request,
            "pipelines.html",
            org_id,
            nav="pipelines",
            pipelines=storage.list_pipelines(org_id),
            latest=latest,
        )

    @app.get("/register", response_class=HTMLResponse)
    def ui_register_form(request: Request, org_id: Org, pipeline: str | None = None):
        """Blank form, or with ?pipeline=name, that pipeline's YAML ready to edit."""
        text = None
        if pipeline and re.fullmatch(contract.PIPELINE_NAME, pipeline):
            text = storage.get_pipeline_text(org_id, pipeline)
        return page(
            request,
            "register.html",
            org_id,
            nav="pipelines",
            yaml_text=text or STARTER_YAML,
            editing=pipeline if text else None,
            errors=[],
        )

    @app.post("/register", response_class=HTMLResponse)
    def ui_register(request: Request, org_id: Org, body: Annotated[bytes, Body()]):
        # Browsers send textarea line breaks as CRLF.
        text = parse_qs(body.decode(errors="replace")).get("yaml", [""])[0].replace("\r\n", "\n")
        try:
            spec = contract.parse_pipeline(text)
        except ContractError as e:
            return page(
                request,
                "register.html",
                org_id,
                nav="pipelines",
                yaml_text=text,
                editing=None,
                errors=e.errors,
                status_code=422,
            )
        storage.put_pipeline(org_id, spec.name, text)
        return RedirectResponse(f"/pipelines/{spec.name}", status_code=303)

    @app.get("/pipelines/{name}", response_class=HTMLResponse)
    def ui_pipeline(request: Request, org_id: Org, name: PipelineName):
        spec = get_pipeline(org_id, name)
        return page(
            request,
            "pipeline.html",
            org_id,
            nav="pipelines",
            spec=spec,
            runs=list_runs(org_id, name),
            reports=[report_view(spec, r, report_app(org_id, name, r.slug)) for r in spec.reports],
        )

    @app.get("/pipelines/{name}/runs", response_class=HTMLResponse)
    def ui_runs(request: Request, org_id: Org, name: PipelineName):
        spec = get_pipeline(org_id, name)
        return page(request, "_runs.html", org_id, spec=spec, runs=list_runs(org_id, name))

    @app.post("/pipelines/{name}/runs", response_class=HTMLResponse)
    def ui_trigger(request: Request, org_id: Org, name: PipelineName):
        spec = get_pipeline(org_id, name)
        trigger(org_id, spec)
        return page(request, "_runs.html", org_id, spec=spec, runs=list_runs(org_id, name))

    @app.get("/datasets", response_class=HTMLResponse)
    def ui_datasets(request: Request, org_id: Org):
        producers: dict[str, list[PipelineSpec]] = {}
        for spec in storage.list_pipelines(org_id):
            producers.setdefault(spec.output_dataset, []).append(spec)
        return page(
            request,
            "datasets.html",
            org_id,
            nav="datasets",
            datasets=list_datasets(org_id),
            producers=producers,
        )

    @app.get("/datasets/{dataset}", response_class=HTMLResponse)
    def ui_dataset(request: Request, org_id: Org, dataset: DatasetName):
        entry = next((d for d in list_datasets(org_id) if d["dataset"] == dataset), None)
        if entry is None:
            raise HTTPException(404, f"dataset '{dataset}' not found")
        runs = [r for r in list_runs(org_id) if r["dataset"] == dataset]
        return page(request, "dataset.html", org_id, nav="datasets", dataset=entry, runs=runs[:10])

    @app.get("/datasets/{dataset}/contract", response_class=HTMLResponse)
    def ui_contract_form(request: Request, org_id: Org, dataset: DatasetName):
        text = storage.get_data_contract(org_id, dataset)
        if text is None:  # offer a first draft written from what the dataset contains
            listed = query.list_datasets(settings, org_id, list_runs(org_id))
            observed = next((d["schema"] for d in listed if d["dataset"] == dataset), [])
            text = datacontract.starter(dataset, observed)
        return page(
            request, "contract.html", org_id, nav="datasets", dataset=dataset, yaml_text=text,
            errors=[],
        )  # fmt: skip

    @app.post("/datasets/{dataset}/contract", response_class=HTMLResponse)
    def ui_contract_save(
        request: Request, org_id: Org, dataset: DatasetName, body: Annotated[bytes, Body()]
    ):
        text = parse_qs(body.decode(errors="replace")).get("yaml", [""])[0].replace("\r\n", "\n")
        try:
            save_contract(org_id, dataset, text)
        except HTTPException as e:
            return page(
                request, "contract.html", org_id, nav="datasets", dataset=dataset,
                yaml_text=text, errors=e.detail, status_code=422,
            )  # fmt: skip
        return RedirectResponse(f"/datasets/{dataset}", status_code=303)

    def report_page(request: Request, org_id: str, template: str, spec, report, app_):
        view = report_view(spec, report, app_)
        return page(request, template, org_id, nav="pipelines", spec=spec, report=view)

    @app.get("/pipelines/{name}/reports/{slug}", response_class=HTMLResponse)
    def ui_report(request: Request, org_id: Org, name: PipelineName, slug: Slug):
        spec, report = get_report(org_id, name, slug)
        app_ = report_app(org_id, name, slug)
        if app_ is None or (app_.status == "stopped" and not app_.error):
            # Opening a report wakes it up. One that stopped with an error waits for the
            # button instead, so reloading the page does not restart a crashing app forever.
            app_ = start_report(org_id, spec, report)
        return report_page(request, org_id, "report.html", spec, report, app_)

    @app.get("/pipelines/{name}/reports/{slug}/state", response_class=HTMLResponse)
    def ui_report_state(request: Request, org_id: Org, name: PipelineName, slug: Slug):
        spec, report = get_report(org_id, name, slug)
        app_ = report_app(org_id, name, slug)
        return report_page(request, org_id, "_report.html", spec, report, app_)

    @app.post("/pipelines/{name}/reports/{slug}/start", response_class=HTMLResponse)
    def ui_report_start(request: Request, org_id: Org, name: PipelineName, slug: Slug):
        spec, report = get_report(org_id, name, slug)
        app_ = start_report(org_id, spec, report)
        return report_page(request, org_id, "_report.html", spec, report, app_)

    @app.post("/pipelines/{name}/reports/{slug}/stop", response_class=HTMLResponse)
    def ui_report_stop(request: Request, org_id: Org, name: PipelineName, slug: Slug):
        spec, report = get_report(org_id, name, slug)
        app_ = stop_report(org_id, name, slug)
        return report_page(request, org_id, "_report.html", spec, report, app_)

    return app
