"""The control plane: JSON API under /api and the HTMX UI, in one FastAPI app.

Every handler gets ``org_id`` from the ``current_org`` dependency (a verified token) and
nowhere else. No route takes an org in its path, query or body.
"""

from datetime import UTC, datetime
from pathlib import Path as FsPath
from typing import Annotated
from urllib.parse import parse_qs, quote

from fastapi import Body, Depends, FastAPI, HTTPException, Path, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import contract, query
from .auth import COOKIE, Auth, AuthError
from .config import Settings
from .contract import ContractError, PipelineSpec, RunRecord
from .runner import TIMEOUT_EXIT_CODE, Runner, make_runner
from .storage import Storage, org_credentials

PipelineName = Annotated[str, Path(pattern=contract.PIPELINE_NAME)]
RunId = Annotated[str, Path(pattern=contract.RUN_ID_RE.pattern)]
Slug = Annotated[str, Path(pattern=r"^[a-z0-9-]{1,80}$")]

TEMPORAL = ("DATE", "TIMESTAMP")
NUMERIC = ("INT", "DECIMAL", "DOUBLE", "FLOAT", "HUGEINT")


def chart_spec(chart: contract.Chart, result: query.Result) -> dict | None:
    """A Vega-Lite spec for the report's chart, or None if its columns are not in the result."""
    types = dict(zip(result.columns, result.types, strict=True))
    if chart.x not in types or chart.y not in types:
        return None
    x_type = types[chart.x].upper()
    if x_type.startswith(TEMPORAL):
        x_kind = "temporal"
    elif x_type.startswith(NUMERIC) or "INT" in x_type:
        x_kind = "quantitative"
    else:
        x_kind = "nominal"
    mark: dict = {"type": chart.type, "tooltip": True}
    if chart.type in ("line", "area"):
        mark |= {"strokeWidth": 2, "point": {"size": 30}}
    if chart.type == "area":
        mark |= {"line": True, "fillOpacity": 0.15}
    if chart.type == "bar":
        mark |= {"cornerRadiusEnd": 4, "width": {"band": 0.6}}
    if chart.type == "point":
        mark |= {"size": 64, "filled": True}
    x: dict = {"field": chart.x, "type": x_kind, "title": chart.x}
    if x_kind == "nominal":
        x |= {"sort": None, "axis": {"labelAngle": 0}}  # keep the query's order
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": "container",
        "height": 280,
        "autosize": {"type": "fit-x", "contains": "padding"},
        "data": {"values": jsonable_encoder(result.dicts())},
        "mark": mark,
        "encoding": {
            "x": x,
            "y": {"field": chart.y, "type": "quantitative", "title": chart.y},
        },
    }


def create_app(settings: Settings | None = None, runner: Runner | None = None) -> FastAPI:
    settings = settings or Settings()
    runner = runner or make_runner(settings)
    storage = Storage(settings)
    auth = Auth(settings)
    templates = Jinja2Templates(directory=FsPath(__file__).parent / "templates")
    query.install_extensions()
    if not settings.is_aws:
        storage.ensure_bucket()

    app = FastAPI(title="pipeyard", docs_url=None, redoc_url=None, openapi_url=None)

    # --- auth -------------------------------------------------------------------------

    def current_org(request: Request) -> str:
        header = request.headers.get("authorization", "")
        token = header[7:] if header.lower().startswith("bearer ") else request.cookies.get(COOKIE)
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

    def get_pipeline(org_id: str, name: str) -> PipelineSpec:
        spec = storage.get_pipeline(org_id, name)
        if spec is None:
            raise HTTPException(404, f"pipeline '{name}' not found")
        return spec

    def reconcile(run: RunRecord) -> RunRecord:
        """If a run is still marked running, ask the runner and settle the record."""
        if run.status != "running" or run.handle is None:
            return run
        state = runner.poll(run.handle)
        if state.running:
            return run
        error = None
        if state.exit_code == TIMEOUT_EXIT_CODE:
            error = f"timed out after {run.timeout_seconds}s"
        elif state.exit_code != 0:
            error = f"container exited with code {state.exit_code}"
            if state.detail:
                error += f"\n{state.detail}"
        else:
            text = storage.get_manifest_text(run.org_id, run.dataset, run.run_id)
            if text is None:
                error = f"container exited 0 but wrote no {contract.MANIFEST_FILE}"
            elif not storage.has_parquet(run.org_id, run.dataset, run.run_id):
                error = "container exited 0 but wrote no .parquet files"
            else:
                try:
                    manifest = contract.parse_manifest(text, dataset=run.dataset, run_id=run.run_id)
                    run.row_count = manifest.row_count
                except ContractError as e:
                    error = str(e)
        run.status = "failed" if error else "succeeded"
        run.error = error
        run.finished_at = datetime.now(UTC)
        storage.put_run(run)
        return run

    def list_runs(org_id: str, pipeline: str | None = None) -> list[dict]:
        runs = query.list_runs(settings, org_id, pipeline)
        for i, r in enumerate(runs):
            if r["status"] == "running":
                record = storage.get_run(org_id, r["run_id"])
                if record is not None:
                    settled = reconcile(record).model_dump()
                    runs[i] = {k: settled[k] for k in r}
        return runs

    def trigger(org_id: str, spec: PipelineSpec) -> RunRecord:
        run_id = contract.new_run_id()
        dataset = spec.output_dataset
        output_uri = storage.uri(contract.output_prefix(org_id, dataset, run_id))
        creds = org_credentials(settings, org_id, fresh=True)
        env = {
            "ORG_ID": org_id,
            "RUN_ID": run_id,
            "OUTPUT_URI": output_uri,
            "AWS_REGION": settings.region,
            "AWS_DEFAULT_REGION": settings.region,
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
        }
        if creds.session_token:
            env["AWS_SESSION_TOKEN"] = creds.session_token
        if spec.inputs:
            env["INPUT_URI"] = storage.uri(contract.dataset_prefix(org_id, spec.inputs[0].dataset))
        if not settings.is_aws:
            env["AWS_ENDPOINT_URL"] = settings.runner_s3_endpoint_url
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
            run.handle = runner.start(spec, org_id, run_id, env)
        except Exception as e:  # noqa: BLE001 - any start failure becomes a failed run
            run.status, run.error, run.finished_at = (
                "failed",
                f"could not start: {e}",
                run.started_at,
            )
        storage.put_run(run)
        return run

    def report_result(org_id: str, spec: PipelineSpec, slug: str):
        report = spec.report(slug)
        if report is None:
            raise HTTPException(404, f"report '{slug}' not found")
        try:
            result = query.run_report(settings, org_id, report)
        except LookupError as e:
            raise HTTPException(409, str(e)) from e
        except query.duckdb.Error as e:
            raise HTTPException(400, f"report query failed: {str(e).splitlines()[0]}") from e
        return report, result

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
        return query.list_datasets(settings, org_id)

    @app.get("/api/pipelines/{name}/reports/{slug}")
    def api_report(org_id: Org, name: PipelineName, slug: Slug):
        report, result = report_result(org_id, get_pipeline(org_id, name), slug)
        return {
            "name": report.name,
            "dataset": report.dataset,
            "columns": [
                {"name": n, "type": t} for n, t in zip(result.columns, result.types, strict=True)
            ],
            "rows": result.dicts(),
            "truncated": result.truncated,
        }

    # --- UI ---------------------------------------------------------------------------

    def page(request: Request, template: str, org_id: str, **context):
        return templates.TemplateResponse(request, template, {"org_id": org_id, **context})

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
            datasets=query.list_datasets(settings, org_id),
            producers=producers,
        )

    @app.get("/pipelines/{name}/reports/{slug}", response_class=HTMLResponse)
    def ui_report(request: Request, org_id: Org, name: PipelineName, slug: Slug):
        spec = get_pipeline(org_id, name)
        report = spec.report(slug)
        if report is None:
            raise HTTPException(404, f"report '{slug}' not found")
        result = chart = problem = None
        try:
            _, result = report_result(org_id, spec, slug)
            chart = chart_spec(report.chart, result) if report.chart else None
            if report.chart and chart is None:
                problem = (
                    f"Chart columns '{report.chart.x}' and '{report.chart.y}' are not both in "
                    "the query result, so only the table is shown."
                )
        except HTTPException as e:
            if e.status_code not in (400, 409):
                raise
            problem = e.detail
        return page(
            request,
            "report.html",
            org_id,
            nav="pipelines",
            spec=spec,
            report=report,
            result=result,
            chart=chart,
            problem=problem,
        )

    return app
