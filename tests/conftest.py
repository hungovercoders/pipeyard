"""Tests run the real app against the AWS emulator (`task test` starts it), with a fake runner."""

import io
import json
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient
from websockets.sync.server import serve

from pipeyard import contract
from pipeyard.app import create_app
from pipeyard.auth import Auth
from pipeyard.config import Settings
from pipeyard.runner import RunState
from pipeyard.storage import Storage

EXAMPLE = Path(__file__).parent.parent / "examples/pipelines/fake-transactions/pipeline.yaml"


class FakeApp:
    """A stand-in for a report container: answers HTTP with what it received, echoes WebSockets."""

    def __init__(self, env: dict[str, str]):
        self.env = env
        self.requests: list[dict] = []

        def respond(connection, request):
            if request.headers.get("Upgrade", "").lower() == "websocket":
                return None  # carry on with the WebSocket handshake
            seen = {"path": request.path, "headers": dict(request.headers.raw_items())}
            self.requests.append(seen)
            return connection.respond(200, json.dumps({"app": env["ORG_ID"], **seen}))

        def echo(connection):
            for message in connection:
                connection.send(f"{env['ORG_ID']} heard {message}")

        self.server = serve(echo, "127.0.0.1", 0, process_request=respond)
        self.port = self.server.socket.getsockname()[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


class FakeRunner:
    """Stands in for Docker: does what a contract-following container would do, in-process.

    ``behaviour`` picks a run's outcome: ok | exit1 | exit1_after_manifest | no_manifest |
    bad_manifest | junk_manifest | running. A job with a port is a report app (FakeApp).
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.behaviour = "ok"
        self.started: list[dict] = []
        self.states: dict[str, RunState] = {}
        self.cleaned: list[str] = []
        self.apps: dict[str, FakeApp] = {}
        self.app_ready = True

    def start(self, job, org_id, run_id, env):
        self.started.append({"job": job, "org_id": org_id, "run_id": run_id, "env": env})
        if job.port is not None:
            self.apps[run_id] = FakeApp(env)
            self.states[run_id] = RunState(True)
            return run_id
        prefix = env["OUTPUT_URI"].split("/", 3)[3]
        dataset = prefix.split("/")[3]
        assert prefix == contract.output_prefix(org_id, dataset, run_id)
        if self.behaviour == "running":
            self.states[run_id] = RunState(True)
            return run_id
        if self.behaviour == "exit1":
            self.states[run_id] = RunState(False, 1, "Traceback: boom")
            return run_id
        path = f"/tmp/{uuid.uuid4().hex}.parquet"  # noqa: S108
        duckdb.sql(
            "select (timestamp '2026-01-01' + to_days((i % 3)::int)) as ts, "
            f"i::double as amount, '{org_id}' as owner from range(30) t(i)"
        ).write_parquet(path)
        self.storage.s3.upload_file(path, self.storage.bucket, prefix + "part-0.parquet")
        Path(path).unlink()
        manifest = {
            "dataset": dataset,
            "run_id": run_id,
            "row_count": 30,
            "schema": [
                {"name": "ts", "type": "TIMESTAMP"},
                {"name": "amount", "type": "DOUBLE"},
                {"name": "owner", "type": "VARCHAR"},
            ],
            "created_at": datetime.now(UTC).isoformat(),
        }
        if self.behaviour == "bad_manifest":
            manifest["run_id"] = "someone-elses-run"
        if self.behaviour == "junk_manifest":
            self.storage.s3.put_object(
                Bucket=self.storage.bucket, Key=prefix + contract.MANIFEST_FILE, Body=b"{not json"
            )
        elif self.behaviour != "no_manifest":
            self.storage.s3.upload_fileobj(
                io.BytesIO(json.dumps(manifest).encode()),
                self.storage.bucket,
                prefix + contract.MANIFEST_FILE,
            )
        code = 1 if self.behaviour == "exit1_after_manifest" else 0
        self.states[run_id] = RunState(False, code)
        return run_id

    def poll(self, handle):
        # Like the real runners: unknown once cleaned up, and never forgets before that.
        return self.states.get(handle) or RunState(False, None, "container no longer exists", True)

    def cleanup(self, handle):
        self.cleaned.append(handle)
        self.states.pop(handle, None)

    def address(self, handle, port):
        app = self.apps.get(handle)
        return f"127.0.0.1:{app.port}" if app and self.app_ready else None

    def stop(self, handle):
        app = self.apps.pop(handle, None)
        if app:
            app.close()
        self.cleanup(handle)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings(mode="local", bucket=f"pipeyard-test-{uuid.uuid4().hex[:10]}")


@pytest.fixture(scope="session")
def storage(settings) -> Storage:
    storage = Storage(settings)
    try:
        storage.ensure_bucket()
    except Exception as e:  # noqa: BLE001
        pytest.exit(
            f"AWS emulator not reachable at {settings.s3_endpoint_url} ({e}). Run `task emulator`."
        )
    yield storage
    objects = storage.s3.list_objects_v2(Bucket=storage.bucket).get("Contents", [])
    for obj in objects:
        storage.s3.delete_object(Bucket=storage.bucket, Key=obj["Key"])
    storage.s3.delete_bucket(Bucket=storage.bucket)


@pytest.fixture
def runner(storage) -> FakeRunner:
    runner = FakeRunner(storage)
    yield runner
    for app in runner.apps.values():
        app.close()


@pytest.fixture
def app(settings, storage, runner):
    return create_app(settings, runner)


@pytest.fixture
def client_for(app, settings):
    """client_for("acme") -> a TestClient whose token says org_id=acme."""

    def make(org_id: str | None) -> TestClient:
        client = TestClient(app, follow_redirects=False)
        if org_id is not None:
            client.headers["Authorization"] = f"Bearer {Auth(settings).dev_token(org_id)}"
        return client

    return make


@pytest.fixture
def org() -> str:
    """A fresh org per test, so tests never see each other's objects."""
    return f"org-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def client(client_for, org) -> TestClient:
    return client_for(org)


@pytest.fixture
def example_yaml() -> str:
    return EXAMPLE.read_text()


def register(client: TestClient, yaml_text: str):
    return client.post(
        "/api/pipelines", content=yaml_text, headers={"Content-Type": "application/yaml"}
    )
