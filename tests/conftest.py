"""Tests run the real app against MinIO (`task test` starts it) with a fake runner."""

import io
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from pipeyard import contract
from pipeyard.app import create_app
from pipeyard.auth import Auth
from pipeyard.config import Settings
from pipeyard.runner import RunState
from pipeyard.storage import Storage

EXAMPLE = Path(__file__).parent.parent / "examples/pipelines/fake-transactions/pipeline.yaml"


class FakeRunner:
    """Stands in for Docker: does what a contract-following container would do, in-process.

    ``behaviour`` picks the outcome: ok | exit1 | no_manifest | bad_manifest | running.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.behaviour = "ok"
        self.started: list[dict] = []
        self.states: dict[str, RunState] = {}

    def start(self, spec, org_id, run_id, env):
        self.started.append({"spec": spec, "org_id": org_id, "run_id": run_id, "env": env})
        prefix = contract.output_prefix(org_id, spec.output_dataset, run_id)
        assert env["OUTPUT_URI"] == f"s3://{self.storage.bucket}/{prefix}"
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
            "dataset": spec.output_dataset,
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
        if self.behaviour != "no_manifest":
            self.storage.s3.upload_fileobj(
                io.BytesIO(json.dumps(manifest).encode()),
                self.storage.bucket,
                prefix + contract.MANIFEST_FILE,
            )
        self.states[run_id] = RunState(False, 0)
        return run_id

    def poll(self, handle):
        return self.states[handle]


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
            f"MinIO is not reachable at {settings.s3_endpoint_url} ({e}). Run `task minio`."
        )
    yield storage
    objects = storage.s3.list_objects_v2(Bucket=storage.bucket).get("Contents", [])
    for obj in objects:
        storage.s3.delete_object(Bucket=storage.bucket, Key=obj["Key"])
    storage.s3.delete_bucket(Bucket=storage.bucket)


@pytest.fixture
def runner(storage) -> FakeRunner:
    return FakeRunner(storage)


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
