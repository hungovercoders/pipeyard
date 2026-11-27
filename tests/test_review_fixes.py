"""Regressions for failure modes found in review: each test reproduces one of them."""

import threading
from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError
from conftest import register

from pipeyard import contract
from pipeyard import runner as runner_module
from pipeyard.config import Settings
from pipeyard.runner import TIMEOUT_EXIT_CODE, DockerRunner, RunState

P = "/api/pipelines/fake-transactions"


def run_once(client):
    run = client.post(f"{P}/runs").json()
    return client.get(f"/api/runs/{run['run_id']}").json()


def test_failed_run_that_left_a_manifest_is_not_data(client, runner, example_yaml):
    register(client, example_yaml)
    runner.behaviour = "exit1_after_manifest"  # wrote parquet + manifest, then crashed
    assert run_once(client)["status"] == "failed"
    assert client.get("/api/datasets").json() == []

    runner.behaviour = "ok"
    assert run_once(client)["status"] == "succeeded"
    (dataset,) = client.get("/api/datasets").json()
    assert (dataset["runs"], dataset["row_count"]) == (1, 30)


@pytest.mark.parametrize("behaviour", ["junk_manifest", "bad_manifest", "no_manifest"])
def test_a_bad_manifest_cannot_break_datasets_or_reports(client, runner, example_yaml, behaviour):
    register(client, example_yaml)
    assert run_once(client)["status"] == "succeeded"
    runner.behaviour = behaviour
    assert run_once(client)["status"] == "failed"
    (dataset,) = client.get("/api/datasets").json()
    assert dataset["row_count"] == 30 and dataset["schema"]
    assert client.get("/datasets").status_code == 200


def test_manifest_overwritten_after_success_only_loses_the_schema(
    client, runner, storage, org, example_yaml
):
    register(client, example_yaml)
    run = run_once(client)
    key = contract.output_prefix(org, "transactions", run["run_id"]) + contract.MANIFEST_FILE
    storage.s3.put_object(Bucket=storage.bucket, Key=key, Body=b"{not json")
    (dataset,) = client.get("/api/datasets").json()
    assert dataset["row_count"] == 30 and dataset["schema"] == []


def test_datasets_and_reports_settle_runs_nobody_looked_at(client, example_yaml):
    register(client, example_yaml)
    client.post(f"{P}/runs")  # never fetched, so still recorded as running
    assert client.get("/api/datasets").json()[0]["row_count"] == 30


def test_concurrent_reconciles_agree(client, runner, example_yaml):
    """Two readers settling the same run must not turn a success into a failure."""
    register(client, example_yaml)
    run_id = client.post(f"{P}/runs").json()["run_id"]
    barrier = threading.Barrier(2)
    real_poll = runner.poll

    def poll_together(handle):
        state = real_poll(handle)
        barrier.wait(timeout=5)  # both requests have polled before either saves
        return state

    runner.poll = poll_together
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(client.get(f"/api/runs/{run_id}").json()))
        for _ in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    runner.poll = real_poll
    assert [r["status"] for r in results] == ["succeeded", "succeeded"]
    assert client.get(f"/api/runs/{run_id}").json()["status"] == "succeeded"


def test_cleanup_happens_only_after_the_outcome_is_saved(client, runner, app, example_yaml):
    storage = app.state.storage
    register(client, example_yaml)
    run_id = client.post(f"{P}/runs").json()["run_id"]
    real_put = storage.put_run
    storage.put_run = lambda run: (_ for _ in ()).throw(RuntimeError("S3 is down"))
    try:
        with pytest.raises(RuntimeError):
            client.get(f"/api/runs/{run_id}")
    finally:
        storage.put_run = real_put
    assert runner.cleaned == []  # outcome still available from the runner
    assert client.get(f"/api/runs/{run_id}").json()["status"] == "succeeded"
    assert runner.cleaned == [run_id]


def test_run_the_runner_forgot_is_decided_from_its_output(client, runner, example_yaml):
    """ECS forgets stopped tasks after about an hour; a good run must not become a failure."""
    register(client, example_yaml)
    good = client.post(f"{P}/runs").json()["run_id"]
    runner.behaviour = "no_manifest"
    bad = client.post(f"{P}/runs").json()["run_id"]
    runner.states.clear()  # the runner no longer knows either container
    assert client.get(f"/api/runs/{good}").json()["status"] == "succeeded"
    forgotten = client.get(f"/api/runs/{bad}").json()
    assert forgotten["status"] == "failed"
    assert forgotten["error"] == "container no longer exists but wrote no _manifest.json"


def test_missing_objects_are_404_when_s3_answers_403(client, app, example_yaml, monkeypatch):
    """With the ABAC role, S3 says AccessDenied (not NoSuchKey) for a key that is not there."""
    storage = app.state.storage
    register(client, example_yaml)
    real_get = storage.s3.get_object

    def get_object(**kwargs):
        try:
            return real_get(**kwargs)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                e.response["Error"]["Code"] = "AccessDenied"
            raise

    monkeypatch.setattr(storage.s3, "get_object", get_object)
    assert client.get("/api/pipelines/unknown").status_code == 404
    assert client.get("/api/runs/20260101t000000z-abcdef12").status_code == 404
    assert client.get("/api/pipelines/fake-transactions").status_code == 200

    def denied(**kwargs):  # a real permission problem on an existing object still surfaces
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetObject")

    monkeypatch.setattr(storage.s3, "get_object", denied)
    with pytest.raises(ClientError):
        client.get("/api/pipelines/fake-transactions")


@pytest.mark.parametrize("mode", ["AWS", "prod", "", "Local"])
def test_unknown_mode_refuses_to_start(mode):
    with pytest.raises(ValueError, match="PIPEYARD_MODE must be"):
        Settings(mode=mode)


def test_timeout_leaves_room_for_the_credentials():
    assert contract.PipelineSpec.model_fields["timeout_seconds"].metadata[1].le <= 3000


def _docker_inspect(monkeypatch, state, timeout=60):
    """Make DockerRunner see a container in ``state``; returns the docker commands it ran."""
    import json

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd[:2])
        out = ""
        if cmd[1] == "inspect":
            labels = {runner_module.TIMEOUT_LABEL: str(timeout)}
            out = json.dumps({"State": _resolve(state), "Config": {"Labels": labels}})
        if cmd[1] == "logs":
            out = "Traceback: boom"
        return type("P", (), {"returncode": 0, "stdout": out, "stderr": ""})

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    return calls


class Ago(float):
    """A moment this many seconds before the test runs.

    Resolved inside the test, not when the module is collected: collection can be a minute
    before a test at the end of the suite runs, which would shift every timestamp.
    """


def _resolve(state: dict) -> dict:
    return {k: _ts(v) if isinstance(v, Ago) else v for k, v in state.items()}


def _ts(seconds_ago: float) -> str:
    return (datetime.now(UTC) - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")


NOT_YET = "0001-01-01T00:00:00Z"


def test_docker_timeout_survives_a_control_plane_restart(monkeypatch, settings):
    """No timer exists (the process restarted), but the container is past its deadline."""
    calls = _docker_inspect(
        monkeypatch,
        {"Status": "running", "StartedAt": Ago(90), "FinishedAt": NOT_YET, "ExitCode": 0},
    )
    state = DockerRunner(settings).poll("c")
    assert (state.running, state.exit_code) == (False, TIMEOUT_EXIT_CODE)
    assert ["docker", "kill"] in calls


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (
            {"Status": "running", "StartedAt": Ago(5), "FinishedAt": NOT_YET, "ExitCode": 0},
            RunState(True),
        ),
        (
            {"Status": "exited", "StartedAt": Ago(70), "FinishedAt": Ago(9), "ExitCode": 137},
            RunState(False, TIMEOUT_EXIT_CODE, "timed out"),
        ),
        (
            {
                "Status": "exited",
                "StartedAt": Ago(70),
                "FinishedAt": Ago(60),
                "ExitCode": 137,
                "OOMKilled": True,
            },
            RunState(False, 137, "out of memory\nTraceback: boom"),
        ),
        (
            {"Status": "exited", "StartedAt": Ago(70), "FinishedAt": Ago(60), "ExitCode": 1},
            RunState(False, 1, "Traceback: boom"),
        ),
        (
            {"Status": "exited", "StartedAt": Ago(70), "FinishedAt": Ago(60), "ExitCode": 0},
            RunState(False, 0),
        ),
    ],
)
def test_docker_poll_outcomes(monkeypatch, settings, state, expected):
    calls = _docker_inspect(monkeypatch, state)
    assert DockerRunner(settings).poll("c") == expected
    assert ["docker", "rm"] not in calls  # poll never discards the outcome


def test_timeout_sidecar_stops_promptly():
    """The sidecar must exit on SIGTERM, or every Fargate run lingers for the stop timeout."""
    import subprocess
    import time

    from test_aws_mode import template as template_fixture

    built = template_fixture.__wrapped__()
    run = next(
        t["Properties"]
        for t in built["Resources"].values()
        if t["Type"] == "AWS::ECS::TaskDefinition"
        and {c["Name"] for c in t["Properties"]["ContainerDefinitions"]} == {"pipeline", "timeout"}
    )
    command = next(c for c in run["ContainerDefinitions"] if c["Name"] == "timeout")["Command"]

    proc = subprocess.Popen(command, env={"TIMEOUT_SECONDS": "30", "PATH": "/usr/bin:/bin"})
    time.sleep(0.3)
    started = time.monotonic()
    proc.terminate()
    assert proc.wait(timeout=5) == 0 and time.monotonic() - started < 2

    expired = subprocess.run(command, env={"TIMEOUT_SECONDS": "0.2", "PATH": "/usr/bin:/bin"})
    assert expired.returncode == TIMEOUT_EXIT_CODE
