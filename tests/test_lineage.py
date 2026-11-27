"""OpenLineage events: one START and one COMPLETE or FAIL per run, stored and optionally sent."""

import dataclasses
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from conftest import register
from fastapi.testclient import TestClient

from pipeyard.app import create_app
from pipeyard.auth import Auth
from pipeyard.lineage import PRODUCER, run_uuid
from pipeyard.runner import RunState

P = "/api/pipelines/fake-transactions"
REPORT = f"{P}/reports/transactions-dashboard"


def run_once(client):
    run = client.post(f"{P}/runs").json()
    return client.get(f"/api/runs/{run['run_id']}").json()


def lineage(client, run_id):
    return client.get(f"/api/runs/{run_id}/lineage").json()


def assert_well_formed(event: dict) -> None:
    """The parts of the OpenLineage RunEvent schema that are required."""
    assert event["producer"] == PRODUCER
    assert (
        event["schemaURL"] == "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
    )
    assert event["eventTime"].endswith("Z") and "T" in event["eventTime"]
    assert str(uuid.UUID(event["run"]["runId"])) == event["run"]["runId"]
    assert event["job"]["namespace"] and event["job"]["name"]
    facets = [*event["run"]["facets"].values(), *event["job"]["facets"].values()]
    for dataset in [*event["inputs"], *event["outputs"]]:
        assert dataset["namespace"].startswith("s3://") and not dataset["name"].startswith("/")
        facets += [*dataset["facets"].values(), *dataset.get("outputFacets", {}).values()]
    for facet in facets:
        assert facet["_producer"] == PRODUCER
        assert facet["_schemaURL"].startswith("https://openlineage.io/spec/facets/")


def test_a_successful_run(client, org, settings, example_yaml):
    register(client, example_yaml)
    run = run_once(client)
    start, complete = lineage(client, run["run_id"])
    for event in (start, complete):
        assert_well_formed(event)
        assert event["run"]["runId"] == run_uuid(org, run["run_id"])
        assert event["job"] == {
            "namespace": f"pipeyard/{org}",
            "name": "fake-transactions",
            "facets": {"jobType": event["job"]["facets"]["jobType"]},
        }
        assert event["job"]["facets"]["jobType"]["integration"] == "PIPEYARD"
        (output,) = event["outputs"]
        assert output["namespace"] == f"s3://{settings.bucket}"
        assert output["name"] == f"orgs/{org}/datasets/transactions"
        assert event["inputs"] == []

    assert (start["eventType"], complete["eventType"]) == ("START", "COMPLETE")
    assert start["eventTime"] < complete["eventTime"]
    assert start["outputs"][0]["facets"] == {}
    (output,) = complete["outputs"]
    assert output["outputFacets"]["outputStatistics"]["rowCount"] == 30
    assert output["facets"]["schema"]["fields"] == [
        {"name": "ts", "type": "TIMESTAMP"},
        {"name": "amount", "type": "DOUBLE"},
        {"name": "owner", "type": "VARCHAR"},
    ]
    assert complete["run"]["facets"] == {}


def test_a_failed_run(client, runner, example_yaml):
    register(client, example_yaml)
    runner.behaviour = "exit1"
    run = run_once(client)
    start, fail = lineage(client, run["run_id"])
    assert (start["eventType"], fail["eventType"]) == ("START", "FAIL")
    assert_well_formed(fail)
    error = fail["run"]["facets"]["errorMessage"]
    assert (
        error["message"].startswith("container exited with code 1") and error["programmingLanguage"]
    )
    assert "outputFacets" not in fail["outputs"][0] and fail["outputs"][0]["facets"] == {}


def test_a_run_that_breaks_its_data_contract_is_a_fail(client, example_yaml):
    register(client, example_yaml)
    contract = (
        "apiVersion: v3.2.0\nkind: DataContract\nid: c\nversion: 1.0.0\n"
        "schema:\n  - name: transactions\n    properties:\n      - {name: nope}\n"
    )
    client.put("/api/datasets/transactions/contract", content=contract)
    run = run_once(client)
    fail = lineage(client, run["run_id"])[-1]
    assert fail["eventType"] == "FAIL"
    assert "column 'nope' is missing" in fail["run"]["facets"]["errorMessage"]["message"]


def test_a_run_that_could_not_start_still_has_both_events(
    client, runner, example_yaml, monkeypatch
):
    register(client, example_yaml)

    def boom(*args):
        raise RuntimeError("no such image")

    monkeypatch.setattr(runner, "start", boom)
    run = client.post(f"{P}/runs").json()
    assert [e["eventType"] for e in lineage(client, run["run_id"])] == ["START", "FAIL"]


def test_inputs_are_recorded(client, org, example_yaml):
    register(client, example_yaml.replace("outputs:", "inputs:\n  - dataset: raw_events\noutputs:"))
    run = run_once(client)
    for event in lineage(client, run["run_id"]):
        assert [i["name"] for i in event["inputs"]] == [f"orgs/{org}/datasets/raw_events"]


def test_settling_a_run_twice_records_its_end_once(client, runner, app, org, example_yaml):
    register(client, example_yaml)
    runner.behaviour = "running"
    run_id = client.post(f"{P}/runs").json()["run_id"]
    assert [e["eventType"] for e in lineage(client, run_id)] == ["START"]  # still running
    runner.states[run_id] = RunState(False, 1, "boom")
    client.get(f"/api/runs/{run_id}")
    (_, fail) = lineage(client, run_id)
    # Two requests settling the same run at once would both store its end: same object.
    app.state.storage.put_lineage_event(org, run_id, fail)
    assert [e["eventType"] for e in lineage(client, run_id)] == ["START", "FAIL"]


def test_report_apps_are_jobs_that_read_a_dataset(client, runner, org, app, example_yaml):
    register(client, example_yaml)
    client.post(f"{REPORT}/start")
    assert client.get(REPORT).json()["status"] == "running"
    (instance,) = runner.apps
    (start,) = lineage(client, instance)
    assert_well_formed(start)
    assert start["eventType"] == "START"
    assert start["job"]["name"] == "fake-transactions.transactions-dashboard"
    assert start["job"]["facets"]["jobType"]["jobType"] == "REPORT_APP"
    assert [i["name"] for i in start["inputs"]] == [f"orgs/{org}/datasets/transactions"]
    assert start["outputs"] == []

    client.post(f"{REPORT}/stop")
    assert [e["eventType"] for e in lineage(client, instance)] == ["START", "COMPLETE"]

    # an idle shutdown is a normal end too
    client.post(f"{REPORT}/start")
    (second,) = runner.apps
    app.state.sweep_idle(9e12)
    assert [e["eventType"] for e in lineage(client, second)] == ["START", "COMPLETE"]


def test_a_report_app_that_crashes_is_a_fail(client, runner, example_yaml):
    register(client, example_yaml)
    client.post(f"{REPORT}/start")
    (instance,) = runner.apps
    runner.states[instance] = RunState(False, 2, "boom")
    client.get(REPORT)
    start, fail = lineage(client, instance)
    assert (
        fail["eventType"] == "FAIL"
        and "exited (code 2)" in fail["run"]["facets"]["errorMessage"]["message"]
    )


def test_lineage_is_per_org(client, client_for, example_yaml):
    register(client, example_yaml)
    run = run_once(client)
    assert client_for("org-lineage-b").get(f"/api/runs/{run['run_id']}/lineage").status_code == 404
    assert client.get("/api/runs/20260101t000000z-abcdef12/lineage").status_code == 404
    assert client_for(None).get(f"/api/runs/{run['run_id']}/lineage").status_code == 401


# --- sending to an OpenLineage server --------------------------------------------------------


@pytest.fixture
def receiver():
    """A stand-in for Marquez or any OpenLineage HTTP endpoint."""
    received = []

    class Handler(BaseHTTPRequestHandler):
        status = 201

        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append((self.path, self.headers.get("Authorization"), json.loads(body)))
            self.send_response(Handler.status)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", received, Handler
    server.shutdown()


def client_with(settings, runner, org, **changes):
    app = create_app(dataclasses.replace(settings, **changes), runner)
    client = TestClient(app, follow_redirects=False)
    client.headers["Authorization"] = f"Bearer {Auth(settings).dev_token(org)}"
    return client, app


def test_events_are_posted_in_order(settings, runner, org, example_yaml, receiver):
    url, received, _ = receiver
    client, app = client_with(
        settings, runner, org, openlineage_url=url, openlineage_api_key="s3cret",
        openlineage_namespace="prod",
    )  # fmt: skip
    register(client, example_yaml)
    run = run_once(client)
    app.state.lineage.flush()
    assert [(path, auth) for path, auth, _ in received] == [
        ("/api/v1/lineage", "Bearer s3cret")
    ] * 2
    assert [e["eventType"] for _, _, e in received] == ["START", "COMPLETE"]
    assert [e for _, _, e in received] == lineage(client, run["run_id"])  # same as what is stored
    assert received[0][2]["job"]["namespace"] == f"prod/{org}"


def test_a_custom_endpoint_path(settings, runner, org, example_yaml, receiver):
    url, received, _ = receiver
    client, app = client_with(
        settings, runner, org, openlineage_url=url + "/", openlineage_endpoint="/custom/events"
    )
    register(client, example_yaml)
    run_once(client)
    app.state.lineage.flush()
    assert {path for path, _, _ in received} == {"/custom/events"}


@pytest.mark.parametrize("target", ["refuses", "errors"])
def test_a_broken_lineage_server_never_breaks_a_run(
    settings, runner, org, example_yaml, receiver, target
):
    url, received, handler = receiver
    handler.status = 500
    if target == "refuses":
        url = "http://127.0.0.1:1"  # nothing listens here
    client, app = client_with(settings, runner, org, openlineage_url=url)
    register(client, example_yaml)
    run = run_once(client)
    app.state.lineage.flush()
    assert run["status"] == "succeeded"
    assert len(lineage(client, run["run_id"])) == 2  # still on record in S3
    handler.status = 201


def test_nothing_is_sent_without_a_url(client, app, example_yaml, receiver):
    _, received, _ = receiver
    register(client, example_yaml)
    run_once(client)
    app.state.lineage.flush()
    assert received == [] and app.state.lineage._queue is None


# --- lineage inside pipeyard: who writes a dataset, who reads it -----------------------------

DOWNSTREAM = """
name: daily-summary
image: summary:latest
inputs:
  - dataset: transactions
outputs:
  - dataset: daily_summary
reports:
  - name: Summary board
    image: board:latest
    dataset: daily_summary
"""


def test_dataset_lineage_api(client, example_yaml):
    register(client, example_yaml)
    register(client, DOWNSTREAM)
    assert client.get("/api/datasets/transactions/lineage").json() == {
        "dataset": "transactions",
        "written_by": [{"pipeline": "fake-transactions"}],
        "read_by": [
            {"kind": "pipeline", "pipeline": "daily-summary"},
            {
                "kind": "report",
                "pipeline": "fake-transactions",
                "report": "Transactions dashboard",
                "slug": "transactions-dashboard",
            },
        ],
    }
    summary = client.get("/api/datasets/daily_summary/lineage").json()
    assert summary["written_by"] == [{"pipeline": "daily-summary"}]
    assert [r["report"] for r in summary["read_by"]] == ["Summary board"]
    assert client.get("/api/datasets/unknown_data/lineage").status_code == 404


def test_lineage_is_shown_on_dataset_and_pipeline_pages(client, example_yaml):
    register(client, example_yaml)
    register(client, DOWNSTREAM)
    run_once(client)

    dataset = client.get("/datasets/transactions").text
    assert "<h2>Lineage</h2>" in dataset
    assert 'href="/pipelines/fake-transactions"' in dataset  # written by, with its last result
    assert '<span class="status succeeded">succeeded</span>' in dataset.split("<h2>Recent runs")[0]
    assert 'href="/pipelines/daily-summary"' in dataset  # read by a pipeline
    assert 'href="/pipelines/fake-transactions/reports/transactions-dashboard"' in dataset

    upstream = client.get("/pipelines/fake-transactions").text.split("<h2>Reports</h2>")[0]
    assert 'href="/datasets/transactions"' in upstream and "daily-summary" in upstream

    downstream = client.get("/pipelines/daily-summary").text.split("<h2>Reports</h2>")[0]
    # reads transactions, writes daily_summary, which its report reads
    assert downstream.index("/datasets/transactions") < downstream.index("/datasets/daily_summary")
    assert "Summary board" in downstream


def test_lineage_only_shows_the_callers_org(client, client_for, example_yaml):
    register(client, example_yaml)
    other = client_for("org-flow-b")
    assert other.get("/api/datasets/transactions/lineage").status_code == 404
    register(other, DOWNSTREAM)
    mine = client.get("/api/datasets/transactions/lineage").json()
    assert [r["pipeline"] for r in mine["read_by"]] == ["fake-transactions"]  # not org B's reader


def test_a_dataset_nothing_reads_yet(client):
    register(client, "name: lonely\nimage: i\noutputs: [{dataset: lonely_data}]\n")
    run_once_named = client.post("/api/pipelines/lonely/runs").json()
    client.get(f"/api/runs/{run_once_named['run_id']}")
    page = client.get("/datasets/lonely_data").text
    assert "nothing yet" in page
