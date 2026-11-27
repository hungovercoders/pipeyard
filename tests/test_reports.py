"""Reports are web apps in containers: started on demand, reached only through the proxy."""

import json
import time
from datetime import UTC, datetime, timedelta

import pytest
from conftest import register
from fastapi.testclient import TestClient

from pipeyard.app import create_app
from pipeyard.auth import Auth
from pipeyard.runner import TIMEOUT_EXIT_CODE, RunState

REPORT = "/api/pipelines/fake-transactions/reports/transactions-dashboard"
APP = "/apps/fake-transactions/transactions-dashboard"


@pytest.fixture
def started(client, runner, example_yaml):
    """A client whose example report has been started and is running."""
    register(client, example_yaml)
    assert client.post(f"{REPORT}/start").json()["status"] == "starting"
    assert client.get(REPORT).json()["status"] == "running"
    return client


def test_report_starts_on_demand_and_is_served_through_the_proxy(
    client, runner, org, settings, example_yaml
):
    register(client, example_yaml)
    status = client.get(REPORT).json()
    assert (status["status"], status["url"]) == ("stopped", None)
    assert client.get(f"{APP}/").status_code == 409  # nothing to proxy to yet

    assert client.post(f"{REPORT}/start").json()["status"] == "starting"
    started = runner.started[-1]
    job, env = started["job"], started["env"]
    assert (job.image, job.cpu, job.memory, job.timeout_seconds) == (
        "pipeyard-example-fake-transactions-report:latest", 512, 1024, 3000,
    )  # fmt: skip
    assert job.port == int(env["PORT"])
    assert env["BASE_PATH"] == APP
    assert env["INPUT_URI"] == f"s3://{settings.bucket}/orgs/{org}/datasets/transactions/"
    assert env["ORG_ID"] == org and "AWS_ACCESS_KEY_ID" in env
    assert "RUN_ID" not in env and "OUTPUT_URI" not in env

    status = client.get(REPORT).json()
    assert status["status"] == "running" and status["url"] == f"{APP}/"
    assert "handle" not in status and "address" not in status

    answer = client.get(f"{APP}/some/page?x=1&y=two").json()
    assert answer["app"] == org
    assert answer["path"] == f"{APP}/some/page?x=1&y=two"  # the prefix is passed through


def test_starting_twice_keeps_the_one_instance(started, runner):
    before = len(runner.started)
    assert started.post(f"{REPORT}/start").json()["status"] == "running"
    assert len(runner.started) == before


def test_report_waits_until_the_app_answers(client, runner, example_yaml):
    register(client, example_yaml)
    runner.app_ready = False  # container started, but no address or not listening yet
    client.post(f"{REPORT}/start")
    assert client.get(REPORT).json()["status"] == "starting"
    assert client.get(f"{APP}/").status_code == 409
    runner.app_ready = True
    assert client.get(REPORT).json()["status"] == "running"


def test_the_app_never_sees_pipeyard_credentials(started, runner, settings, org):
    token = Auth(settings).dev_token(org)
    plain = TestClient(started.app, follow_redirects=False)
    cookie = f"theme=dark; pipeyard_token={token}; session=abc"
    by_cookie = plain.get(f"{APP}/", headers={"Cookie": cookie, "X-Custom": "kept"}).json()
    assert by_cookie["headers"]["cookie"] == "theme=dark; session=abc"  # the app's own cookies
    assert by_cookie["headers"]["x-custom"] == "kept"

    by_bearer = started.get(f"{APP}/").json()
    assert "authorization" not in by_bearer["headers"] and "cookie" not in by_bearer["headers"]
    assert token not in json.dumps([by_cookie, by_bearer])
    received = json.dumps([a.requests for a in runner.apps.values()])
    assert token not in received and "pipeyard_token" not in received


def test_websockets_are_proxied(started, settings, org):
    token = Auth(settings).dev_token(org)
    plain = TestClient(started.app)
    with plain.websocket_connect(
        f"{APP}/_stcore/stream", headers={"Cookie": f"pipeyard_token={token}"}
    ) as ws:
        ws.send_text("hello")
        assert ws.receive_text() == f"{org} heard hello"
        ws.send_text("again")
        assert ws.receive_text() == f"{org} heard again"


def test_another_org_cannot_reach_the_app(started, client_for, settings):
    other = client_for("org-outsider")
    assert other.get(f"{APP}/").status_code == 404
    assert other.get(REPORT).status_code == 404
    assert other.post(f"{REPORT}/stop").status_code == 404
    assert started.get(REPORT).json()["status"] == "running"  # and they could not stop it

    anonymous = client_for(None)
    assert anonymous.get(f"{APP}/").status_code == 303  # to the sign-in page

    for headers in (
        {},
        {"Cookie": "pipeyard_token=not-a-token"},
        {"Cookie": f"pipeyard_token={Auth(settings).dev_token('org-outsider')}"},
    ):
        with pytest.raises(Exception):  # noqa: B017, PT011 - the handshake is refused
            with TestClient(started.app).websocket_connect(f"{APP}/ws", headers=headers):
                pass


def test_same_report_in_two_orgs_runs_two_apps(client_for, runner, example_yaml):
    answers = {}
    for org in ("org-apps-a", "org-apps-b"):
        client = client_for(org)
        register(client, example_yaml)
        client.post(f"{REPORT}/start")
        assert client.get(REPORT).json()["status"] == "running"
    for org in ("org-apps-a", "org-apps-b"):
        answers[org] = client_for(org).get(f"{APP}/").json()["app"]
    assert answers == {"org-apps-a": "org-apps-a", "org-apps-b": "org-apps-b"}
    assert len(runner.apps) == 2


def test_stop(started, runner):
    assert started.post(f"{REPORT}/stop").json()["status"] == "stopped"
    assert runner.apps == {}
    assert started.get(f"{APP}/").status_code == 409
    assert started.get(REPORT).json() | {"error": None} == started.get(REPORT).json()
    assert started.post(f"{REPORT}/start").json()["status"] == "starting"  # and it restarts


def test_app_that_crashes_is_reported(started, runner):
    (instance,) = runner.apps
    runner.states[instance] = RunState(False, 3, "ModuleNotFoundError: No module named 'x'")
    status = started.get(REPORT).json()
    assert status["status"] == "stopped"
    assert status["error"] == "the app exited (code 3)\nModuleNotFoundError: No module named 'x'"
    assert started.get(f"{APP}/").status_code in (409, 502)
    assert (
        "stopped with an error"
        in started.get("/pipelines/fake-transactions/reports/transactions-dashboard").text
    )


def test_app_stops_at_the_end_of_its_lifetime(started, runner, app, org):
    # Killed by the runner's own timeout: a normal end, not an error.
    (instance,) = runner.apps
    runner.states[instance] = RunState(False, TIMEOUT_EXIT_CODE, "timed out")
    status = started.get(REPORT).json()
    assert (status["status"], status["error"]) == ("stopped", None)

    # Still running past its expiry (the runner's timer was lost): pipeyard stops it.
    started.post(f"{REPORT}/start")
    assert started.get(REPORT).json()["status"] == "running"
    storage = app.state.storage
    record = storage.get_report_app(org, "fake-transactions", "transactions-dashboard")
    record.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    storage.put_report_app(record)
    assert started.get(REPORT).json()["status"] == "stopped"
    assert runner.apps == {}


def test_start_failure_is_shown(client, runner, example_yaml, monkeypatch):
    register(client, example_yaml)

    def boom(*args):
        raise RuntimeError("docker run failed: no such image")

    monkeypatch.setattr(runner, "start", boom)
    status = client.post(f"{REPORT}/start").json()
    assert status["status"] == "stopped" and "no such image" in status["error"]


def test_unknown_report_is_404(client, example_yaml):
    register(client, example_yaml)
    assert client.get("/api/pipelines/fake-transactions/reports/nope").status_code == 404
    assert client.post("/api/pipelines/fake-transactions/reports/nope/start").status_code == 404
    assert client.get("/apps/fake-transactions/nope/").status_code == 404


def test_ui_flow(client, runner, example_yaml):
    register(client, example_yaml)
    page = "/pipelines/fake-transactions/reports/transactions-dashboard"
    assert "stopped" in client.get("/pipelines/fake-transactions").text  # listing does not wake it

    runner.app_ready = False
    starting = client.get(page).text  # opening the report wakes it up
    assert "starting" in starting and 'hx-trigger="every 2s"' in starting
    assert "<iframe" not in starting and len(runner.apps) == 1
    assert "starting" in client.get("/pipelines/fake-transactions").text

    runner.app_ready = True
    running = client.get(f"{page}/state").text
    assert f'<iframe class="card report-frame" src="{APP}/"' in running
    assert "hx-trigger" not in running  # no polling, or the iframe would be reloaded
    assert "after 5 min unused" in running
    assert "Stop" in running and "Open in new tab" in running

    assert "Start report" in client.post(f"{page}/stop").text
    assert runner.apps == {}


def test_opening_a_crashed_report_does_not_restart_it(started, runner):
    (instance,) = runner.apps
    runner.states[instance] = RunState(False, 1, "boom")
    page = started.get("/pipelines/fake-transactions/reports/transactions-dashboard").text
    assert "stopped with an error" in page and "Start report" in page
    assert len(runner.started) == 1  # shown, not silently retried on every reload


# --- scale to zero ---------------------------------------------------------------------


def test_idle_app_is_stopped(started, runner, app):
    sweep = app.state.sweep_idle
    assert sweep() == []  # just used
    assert sweep(time.time() + 299) == []
    (stopped,) = sweep(time.time() + 301)  # default idle timeout is 300 s
    assert stopped[1:] == ("fake-transactions", "transactions-dashboard")
    assert runner.apps == {}  # the container is gone
    status = started.get(REPORT).json()
    assert (status["status"], status["error"]) == ("stopped", None)
    assert sweep(time.time() + 9999) == []  # nothing left to stop


def test_requests_keep_an_app_alive(started, runner, app, monkeypatch):
    sweep = app.state.sweep_idle
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    for _ in range(5):  # used every 4 minutes for 20 minutes
        clock[0] += 240
        assert started.get(f"{APP}/").status_code == 200
        assert sweep() == []
    clock[0] += 301  # then left alone
    assert len(sweep()) == 1 and runner.apps == {}


def test_an_open_websocket_keeps_an_app_alive(started, runner, app, settings, org):
    sweep = app.state.sweep_idle
    token = Auth(settings).dev_token(org)
    with TestClient(started.app).websocket_connect(
        f"{APP}/_stcore/stream", headers={"Cookie": f"pipeyard_token={token}"}
    ) as ws:
        ws.send_text("hi")
        ws.receive_text()
        assert sweep(time.time() + 5000) == []  # the tab is open: in use, however quiet
    assert sweep(time.time() + 299) == []  # closed just now: the idle clock starts here
    assert len(sweep(time.time() + 301)) == 1


def test_idle_timeout_comes_from_pipeline_yaml(client, runner, app, example_yaml):
    register(
        client, example_yaml.replace("    cpu: 512", "    idle_timeout_seconds: 60\n    cpu: 512")
    )
    client.post(f"{REPORT}/start")
    assert client.get(REPORT).json()["idle_timeout_seconds"] == 60
    assert app.state.sweep_idle(time.time() + 59) == []
    assert len(app.state.sweep_idle(time.time() + 61)) == 1


def test_after_a_restart_the_idle_clock_starts_again(started, runner, settings):
    """Usage is kept in memory. A new process must not stop apps it knows nothing about,
    nor forget them: it counts them as used from when it first sees them."""
    restarted = create_app(settings, runner)
    client = TestClient(restarted, follow_redirects=False)
    client.headers.update(started.headers)
    sweep = restarted.state.sweep_idle
    assert sweep(time.time() + 9999) == []  # not seen yet: left alone
    assert client.get(REPORT).json()["status"] == "running"  # seen now
    assert sweep(time.time() + 299) == []
    assert len(sweep(time.time() + 301)) == 1 and runner.apps == {}


def test_a_waking_app_is_not_stopped_while_someone_waits(
    client, runner, app, example_yaml, monkeypatch
):
    register(client, example_yaml)
    runner.app_ready = False  # a slow start
    client.post(f"{REPORT}/start")
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    for _ in range(4):  # the report page keeps polling, 200 s apart here
        clock[0] += 200
        assert client.get(REPORT).json()["status"] == "starting"
        assert app.state.sweep_idle() == []
    clock[0] += 301  # the person gave up and closed the page
    assert len(app.state.sweep_idle()) == 1 and runner.apps == {}
