"""Phase 1: register -> trigger -> dataset -> report, through the API and the UI."""

from conftest import register


def test_demo_flow(client, runner, org, example_yaml):
    assert register(client, example_yaml).status_code == 201
    assert [p["name"] for p in client.get("/api/pipelines").json()] == ["fake-transactions"]

    run = client.post("/api/pipelines/fake-transactions/runs").json()
    assert run["status"] == "running"
    env = runner.started[0]["env"]
    assert env["ORG_ID"] == org and env["RUN_ID"] == run["run_id"]
    assert env["OUTPUT_URI"].endswith(f"/orgs/{org}/datasets/transactions/run={run['run_id']}/")
    assert env["AWS_ENDPOINT_URL"] == "https://127.0.0.1:9297"

    run = client.get(f"/api/runs/{run['run_id']}").json()
    assert run["status"] == "succeeded" and run["row_count"] == 30
    assert "handle" not in run

    runs = client.get("/api/pipelines/fake-transactions/runs").json()
    assert [r["status"] for r in runs] == ["succeeded"]

    (dataset,) = client.get("/api/datasets").json()
    assert dataset["dataset"] == "transactions" and dataset["row_count"] == 30
    assert {"name": "amount", "type": "DOUBLE"} in dataset["schema"]

    report = client.get("/api/pipelines/fake-transactions/reports/daily-volume").json()
    assert [c["name"] for c in report["columns"]] == ["day", "total"]
    assert len(report["rows"]) == 3
    assert sum(r["total"] for r in report["rows"]) == sum(range(30))


def test_reports_read_every_committed_run(client, example_yaml):
    register(client, example_yaml)
    for _ in range(2):
        run = client.post("/api/pipelines/fake-transactions/runs").json()
        client.get(f"/api/runs/{run['run_id']}")
    report = client.get("/api/pipelines/fake-transactions/reports/daily-volume").json()
    assert sum(r["total"] for r in report["rows"]) == 2 * sum(range(30))
    assert client.get("/api/datasets").json()[0]["runs"] == 2


def test_failed_runs(client, runner, example_yaml):
    register(client, example_yaml)
    expected = {
        "exit1": "container exited with code 1\nTraceback: boom",
        "no_manifest": "container exited 0 but wrote no _manifest.json",
        "bad_manifest": "_manifest.json is for transactions/someone-elses-run",
    }
    for behaviour, message in expected.items():
        runner.behaviour = behaviour
        run = client.post("/api/pipelines/fake-transactions/runs").json()
        run = client.get(f"/api/runs/{run['run_id']}").json()
        assert run["status"] == "failed", behaviour
        assert run["error"].startswith(message), behaviour


def test_uncommitted_output_is_not_readable(client, runner, example_yaml):
    """Parquet without a manifest (a failed run) must not leak into reports or datasets."""
    register(client, example_yaml)
    runner.behaviour = "no_manifest"
    client.post("/api/pipelines/fake-transactions/runs")
    assert client.get("/api/datasets").json() == []
    response = client.get("/api/pipelines/fake-transactions/reports/daily-volume")
    assert response.status_code == 409
    assert "no successful runs" in response.json()["detail"]


def test_start_failure_is_a_failed_run(client, runner, example_yaml, monkeypatch):
    register(client, example_yaml)

    def boom(*args):
        raise RuntimeError("docker run failed: no such image")

    monkeypatch.setattr(runner, "start", boom)
    run = client.post("/api/pipelines/fake-transactions/runs").json()
    assert run["status"] == "failed" and "no such image" in run["error"]


def test_ui_pages(client, runner, example_yaml):
    assert "No pipelines yet" in client.get("/pipelines").text
    assert "No datasets yet" in client.get("/datasets").text
    register(client, example_yaml)

    detail = client.get("/pipelines/fake-transactions")
    assert "Trigger run" in detail.text and "No runs yet" in detail.text
    assert "hx-trigger" not in detail.text  # nothing running, so no polling

    runner.behaviour = "running"
    triggered = client.post("/pipelines/fake-transactions/runs")
    assert "running" in triggered.text and 'hx-trigger="every 2s"' in triggered.text

    runner.behaviour = "ok"
    client.post("/pipelines/fake-transactions/runs")
    assert "succeeded" in client.get("/pipelines/fake-transactions/runs").text
    assert "succeeded" in client.get("/pipelines").text
    assert "transactions" in client.get("/datasets").text

    report = client.get("/pipelines/fake-transactions/reports/daily-volume")
    assert report.status_code == 200
    assert "vegaEmbed" in report.text and '"type": "temporal"' in report.text
    assert "2026-01-01" in report.text
    assert client.get("/pipelines/nope").status_code == 404


def test_local_login_stub(client_for):
    anonymous = client_for(None)
    assert anonymous.get("/api/pipelines").status_code == 401
    redirect = anonymous.get("/pipelines")
    assert redirect.status_code == 303 and redirect.headers["location"].startswith("/login")
    login = anonymous.get("/login?next=/datasets")
    assert login.headers["location"] == "/datasets" and "pipeyard_token" in login.cookies
    token = anonymous.post("/dev/token").json()
    assert token["org_id"] == "demo"


def test_register_form(client, example_yaml):
    from urllib.parse import urlencode

    def post(text):
        return client.post("/register", content=urlencode({"yaml": text}))

    blank = client.get("/register")
    assert "Register pipeline" in blank.text and "name: my-pipeline" in blank.text
    assert 'href="/register"' in client.get("/pipelines").text

    bad = post("name: Bad Name\nimage: i\noutputs: []")
    assert bad.status_code == 422
    assert "name: String should match" in bad.text and "outputs: List should have" in bad.text
    assert "name: Bad Name" in bad.text  # what was typed is kept
    assert client.get("/api/pipelines").json() == []

    saved = post(example_yaml.replace("\n", "\r\n"))  # browsers send CRLF
    assert saved.status_code == 303 and saved.headers["location"] == "/pipelines/fake-transactions"
    assert client.get("/api/pipelines/fake-transactions").json()["cpu"] == 256

    edit = client.get("/register?pipeline=fake-transactions")
    assert "Edit fake-transactions" in edit.text and "Daily volume" in edit.text
    assert "\r" not in edit.text
    post(example_yaml.replace("cpu: 256", "cpu: 512").replace("memory: 512", "memory: 1024"))
    assert client.get("/api/pipelines/fake-transactions").json()["cpu"] == 512
    assert len(client.get("/api/pipelines").json()) == 1


def test_register_form_cannot_show_another_orgs_yaml(client_for, example_yaml):
    from urllib.parse import urlencode

    client_for("org-form-a").post("/register", content=urlencode({"yaml": example_yaml}))
    other = client_for("org-form-b").get("/register?pipeline=fake-transactions")
    assert "Daily volume" not in other.text and "name: my-pipeline" in other.text
