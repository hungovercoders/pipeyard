"""The main loop: register -> trigger -> dataset, through the API and the UI."""

from conftest import register

from pipeyard import contract


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


def test_datasets_count_every_succeeded_run(client, example_yaml):
    register(client, example_yaml)
    for _ in range(2):
        run = client.post("/api/pipelines/fake-transactions/runs").json()
        client.get(f"/api/runs/{run['run_id']}")
    (dataset,) = client.get("/api/datasets").json()
    assert (dataset["runs"], dataset["row_count"]) == (2, 60)


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


def test_failed_runs_leave_nothing_behind(client, runner, storage, org, example_yaml):
    """Report apps read the dataset's files directly, so a failed run's output is removed."""
    register(client, example_yaml)
    runner.behaviour = "no_manifest"  # uploads Parquet, then "forgets" the manifest
    run = client.post("/api/pipelines/fake-transactions/runs").json()
    prefix = contract.output_prefix(org, "transactions", run["run_id"])
    assert storage.s3.list_objects_v2(Bucket=storage.bucket, Prefix=prefix)["KeyCount"] == 1
    assert client.get(f"/api/runs/{run['run_id']}").json()["status"] == "failed"
    assert storage.s3.list_objects_v2(Bucket=storage.bucket, Prefix=prefix)["KeyCount"] == 0
    assert client.get("/api/datasets").json() == []


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

    assert "Transactions dashboard" in client.get("/pipelines/fake-transactions").text
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
    assert "Edit fake-transactions" in edit.text and "Transactions dashboard" in edit.text
    assert "\r" not in edit.text
    post(example_yaml.replace("cpu: 256", "cpu: 512").replace("memory: 512", "memory: 1024"))
    assert client.get("/api/pipelines/fake-transactions").json()["cpu"] == 512
    assert len(client.get("/api/pipelines").json()) == 1


def test_register_form_cannot_show_another_orgs_yaml(client_for, example_yaml):
    from urllib.parse import urlencode

    client_for("org-form-a").post("/register", content=urlencode({"yaml": example_yaml}))
    other = client_for("org-form-b").get("/register?pipeline=fake-transactions")
    assert "Transactions dashboard" not in other.text and "name: my-pipeline" in other.text
