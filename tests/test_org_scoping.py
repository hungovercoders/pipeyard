"""Phase 2: org_id comes from the token only, and one org can never read another's data."""

import time

import jwt
import pytest
from conftest import register

from pipeyard import contract, query
from pipeyard.auth import ORG_CLAIM

REPORT = "/api/pipelines/fake-transactions/reports/transactions-dashboard"


@pytest.fixture
def two_orgs(client_for, example_yaml):
    """Org A has a pipeline with one successful run. Org B has nothing."""
    a, b = client_for("org-a-" + contract.new_run_id()[-8:]), client_for("org-b")
    register(a, example_yaml)
    run = a.post("/api/pipelines/fake-transactions/runs").json()
    assert a.get(f"/api/runs/{run['run_id']}").json()["status"] == "succeeded"
    return a, b, run["run_id"]


def test_org_b_sees_nothing_of_org_a(two_orgs):
    a, b, run_id = two_orgs
    assert a.get("/api/pipelines").json() and a.get("/api/datasets").json()
    assert a.get(REPORT).status_code == 200

    assert b.get("/api/pipelines").json() == []
    assert b.get("/api/datasets").json() == []
    for path in (
        "/api/pipelines/fake-transactions",
        "/api/pipelines/fake-transactions/runs",
        f"/api/runs/{run_id}",
        REPORT,
        "/pipelines/fake-transactions",
        "/pipelines/fake-transactions/runs",
        "/pipelines/fake-transactions/reports/transactions-dashboard",
        "/apps/fake-transactions/transactions-dashboard/",
    ):
        assert b.get(path).status_code == 404, path
    assert b.post("/api/pipelines/fake-transactions/runs").status_code == 404
    assert b.post("/pipelines/fake-transactions/runs").status_code == 404
    assert "fake-transactions" not in b.get("/pipelines").text
    assert "transactions" not in b.get("/datasets").text.split("<main>")[1]


def test_org_cannot_be_chosen_by_the_caller(two_orgs, runner, example_yaml):
    """Query strings, headers and bodies naming another org change nothing."""
    a, b, run_id = two_orgs
    org_a = runner.started[-1]["org_id"]
    spoof = {"X-Org-Id": org_a, "ORG_ID": org_a}
    assert b.get(f"/api/pipelines?org_id={org_a}", headers=spoof).json() == []
    assert b.get(f"/api/runs/{run_id}?org_id={org_a}", headers=spoof).status_code == 404
    assert b.get(f"/api/datasets?org_id={org_a}", headers=spoof).json() == []

    # Registering with an org_id in the body is rejected, not honoured.
    response = register(b, example_yaml + f"org_id: {org_a}\n")
    assert response.status_code == 422 and "org_id" in response.json()["detail"][0]

    # A run triggered by B lands under B's prefix whatever else is sent.
    register(b, example_yaml)
    b.post(f"/api/pipelines/fake-transactions/runs?org_id={org_a}", headers=spoof)
    started = runner.started[-1]
    assert started["org_id"] == "org-b" and started["env"]["ORG_ID"] == "org-b"
    assert "/orgs/org-b/datasets/" in started["env"]["OUTPUT_URI"]


def test_duckdb_connections_are_locked_to_the_org(two_orgs, settings):
    """A connection that has been handed out is locked: config cannot be changed back."""
    con, root = query._connect(settings, "org-b")
    assert root.endswith("/orgs/org-b/")
    with pytest.raises(query.duckdb.Error, match="locked"):
        con.execute("set enable_external_access = true")
    with pytest.raises(query.duckdb.Error, match="locked"):
        con.execute("set allowed_directories = ['s3://']")


def _token(settings, **overrides) -> str:
    claims = {"sub": "x", ORG_CLAIM: "org-a", "exp": int(time.time()) + 60} | overrides
    secret = claims.pop("secret", settings.dev_secret)
    return jwt.encode({k: v for k, v in claims.items() if v is not None}, secret, "HS256")


@pytest.mark.parametrize(
    "overrides",
    [
        {"secret": "not-the-signing-secret-not-the-signing-secret"},
        {"exp": int(time.time()) - 10},
        {ORG_CLAIM: None},
        {ORG_CLAIM: "../org-a"},
        {ORG_CLAIM: "org-a/../../x"},
        {ORG_CLAIM: "*"},
        {ORG_CLAIM: ""},
        {ORG_CLAIM: ["org-a"]},
    ],
)
def test_bad_tokens_are_rejected(client_for, settings, overrides):
    client = client_for(None)
    headers = {"Authorization": f"Bearer {_token(settings, **overrides)}"}
    assert client.get("/api/pipelines", headers=headers).status_code == 401
    cookie = {"Cookie": f"pipeyard_token={headers['Authorization'][7:]}"}
    assert client.get("/api/datasets", headers=cookie).status_code == 401


def test_unsigned_token_is_rejected(client_for):
    token = jwt.encode({ORG_CLAIM: "org-a", "exp": int(time.time()) + 60}, None, algorithm="none")
    response = client_for(None).get("/api/pipelines", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401


@pytest.mark.parametrize(
    "path",
    [
        "/api/runs/..%2F..%2Forg-a%2Fruns%2Fx",
        "/api/runs/not-a-run-id",
        "/api/pipelines/..%2F..%2Forg-a",
        "/api/pipelines/Bad_Name/runs",
        "/api/pipelines/fake-transactions/reports/..%2Fx",
    ],
)
def test_path_parameters_cannot_escape(client, path):
    assert client.get(path).status_code in (404, 422)


def test_every_data_route_requires_a_token(app, client_for):
    """Guards against a future route forgetting the org dependency."""
    open_paths = {"/login", "/logout", "/dev/token", "/healthz", "/"}
    anonymous = client_for(None)
    checked = 0
    for route in app.routes:
        if route.path in open_paths:
            continue
        path = (
            route.path.replace("{name}", "fake-transactions")
            .replace("{slug}", "transactions-dashboard")
            .replace("{path:path}", "x")
            .replace("{run_id}", "20260101t000000z-abcdef12")
        )
        if not getattr(route, "methods", None):
            continue  # the WebSocket route: covered in test_reports.py
        for method in route.methods - {"HEAD", "OPTIONS"}:
            response = anonymous.request(method, path)
            expected = 401 if path.startswith("/api/") else 303
            assert response.status_code == expected, f"{method} {path}"
            checked += 1
    assert checked >= 13
