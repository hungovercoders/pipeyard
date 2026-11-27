"""Data contracts in the Open Data Contract Standard: registered per dataset, enforced per run."""

from pathlib import Path
from urllib.parse import urlencode

import pytest
import yaml
from conftest import register

from pipeyard import contract, datacontract
from pipeyard.datacontract import Property, type_problem

P = "/api/pipelines/fake-transactions"
CONTRACT = "/api/datasets/transactions/contract"
EXAMPLE = (
    Path(__file__).parent.parent / "examples/pipelines/fake-transactions/transactions.odcs.yaml"
)

# What the fake runner writes: ts TIMESTAMP, amount DOUBLE, owner VARCHAR.
BASE = """
apiVersion: v3.2.0
kind: DataContract
id: test-transactions
name: Test transactions
version: 2.1.0
status: active
description: {purpose: For tests.}
schema:
  - name: transactions
    properties:
      - {name: ts, logicalType: timestamp, required: true}
      - {name: amount, logicalType: number, physicalType: DOUBLE, description: How much.}
      - {name: owner, logicalType: string}
"""


def put(client, text, path=CONTRACT):
    return client.put(path, content=text, headers={"Content-Type": "application/yaml"})


def run_once(client):
    run = client.post(f"{P}/runs").json()
    return client.get(f"/api/runs/{run['run_id']}").json()


def with_property(line: str) -> str:
    """BASE with one more property, or with the property for that column replaced."""
    new = yaml.safe_load(line)
    doc = yaml.safe_load(BASE)
    column = new.get("physicalName", new["name"])
    properties = [p for p in doc["schema"][0]["properties"] if p["name"] != column]
    doc["schema"][0]["properties"] = [*properties, new]
    return yaml.safe_dump(doc, sort_keys=False)


# --- registering ---------------------------------------------------------------------------


def test_register_read_back_and_delete(client):
    assert client.get(CONTRACT).status_code == 404
    summary = put(client, BASE).json()
    assert (summary["name"], summary["version"], summary["status"]) == (
        "Test transactions", "2.1.0", "active",
    )  # fmt: skip
    assert summary["purpose"] == "For tests." and summary["api_version"] == "v3.2.0"
    assert summary["properties"][1] == {
        "name": "amount", "type": "DOUBLE", "logical_type": "number", "required": False,
        "description": "How much.",
    }  # fmt: skip
    stored = client.get(CONTRACT)
    assert stored.text == BASE and stored.headers["content-type"].startswith("application/yaml")
    assert client.delete(CONTRACT).status_code == 204
    assert client.get(CONTRACT).status_code == 404


def test_the_example_contract_is_valid_and_keeps_fields_pipeyard_does_not_use():
    dc = datacontract.parse(EXAMPLE.read_text(), "transactions")
    assert dc.label == "'Fake card transactions' version 1.0.0"
    assert [p.column for p in dc.table("transactions").properties] == [
        "transaction_id", "ts", "customer_id", "category", "amount",
    ]  # fmt: skip
    assert dc.model_extra["domain"] == "payments"  # not modelled, not lost
    assert dc.table("transactions").properties[0].primaryKey is True


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (lambda d: d.update(apiVersion="v2.2.2"), "ODCS v2 is not supported"),
        (lambda d: d.update(apiVersion="3"), "apiVersion: String should match"),
        (lambda d: d.update(kind="Dataset"), "kind: Input should be 'DataContract'"),
        (lambda d: d.pop("id"), "id: Field required"),
        (lambda d: d.pop("version"), "version: Field required"),
        (lambda d: d.pop("schema"), "schema: Field required"),
        (lambda d: d["schema"][0].update(name="orders"), "no object named 'transactions'"),
        (lambda d: d["schema"][0].update(properties=[]), "list at least one property"),
        (
            lambda d: d["schema"][0]["properties"][0].update(logicalType="text"),
            "schema.0.properties.0.logicalType: Input should be 'string', 'date'",
        ),
        (
            lambda d: d["schema"][0]["properties"].append({"name": "ts"}),
            "column names must be unique",
        ),
    ],
)
def test_invalid_contracts_are_refused_with_a_reason(client, change, expected):
    doc = yaml.safe_load(BASE)
    change(doc)
    response = put(client, yaml.safe_dump(doc))
    assert response.status_code == 422
    assert any(expected in e for e in response.json()["detail"]), response.json()
    assert client.get(CONTRACT).status_code == 404  # nothing was saved


def test_not_yaml_and_not_a_mapping(client):
    assert "not valid YAML" in put(client, "a: [unclosed").json()["detail"][0]
    assert "must be a YAML mapping" in put(client, "- a list").json()["detail"][0]


def test_a_schema_object_can_match_by_physical_name():
    doc = yaml.safe_load(BASE)
    doc["schema"][0].update(name="Transactions table", physicalName="transactions")
    assert datacontract.parse(yaml.safe_dump(doc), "transactions").table("transactions")


# --- enforcing -----------------------------------------------------------------------------


def test_a_conforming_run_succeeds_and_records_the_contract_version(client, example_yaml):
    register(client, example_yaml)
    put(client, BASE)
    run = run_once(client)
    assert (run["status"], run["row_count"], run["contract_version"]) == ("succeeded", 30, "2.1.0")
    (dataset,) = client.get("/api/datasets").json()
    assert dataset["contract"]["version"] == "2.1.0" and dataset["row_count"] == 30


@pytest.mark.parametrize(
    ("line", "problem"),
    [
        ("{name: customer_id, logicalType: string}", "- column 'customer_id' is missing"),
        (
            "{name: when, physicalName: ts, logicalType: integer}",
            "- column 'ts' is TIMESTAMP, but the contract's logical type is integer",
        ),
        (
            "{name: total, physicalName: amount, physicalType: 'DECIMAL(18,2)'}",
            "- column 'amount' is DOUBLE, the contract says DECIMAL(18,2)",
        ),
    ],
)
def test_a_run_that_breaks_the_contract_fails_and_adds_nothing(
    client, storage, org, example_yaml, line, problem
):
    register(client, example_yaml)
    assert run_once(client)["status"] == "succeeded"  # before there was a contract
    assert put(client, with_property(line)).status_code == 200

    run = run_once(client)
    assert run["status"] == "failed" and run["contract_version"] == "2.1.0"
    assert run["error"] == f"data contract 'Test transactions' version 2.1.0 is not met:\n{problem}"

    (dataset,) = client.get("/api/datasets").json()
    assert (dataset["runs"], dataset["row_count"]) == (1, 30)  # only the earlier run counts
    prefix = contract.output_prefix(org, "transactions", run["run_id"])
    assert storage.s3.list_objects_v2(Bucket=storage.bucket, Prefix=prefix)["KeyCount"] == 0


def test_required_columns_must_have_no_nulls(client, runner, example_yaml):
    register(client, example_yaml)
    doc = yaml.safe_load(BASE)
    doc["schema"][0]["properties"][1]["required"] = True  # amount
    put(client, yaml.safe_dump(doc))
    assert run_once(client)["status"] == "succeeded"
    runner.behaviour = "nulls"  # one of the 30 amounts is null
    run = run_once(client)
    assert run["status"] == "failed"
    assert run["error"].endswith("- column 'amount' is required but has 1 null values")


def test_every_problem_is_listed(client, example_yaml):
    register(client, example_yaml)
    doc = yaml.safe_load(BASE)
    doc["schema"][0]["properties"] = [
        {"name": "a"},
        {"name": "b"},
        {"name": "owner", "logicalType": "boolean"},
    ]
    assert put(client, yaml.safe_dump(doc)).status_code == 200
    error = run_once(client)["error"]
    assert error.count("\n- ") == 3 and "'a' is missing" in error and "'b' is missing" in error


def test_columns_the_contract_does_not_mention_are_allowed(client, example_yaml):
    register(client, example_yaml)
    doc = yaml.safe_load(BASE)
    doc["schema"][0]["properties"] = doc["schema"][0]["properties"][:1]  # only ts
    put(client, yaml.safe_dump(doc))
    assert run_once(client)["status"] == "succeeded"


def test_removing_the_contract_stops_the_checks(client, example_yaml):
    register(client, example_yaml)
    put(client, with_property("{name: missing}"))
    assert run_once(client)["status"] == "failed"
    client.delete(CONTRACT)
    run = run_once(client)
    assert (run["status"], run["contract_version"]) == ("succeeded", None)


def test_contracts_are_per_org(client, client_for, example_yaml):
    register(client, example_yaml)
    put(client, with_property("{name: missing}"))
    other = client_for("org-contracts-b")
    assert other.get(CONTRACT).status_code == 404  # cannot read it
    register(other, example_yaml)
    assert run_once(other)["status"] == "succeeded"  # and is not bound by it
    put(other, BASE)
    assert "missing" in client.get(CONTRACT).text  # nor can it replace it
    assert other.delete(CONTRACT).status_code == 204
    assert client.get(CONTRACT).status_code == 200


def test_a_contract_can_exist_before_any_data(client):
    put(client, BASE.replace("transactions", "future_data"), "/api/datasets/future_data/contract")
    (dataset,) = client.get("/api/datasets").json()
    assert (dataset["dataset"], dataset["runs"], dataset["contract"]["name"]) == (
        "future_data", 0, "Test future_data",
    )  # fmt: skip
    assert client.get("/datasets/future_data").status_code == 200


@pytest.mark.parametrize("name", ["Bad-Name", "..%2Fx", "a b"])
def test_dataset_names_are_validated(client, name):
    assert put(client, BASE, f"/api/datasets/{name}/contract").status_code in (404, 422)


# --- type matching -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("declared", "actual", "ok"),
    [
        ({"logicalType": "string"}, "VARCHAR", True),
        ({"logicalType": "string"}, "BIGINT", False),
        ({"logicalType": "integer"}, "BIGINT", True),
        ({"logicalType": "integer"}, "UTINYINT", True),
        ({"logicalType": "integer"}, "DOUBLE", False),
        ({"logicalType": "number"}, "INTEGER", True),
        ({"logicalType": "number"}, "DECIMAL(18,2)", True),
        ({"logicalType": "number"}, "VARCHAR", False),
        ({"logicalType": "boolean"}, "BOOLEAN", True),
        ({"logicalType": "date"}, "DATE", True),
        ({"logicalType": "date"}, "TIMESTAMP", True),
        ({"logicalType": "timestamp"}, "TIMESTAMP WITH TIME ZONE", True),
        ({"logicalType": "timestamp"}, "DATE", False),
        ({"logicalType": "array"}, "VARCHAR[]", True),
        ({"logicalType": "array"}, "VARCHAR", False),
        ({"logicalType": "string"}, "VARCHAR[]", False),
        ({"logicalType": "object"}, "STRUCT(a INTEGER)", True),
        ({"logicalType": "map"}, "MAP(VARCHAR, INTEGER)", True),
        ({}, "ANYTHING", True),
        ({"physicalType": "varchar(18)"}, "VARCHAR", True),  # the official example's style
        ({"physicalType": "string"}, "VARCHAR", True),
        ({"physicalType": "int"}, "INTEGER", True),
        ({"physicalType": "INT"}, "BIGINT", False),
        ({"physicalType": "double precision"}, "DOUBLE", True),
        ({"physicalType": "decimal(18, 2)"}, "DECIMAL(18,2)", True),
        ({"physicalType": "DECIMAL(10,2)"}, "DECIMAL(18,2)", False),
        ({"physicalType": "timestamptz"}, "TIMESTAMP WITH TIME ZONE", True),
        ({"physicalType": "TIMESTAMP"}, "TIMESTAMP WITH TIME ZONE", False),
        # physicalType wins over logicalType when both are given
        ({"logicalType": "number", "physicalType": "BIGINT"}, "DOUBLE", False),
    ],
)
def test_type_matching(declared, actual, ok):
    assert (type_problem(Property(name="c", **declared), actual) is None) is ok


# --- UI ------------------------------------------------------------------------------------


def test_ui(client, example_yaml):
    register(client, example_yaml)
    run_once(client)
    listing = client.get("/datasets").text
    assert 'href="/datasets/transactions"' in listing and "none" in listing

    page = client.get("/datasets/transactions").text
    assert "Add a data contract" in page and "No data contract" in page

    form = client.get("/datasets/transactions/contract").text
    assert "apiVersion: v3.2.0" in form  # a draft written from the columns the dataset has
    for column in ("name: ts", "logicalType: timestamp", "name: amount", "physicalType: DOUBLE"):
        assert column in form

    def post(text):
        return client.post("/datasets/transactions/contract", content=urlencode({"yaml": text}))

    bad = post(BASE.replace("kind: DataContract", "kind: Nope"))
    assert (
        bad.status_code == 422 and "kind: Input should be" in bad.text and "kind: Nope" in bad.text
    )

    saved = post(BASE.replace("\n", "\r\n"))  # browsers send CRLF
    assert saved.status_code == 303 and saved.headers["location"] == "/datasets/transactions"

    page = client.get("/datasets/transactions").text
    for text in ("Test transactions", "2.1.0", "For tests.", "How much.", "Edit contract", "ODCS"):
        assert text in page
    assert "Test transactions" in client.get("/datasets").text
    assert "name: Test transactions" in client.get("/datasets/transactions/contract").text
    assert client.get("/datasets/nope").status_code == 404


def test_the_starter_draft_is_itself_a_valid_contract():
    schema = [
        {"name": "id", "type": "BIGINT"},
        {"name": "at", "type": "TIMESTAMP WITH TIME ZONE"},
        {"name": "tags", "type": "VARCHAR[]"},
        {"name": "price", "type": "DECIMAL(18,2)"},
    ]
    dc = datacontract.parse(datacontract.starter("orders", schema), "orders")
    types = {p.column: p.logicalType for p in dc.table("orders").properties}
    assert types == {"id": "integer", "at": "timestamp", "tags": "string", "price": "number"}
    assert datacontract.parse(datacontract.starter("empty", []), "empty")
