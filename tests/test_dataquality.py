"""ODCS quality rules: library metrics, unique / primaryKey, SQL rules and severities.

The fake runner writes 30 rows: ts (3 distinct days), amount (0.0 .. 29.0, all distinct)
and owner (the org id on every row). With behaviour "nulls", one amount is null.
"""

import pytest
import yaml
from conftest import register

from pipeyard import datacontract, query

P = "/api/pipelines/fake-transactions"
CONTRACT = "/api/datasets/transactions/contract"


def contract_yaml(*, table=(), amount=(), ts=(), owner=(), extra_props=(), flags=None) -> str:
    prop_flags = flags or {}

    def prop(name, logical, rules):
        entry = {"name": name, "logicalType": logical, **prop_flags.get(name, {})}
        return entry | ({"quality": list(rules)} if rules else {})

    doc = {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": "q",
        "name": "Q",
        "version": "1.0.0",
        "schema": [
            {
                "name": "transactions",
                "properties": [
                    prop("ts", "timestamp", ts),
                    prop("amount", "number", amount),
                    prop("owner", "string", owner),
                    *extra_props,
                ],
            }
        ],
    }
    if table:
        doc["schema"][0]["quality"] = list(table)
    return yaml.safe_dump(doc, sort_keys=False)


def put(client, text):
    return client.put(CONTRACT, content=text, headers={"Content-Type": "application/yaml"})


def run_with(client, example_yaml, text, runner=None, behaviour="ok"):
    register(client, example_yaml)
    response = put(client, text)
    assert response.status_code == 200, response.json()
    if runner:
        runner.behaviour = behaviour
    run = client.post(f"{P}/runs").json()
    return client.get(f"/api/runs/{run['run_id']}").json()


PASSING = [
    {"table": [{"metric": "rowCount", "mustBe": 30}]},
    {"table": [{"metric": "rowCount", "mustBeBetween": [10, 50]}]},
    {"table": [{"metric": "rowCount", "mustNotBeBetween": [100, 200]}]},
    {"table": [{"rule": "rowCount", "mustBeGreaterOrEqualTo": 30}]},  # deprecated spelling
    {"table": [{"type": "library", "metric": "rowCount", "mustBeLessOrEqualTo": 30}]},
    {"amount": [{"metric": "nullValues", "mustBe": 0}]},
    {"amount": [{"metric": "duplicateValues", "mustBe": 0}]},
    {"ts": [{"metric": "duplicateValues", "mustBe": 27}]},  # 30 rows, 3 distinct days
    {"ts": [{"metric": "duplicateValues", "unit": "percent", "mustBe": 90}]},
    {"table": [{"metric": "duplicateValues", "arguments": {"properties": ["ts", "amount"]}, "mustBe": 0}]},
    {"owner": [{"metric": "invalidValues", "arguments": {"pattern": "^org-[0-9a-f]{8}$"}, "mustBe": 0}]},
    {"amount": [{"metric": "invalidValues", "arguments": {"validValues": [1.0, 2.0]}, "mustBe": 28}]},
    {"amount": [{"metric": "missingValues", "arguments": {"missingValues": [None, "7.0"]}, "mustBe": 1}]},
    {"amount": [{"type": "sql", "query": "SELECT max({property}) FROM {object}", "mustBe": 29}]},
    {"amount": [{"type": "sql", "query": "SELECT avg(${column}) FROM ${table}", "mustBeBetween": [14, 15]}]},
    {"table": [{"type": "sql", "query": "SELECT count(*) = 30 FROM {object}", "mustBe": True}]},
    {"amount": {"unique": True}},
    {"amount": {"primaryKey": True, "primaryKeyPosition": 1}},
    {"ts": {"primaryKey": True, "primaryKeyPosition": 1}, "amount": {"primaryKey": True, "primaryKeyPosition": 2}},
    # not executable: stored, shown, never a reason to fail
    {"table": [{"type": "text", "description": "Amounts are in pounds."}]},
    {"table": [{"type": "custom", "engine": "soda", "implementation": "row_count > 99999"}]},
]  # fmt: skip


@pytest.mark.parametrize("kwargs", PASSING, ids=lambda k: str(k)[:70])
def test_rules_that_hold(client, example_yaml, kwargs):
    rules = {k: v for k, v in kwargs.items() if isinstance(v, list)}
    flags = {k: v for k, v in kwargs.items() if isinstance(v, dict)}
    run = run_with(client, example_yaml, contract_yaml(**rules, flags=flags))
    assert (run["status"], run["error"], run["warnings"]) == ("succeeded", None, None)


FAILING = [
    ({"table": [{"name": "enough rows", "metric": "rowCount", "mustBeGreaterThan": 100}]},
     "quality rule 'enough rows' is not met: rowCount must be greater than 100, but it is 30"),
    ({"table": [{"id": "rc", "metric": "rowCount", "mustBeBetween": [1, 10]}]},
     "quality rule 'rc' is not met: rowCount must be between 1 and 10, but it is 30"),
    ({"ts": [{"metric": "duplicateValues", "mustBe": 0}]},
     "quality rule 'duplicateValues' on column 'ts' is not met: duplicateValues must be 0, but it is 27"),
    ({"owner": [{"name": "known owner", "metric": "invalidValues",
                 "arguments": {"validValues": ["nobody"]}, "mustBe": 0}]},
     "quality rule 'known owner' on column 'owner' is not met: invalidValues must be 0, but it is 30"),
    ({"owner": [{"metric": "invalidValues", "arguments": {"pattern": "^[A-Z]+$"}, "unit": "percent",
                 "mustBeLessThan": 5}]},
     "is not met: invalidValues must be less than 5%, but it is 100%"),
    ({"amount": [{"name": "cap", "type": "sql", "query": "SELECT max({property}) FROM {object}",
                  "mustBeLessThan": 20}]},
     "quality rule 'cap' on column 'amount' is not met: query result must be less than 20, but it is 29"),
    ({"table": [{"type": "sql", "query": "SELECT count(*) > 99 FROM {object}", "mustBe": True}]},
     "is not met: query result must be true, but it is false"),
    ({"ts": {"unique": True}},
     "quality rule unique on column 'ts' is not met: values must be unique, but it is 27"),
    ({"ts": {"primaryKey": True}},
     "quality rule primary key (ts) is not met: values must be unique, but it is 27"),
    ({"table": [{"metric": "duplicateValues", "arguments": {"properties": ["ts", "owner"]}, "mustBe": 0}]},
     "is not met: duplicateValues must be 0, but it is 27"),
]  # fmt: skip


@pytest.mark.parametrize(("kwargs", "message"), FAILING, ids=lambda k: str(k)[:70])
def test_rules_that_fail_the_run(client, storage, org, example_yaml, kwargs, message):
    rules = {k: v for k, v in kwargs.items() if isinstance(v, list)}
    flags = {k: v for k, v in kwargs.items() if isinstance(v, dict)}
    run = run_with(client, example_yaml, contract_yaml(**rules, flags=flags))
    assert run["status"] == "failed"
    assert run["error"].startswith("data contract 'Q' version 1.0.0 is not met:\n- ")
    assert message in run["error"]
    assert client.get("/api/datasets").json()[0]["runs"] == 0  # nothing was added


def test_null_and_percent_rules(client, runner, example_yaml):
    rules = [
        {"name": "strict", "metric": "nullValues", "mustBe": 0},
        {"name": "lenient", "metric": "nullValues", "unit": "percent", "mustBeLessThan": 5},
        {"name": "tight", "metric": "nullValues", "unit": "percent", "mustBeLessThan": 1},
    ]
    run = run_with(client, example_yaml, contract_yaml(amount=rules), runner, "nulls")
    assert (
        "'strict' on column 'amount' is not met: nullValues must be 0, but it is 1" in run["error"]
    )
    assert (
        "'tight' on column 'amount' is not met: nullValues must be less than 1%, but it is 3.33%"
        in run["error"]
    )
    assert "lenient" not in run["error"]  # 1 of 30 is 3.33%, under 5%


def test_warnings_do_not_fail_the_run(client, example_yaml):
    rules = [
        {"name": "ideal size", "metric": "rowCount", "mustBeGreaterThan": 1000, "severity": "warning"},
        {"name": "fyi", "metric": "rowCount", "mustBe": 31, "severity": "info"},
        {"name": "has rows", "metric": "rowCount", "mustBeGreaterThan": 0, "severity": "error"},
    ]  # fmt: skip
    run = run_with(client, example_yaml, contract_yaml(table=rules))
    assert (run["status"], run["error"], run["row_count"]) == ("succeeded", None, 30)
    assert run["warnings"] == (
        "- quality rule 'ideal size' is not met: rowCount must be greater than 1,000, but it is 30\n"
        "- quality rule 'fyi' is not met: rowCount must be 31, but it is 30"
    )
    assert client.get("/api/datasets").json()[0]["row_count"] == 30  # the data is kept
    assert "ideal size" in client.get("/pipelines/fake-transactions").text
    assert "ideal size" in client.get("/datasets/transactions").text


def test_every_failing_rule_is_reported_with_schema_problems(client, example_yaml):
    text = contract_yaml(
        table=[{"name": "big", "metric": "rowCount", "mustBeGreaterThan": 99}],
        ts=[{"metric": "duplicateValues", "mustBe": 0}],
        extra_props=[{"name": "gone", "quality": [{"metric": "nullValues", "mustBe": 0}]}],
    )
    error = run_with(client, example_yaml, text)["error"]
    assert error.count("\n- ") == 3
    assert "- column 'gone' is missing" in error  # reported once, not again by its rule
    assert "'big'" in error and "on column 'ts'" in error


def test_rules_are_listed_with_whether_they_are_checked(client):
    text = contract_yaml(
        table=[
            {"name": "not empty", "metric": "rowCount", "mustBeGreaterThan": 0},
            {"type": "text", "description": "Amounts are in pounds."},
            {"type": "custom", "engine": "soda", "implementation": "x"},
        ],
        amount=[{"name": "soft", "metric": "nullValues", "mustBe": 0, "severity": "warning"}],
        flags={"ts": {"unique": True}},
    )
    rules = put(client, text).json()["rules"]
    by_name = {r["rule"]: r for r in rules}
    assert by_name["'not empty'"] == {
        "rule": "'not empty'", "requires": "rowCount must be greater than 0", "severity": "error",
        "checked": True, "note": None,
    }  # fmt: skip
    assert by_name["unique on column 'ts'"]["requires"] == "values must be unique"
    assert by_name["'soft' on column 'amount'"]["severity"] == "warning"
    assert by_name["'text rule'"] == {
        "rule": "'text rule'", "requires": "Amounts are in pounds.", "severity": "error",
        "checked": False, "note": "a description for people, not a check",
    }  # fmt: skip
    assert by_name["'custom rule'"]["note"] == "pipeyard does not run soda rules"
    page = client.get("/datasets/transactions").text
    assert "not checked: pipeyard does not run soda rules" in page
    assert "a warning on the run" in page and "the run fails" in page


INVALID = [
    ({"table": [{"metric": "rowcount", "mustBe": 1}]}, "metric: 'rowcount' is not one of nullValues"),
    ({"table": [{"metric": "rowCount"}]}, "needs a comparison such as 'mustBe: 0'"),
    ({"table": [{"metric": "rowCount", "mustBe": "many"}]}, "mustBe: must be a number"),
    ({"table": [{"metric": "rowCount", "mustBeBetween": [5]}]}, "mustBeBetween: give two numbers"),
    ({"table": [{"metric": "rowCount", "mustBe": 1, "unit": "percent"}]}, "rowCount is counted in rows"),
    ({"table": [{"metric": "nullValues", "mustBe": 0}]}, "nullValues belongs on a property"),
    ({"amount": [{"metric": "rowCount", "mustBe": 1}]}, "rowCount belongs on the schema object"),
    ({"amount": [{"metric": "nullValues", "mustBe": 0, "unit": "pct"}]}, "unit: use 'rows' or 'percent'"),
    ({"amount": [{"metric": "invalidValues", "mustBe": 0}]}, "give validValues, a pattern, or both"),
    ({"amount": [{"metric": "invalidValues", "arguments": {"pattern": "(unclosed"}, "mustBe": 0}]},
     "arguments.pattern: not a valid regular expression"),
    ({"table": [{"metric": "duplicateValues", "mustBe": 0}]}, "arguments.properties: list the columns"),
    ({"table": [{"metric": "duplicateValues", "arguments": {"properties": ["nope"]}, "mustBe": 0}]},
     "arguments.properties: list the columns to check, from this contract"),
    ({"table": [{"type": "sql", "mustBe": 0}]}, "query: an sql rule needs a query"),
    ({"table": [{"type": "sql", "query": "DROP TABLE x", "mustBe": 0}]}, "must be exactly one SELECT"),
    ({"table": [{"type": "sql", "query": "SELECT 1; SELECT 2", "mustBe": 0}]}, "must be exactly one SELECT"),
    ({"table": [{"type": "sql", "query": "SELEC nope", "mustBe": 0}]}, "query: does not parse"),
    ({"table": [{"type": "sql", "query": "SELECT {property} FROM {object}", "mustBe": 0}]},
     "{property} only means something on a property"),
    ({"table": [{"type": "sql", "query": "SELECT 1"}]}, "needs a comparison"),
    ({"table": [{"type": "nonsense", "mustBe": 0}]}, "type: Input should be 'text', 'library', 'sql' or 'custom'"),
]  # fmt: skip


@pytest.mark.parametrize(("kwargs", "expected"), INVALID, ids=lambda k: str(k)[:60])
def test_rules_that_cannot_be_understood_are_refused(client, kwargs, expected):
    response = put(client, contract_yaml(**kwargs))
    assert response.status_code == 422
    assert any(expected in e for e in response.json()["detail"]), response.json()
    assert client.get(CONTRACT).status_code == 404


def test_all_rule_errors_are_reported_together(client):
    text = contract_yaml(table=[{"metric": "x", "mustBe": 0}], amount=[{"metric": "nullValues"}])
    errors = put(client, text).json()["detail"]
    assert len(errors) == 2
    assert errors[0].startswith("schema.transactions.quality.0.metric:")
    assert errors[1].startswith("schema.transactions.properties.amount.quality.0:")


# --- SQL rules are tenant-written SQL: they run in the locked connection --------------------


@pytest.mark.parametrize(
    "target",
    [
        "read_parquet('s3://{bucket}/orgs/{victim}/datasets/transactions/*/*.parquet')",
        "read_json('s3://{bucket}/orgs/{victim}/runs/*.json')",
        "read_text('s3://{bucket}/orgs/{victim}/contracts/transactions.odcs.yaml')",
        "read_csv('/etc/passwd')",
        "glob('s3://{bucket}/orgs/*/runs/*')",
    ],
)
def test_sql_rules_cannot_read_outside_the_org(
    client_for, client, org, settings, example_yaml, target
):
    victim = client_for("org-victim")
    register(victim, example_yaml)
    victim.get(f"/api/runs/{victim.post(f'{P}/runs').json()['run_id']}")

    sql = "SELECT count(*) FROM " + target.format(bucket=settings.bucket, victim="org-victim")
    run = run_with(
        client, example_yaml, contract_yaml(table=[{"type": "sql", "query": sql, "mustBe": 0}])
    )
    assert run["status"] == "failed"
    assert "could not be evaluated: Permission Error" in run["error"]


def test_sql_rules_can_read_their_own_org(client, example_yaml):
    sql = "SELECT count(*) FROM run_output"  # the view, by its real name
    run = run_with(
        client, example_yaml, contract_yaml(table=[{"type": "sql", "query": sql, "mustBe": 30}])
    )
    assert run["status"] == "succeeded"


def test_a_rule_that_errors_is_reported_and_the_others_still_run(client, example_yaml):
    text = contract_yaml(
        table=[
            {"name": "broken", "type": "sql", "query": "SELECT no_such_column FROM {object}", "mustBe": 0},
            {"name": "empty", "type": "sql", "query": "SELECT 1 FROM {object} WHERE false", "mustBe": 1},
            {"name": "big", "metric": "rowCount", "mustBeGreaterThan": 99},
        ]
    )  # fmt: skip
    error = run_with(client, example_yaml, text)["error"]
    assert "quality rule 'broken' could not be evaluated: Binder Error" in error
    assert "quality rule 'empty' returned no value" in error
    assert "quality rule 'big' is not met" in error


def test_contract_checks_are_cut_off(client, settings, org, example_yaml):
    """A runaway SQL rule must not hang the control plane."""
    register(client, example_yaml)
    run = client.post(f"{P}/runs").json()
    with query.RunOutput(settings, org, "transactions", run["run_id"], timeout=0.2) as out:
        assert out.columns == {"ts": "TIMESTAMP", "amount": "DOUBLE", "owner": "VARCHAR"}
        assert out.scalar("SELECT count(*) FROM run_output") == 30
        with pytest.raises(query.duckdb.Error, match="(?i)interrupt"):
            out.scalar("SELECT count(*) FROM range(100000000000) a, range(1000) b")
        with pytest.raises(query.duckdb.Error, match="single SELECT"):
            out.scalar("DROP VIEW run_output")


def test_the_starter_draft_includes_a_rule_and_is_valid():
    text = datacontract.starter("orders", [{"name": "id", "type": "BIGINT"}])
    dc = datacontract.parse(text, "orders")
    (rule,) = datacontract.rules(dc, "orders")
    assert (rule.label, rule.summary) == ("'not empty'", "rowCount must be greater than 0")
