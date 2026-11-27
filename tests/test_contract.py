"""pipeline.yaml and manifest validation: bad input gets a clear, specific message."""

import pytest
from conftest import register

from pipeyard import contract

VALID = """
name: p
image: img:1
outputs: [{dataset: d}]
"""


def errors(yaml_text: str) -> list[str]:
    with pytest.raises(contract.ContractError) as e:
        contract.parse_pipeline(yaml_text)
    return e.value.errors


def test_example_is_valid(example_yaml):
    spec = contract.parse_pipeline(example_yaml)
    assert spec.output_dataset == "transactions"
    assert [r.slug for r in spec.reports] == ["daily-volume", "volume-by-category"]


def test_defaults():
    spec = contract.parse_pipeline(VALID)
    assert (spec.cpu, spec.memory, spec.timeout_seconds, spec.reports) == (256, 512, 600, [])


@pytest.mark.parametrize(
    ("yaml_text", "expected"),
    [
        ("- just\n- a list", "pipeline.yaml must be a mapping"),
        ("name: [unclosed", "not valid YAML"),
        ("image: i\noutputs: [{dataset: d}]", "name: Field required"),
        ("name: p\noutputs: [{dataset: d}]", "image: Field required"),
        ("name: p\nimage: i", "outputs: Field required"),
        ("name: Bad Name\nimage: i\noutputs: [{dataset: d}]", "name: String should match"),
        ("name: p\nimage: i\noutputs: []", "outputs: List should have at least 1 item"),
        (
            "name: p\nimage: i\noutputs: [{dataset: a}, {dataset: b}]",
            "outputs: List should have at most 1",
        ),
        ("name: p\nimage: i\noutputs: [{dataset: ../x}]", "outputs.0.dataset: String should match"),
        (VALID + "cpu: 300", "cpu must be one of [256, 512, 1024, 2048, 4096]"),
        (VALID + "cpu: 256\nmemory: 4096", "memory 4096 is not valid for cpu 256"),
        (
            VALID + "timeout_seconds: 0",
            "timeout_seconds: Input should be greater than or equal to 1",
        ),
        (
            VALID + "timeout_seconds: 99999",
            "timeout_seconds: Input should be less than or equal to 3000",
        ),
        (VALID + "schedule: daily", "schedule: Extra inputs are not permitted"),
        (
            VALID + "reports: [{name: r, dataset: d, sql: 'drop table x'}]",
            "reports.0.sql: must be a SELECT, found DROP",
        ),
        (
            VALID + "reports: [{name: r, dataset: d, sql: 'select 1; select 2'}]",
            "reports.0.sql: must be exactly one statement, found 2",
        ),
        (
            VALID + "reports: [{name: r, dataset: d, sql: 'selec nope'}]",
            "reports.0.sql: does not parse",
        ),
        (
            VALID
            + "reports: [{name: r, dataset: d, sql: 'select 1', chart: {type: pie, x: a, y: b}}]",
            "reports.0.chart.type: Input should be 'line', 'bar', 'area' or 'point'",
        ),
        (
            VALID + "reports: [{name: A b, dataset: d, sql: from d}, "
            "{name: a-B, dataset: d, sql: from d}]",
            "report names must be unique",
        ),
    ],
)
def test_clear_errors(yaml_text, expected):
    found = errors(yaml_text)
    assert any(expected in e for e in found), found


def test_all_errors_are_reported_together():
    assert len(errors("name: BAD\nimage: 'has space'\noutputs: []")) == 3


def test_api_returns_errors_as_a_list(client):
    response = register(client, "name: p\nimage: i\noutputs: [{dataset: d}]\ncpu: 300")
    assert response.status_code == 422
    assert response.json() == {
        "detail": ["cpu must be one of [256, 512, 1024, 2048, 4096] (Fargate CPU units)"]
    }
    assert client.get("/api/pipelines").json() == []


def test_manifest_validation():
    good = (
        '{"dataset": "d", "run_id": "r", "row_count": 1, '
        '"schema": [{"name": "a", "type": "INT"}], "created_at": "2026-01-01T00:00:00Z"}'
    )
    assert contract.parse_manifest(good, dataset="d", run_id="r").row_count == 1
    with pytest.raises(contract.ContractError, match="expected d/other"):
        contract.parse_manifest(good, dataset="d", run_id="other")
    with pytest.raises(contract.ContractError, match="row_count"):
        contract.parse_manifest(good.replace('"row_count": 1, ', ""), dataset="d", run_id="r")
    with pytest.raises(contract.ContractError, match="schema"):
        contract.parse_manifest(
            good.replace('[{"name": "a", "type": "INT"}]', "[]"), dataset="d", run_id="r"
        )


def test_keys_are_always_under_the_org_prefix():
    assert contract.pipeline_key("acme", "p") == "orgs/acme/pipelines/p/pipeline.yaml"
    assert contract.run_key("acme", "r1") == "orgs/acme/runs/r1.json"
    assert contract.output_prefix("acme", "d", "r1") == "orgs/acme/datasets/d/run=r1/"
    for bad in ("", "..", "a/b", "A", "acme/../other", "*"):
        with pytest.raises(ValueError, match="invalid org_id"):
            contract.org_prefix(bad)
