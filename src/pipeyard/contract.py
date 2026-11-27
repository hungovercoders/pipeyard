"""The container contract: pipeline.yaml, manifest and run-record models, and the S3 layout.

This module is the only place S3 keys are built. Every helper takes ``org_id`` first, so
nothing can address an object outside ``orgs/{org_id}/``. See docs/container-contract.md.
"""

import re
import secrets
from datetime import UTC, datetime
from typing import Literal

import duckdb
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

ORG_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
RUN_ID_RE = re.compile(r"^[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
PIPELINE_NAME = r"^[a-z][a-z0-9-]{0,62}$"
DATASET_NAME = r"^[a-z][a-z0-9_]{0,62}$"
DATASET_PLACEHOLDER = re.compile(r"\{\{\s*dataset\s*\}\}")
MANIFEST_FILE = "_manifest.json"

# Fargate task sizes: cpu units -> allowed memory (MiB). Enforced locally too, so a
# pipeline that validates on a laptop also runs in AWS.
FARGATE_SIZES: dict[int, range] = {
    256: range(512, 2049, 512),
    512: range(1024, 4097, 1024),
    1024: range(2048, 8193, 1024),
    2048: range(4096, 16385, 1024),
    4096: range(8192, 30721, 1024),
}


class ContractError(ValueError):
    """A pipeline.yaml or manifest broke the contract. ``errors`` is a list of plain messages."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DatasetRef(_Strict):
    dataset: str = Field(pattern=DATASET_NAME)


class Chart(_Strict):
    type: Literal["line", "bar", "area", "point"]
    x: str = Field(min_length=1)
    y: str = Field(min_length=1)


class Report(_Strict):
    name: str = Field(min_length=1, max_length=80)
    dataset: str = Field(pattern=DATASET_NAME)
    sql: str = Field(min_length=1)
    chart: Chart | None = None

    @property
    def slug(self) -> str:
        return re.sub(r"[^a-z0-9]+", "-", self.name.lower()).strip("-")

    @field_validator("name")
    @classmethod
    def _name_has_slug(cls, v: str) -> str:
        if not re.search(r"[A-Za-z0-9]", v):
            raise ValueError("must contain at least one letter or digit")
        return v

    @field_validator("sql")
    @classmethod
    def _single_select(cls, v: str) -> str:
        check_report_sql(v)
        return v


class PipelineSpec(_Strict):
    name: str = Field(pattern=PIPELINE_NAME)
    image: str = Field(pattern=r"^\S+$", max_length=255)
    cpu: int = 256
    memory: int = 512
    # Run credentials last 3600s from the trigger; leave room for the task to start.
    timeout_seconds: int = Field(default=600, ge=1, le=3000)
    inputs: list[DatasetRef] = Field(default_factory=list, max_length=1)
    outputs: list[DatasetRef] = Field(min_length=1, max_length=1)
    reports: list[Report] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> "PipelineSpec":
        if self.cpu not in FARGATE_SIZES:
            raise ValueError(f"cpu must be one of {sorted(FARGATE_SIZES)} (Fargate CPU units)")
        if self.memory not in FARGATE_SIZES[self.cpu]:
            allowed = FARGATE_SIZES[self.cpu]
            raise ValueError(
                f"memory {self.memory} is not valid for cpu {self.cpu}: use "
                f"{allowed.start}-{allowed.stop - 1} MiB in steps of {allowed.step}"
            )
        slugs = [r.slug for r in self.reports]
        if len(slugs) != len(set(slugs)):
            raise ValueError("report names must be unique")
        return self

    @property
    def output_dataset(self) -> str:
        return self.outputs[0].dataset

    def report(self, slug: str) -> Report | None:
        return next((r for r in self.reports if r.slug == slug), None)


class Column(_Strict):
    name: str
    type: str


class Manifest(_Strict):
    dataset: str = Field(pattern=DATASET_NAME)
    run_id: str
    row_count: int = Field(ge=0)
    schema_: list[Column] = Field(alias="schema", min_length=1)
    created_at: datetime


class RunRecord(BaseModel):
    run_id: str
    org_id: str
    pipeline: str
    dataset: str
    status: Literal["running", "succeeded", "failed"]
    started_at: datetime
    timeout_seconds: int
    output_uri: str
    handle: str | None = None  # runner's reference: container id or task ARN
    finished_at: datetime | None = None
    row_count: int | None = None
    error: str | None = None


def check_report_sql(sql: str) -> None:
    """Report SQL must be exactly one SELECT. Raises ValueError with a readable message."""
    probe = DATASET_PLACEHOLDER.sub("dataset_placeholder", sql)
    try:
        statements = duckdb.extract_statements(probe)
    except duckdb.Error as e:
        raise ValueError(f"does not parse: {str(e).splitlines()[0]}") from e
    if len(statements) != 1:
        raise ValueError(f"must be exactly one statement, found {len(statements)}")
    if statements[0].type != duckdb.StatementType.SELECT:
        kind = statements[0].type.name
        raise ValueError(f"must be a SELECT, found {kind}")


def _messages(e: ValidationError) -> list[str]:
    out = []
    for err in e.errors():
        loc = ".".join(str(p) for p in err["loc"])
        msg = err["msg"].removeprefix("Value error, ")
        out.append(f"{loc}: {msg}" if loc else msg)
    return out


def parse_pipeline(text: str) -> PipelineSpec:
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ContractError([f"not valid YAML: {e}"]) from e
    if not isinstance(data, dict):
        raise ContractError(["pipeline.yaml must be a mapping with name, image and outputs"])
    try:
        return PipelineSpec.model_validate(data)
    except ValidationError as e:
        raise ContractError(_messages(e)) from e


def parse_manifest(text: str, *, dataset: str, run_id: str) -> Manifest:
    try:
        manifest = Manifest.model_validate_json(text)
    except ValidationError as e:
        raise ContractError([f"{MANIFEST_FILE} {m}" for m in _messages(e)]) from e
    if manifest.dataset != dataset or manifest.run_id != run_id:
        raise ContractError(
            [
                f"{MANIFEST_FILE} is for {manifest.dataset}/{manifest.run_id}, "
                f"expected {dataset}/{run_id}"
            ]
        )
    return manifest


def new_run_id() -> str:
    """Sortable by start time; lowercase so it is safe in keys, tags and container names."""
    return f"{datetime.now(UTC):%Y%m%dt%H%M%Sz}-{secrets.token_hex(4)}"


def _org(org_id: str) -> str:
    if not ORG_ID_RE.fullmatch(org_id):
        raise ValueError(f"invalid org_id: {org_id!r}")
    return org_id


def org_prefix(org_id: str) -> str:
    return f"orgs/{_org(org_id)}/"


def pipelines_prefix(org_id: str) -> str:
    return f"{org_prefix(org_id)}pipelines/"


def pipeline_key(org_id: str, name: str) -> str:
    return f"{pipelines_prefix(org_id)}{name}/pipeline.yaml"


def runs_prefix(org_id: str) -> str:
    return f"{org_prefix(org_id)}runs/"


def run_key(org_id: str, run_id: str) -> str:
    return f"{runs_prefix(org_id)}{run_id}.json"


def datasets_prefix(org_id: str) -> str:
    return f"{org_prefix(org_id)}datasets/"


def dataset_prefix(org_id: str, dataset: str) -> str:
    return f"{datasets_prefix(org_id)}{dataset}/"


def output_prefix(org_id: str, dataset: str, run_id: str) -> str:
    return f"{dataset_prefix(org_id, dataset)}run={run_id}/"
