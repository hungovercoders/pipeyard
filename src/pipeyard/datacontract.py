"""Data contracts in the Open Data Contract Standard (ODCS v3, https://bitol.io).

A dataset may have one contract. pipeyard stores the document exactly as given and enforces
the part of it that describes the schema: when a run is settled, the Parquet it wrote must
have every property the contract lists, with a compatible type, and no nulls where the
contract says ``required``. The rest of the document (quality rules, SLAs, team, pricing,
servers...) is kept and shown, but not checked yet.

Not to be confused with contract.py, which is about how a *container* must behave.
"""

import re
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .contract import ContractError, _messages

LogicalType = Literal[
    "string", "date", "timestamp", "time", "number", "integer", "object", "array", "boolean",
    "map", "vector",
]  # fmt: skip

INTEGERS = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"}
INTEGERS |= {f"U{t}" for t in INTEGERS}
# DuckDB type families a logical type accepts. Checked against the type name without its
# parameters, e.g. DECIMAL(18,2) -> DECIMAL, TIMESTAMP WITH TIME ZONE -> TIMESTAMP.
LOGICAL = {
    "string": {"VARCHAR", "UUID", "ENUM"},
    "integer": INTEGERS,
    "number": INTEGERS | {"FLOAT", "DOUBLE", "DECIMAL"},
    "boolean": {"BOOLEAN"},
    "date": {"DATE", "TIMESTAMP"},  # ODCS v3.0 had no separate timestamp type
    "timestamp": {"TIMESTAMP"},
    "time": {"TIME"},
    "object": {"STRUCT"},
    "map": {"MAP"},
}
# Other engines' names for the same physical type, so a contract written for another
# system still compares equal.
ALIASES = {
    "STRING": "VARCHAR", "TEXT": "VARCHAR", "CHAR": "VARCHAR", "NVARCHAR": "VARCHAR",
    "INT": "INTEGER", "INT4": "INTEGER", "INT8": "BIGINT", "LONG": "BIGINT",
    "INT2": "SMALLINT", "SHORT": "SMALLINT", "INT1": "TINYINT",
    "REAL": "FLOAT", "FLOAT4": "FLOAT", "FLOAT8": "DOUBLE", "DOUBLE PRECISION": "DOUBLE",
    "NUMERIC": "DECIMAL", "BOOL": "BOOLEAN", "DATETIME": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP", "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP_TZ": "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_LTZ": "TIMESTAMP WITH TIME ZONE",
}  # fmt: skip


class _Lenient(BaseModel):
    """ODCS has many optional sections. Keep what we do not model rather than reject it."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class Property(_Lenient):
    name: str = Field(min_length=1)
    physicalName: str | None = None
    logicalType: LogicalType | None = None
    physicalType: str | None = None
    required: bool = False
    description: str | None = None

    @property
    def column(self) -> str:
        return self.physicalName or self.name


class SchemaObject(_Lenient):
    name: str = Field(min_length=1)
    physicalName: str | None = None
    description: str | None = None
    properties: list[Property] = Field(default_factory=list)


class DataContract(_Lenient):
    apiVersion: str = Field(pattern=r"^v3\.\d+\.\d+$")
    kind: Literal["DataContract"]
    id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    name: str | None = None
    status: str | None = None
    schema_: list[SchemaObject] = Field(alias="schema", min_length=1)

    def table(self, dataset: str) -> SchemaObject | None:
        """The schema object that describes this dataset: matched by name or physical name."""
        return next((o for o in self.schema_ if dataset in (o.name, o.physicalName)), None)

    @property
    def purpose(self) -> str | None:
        description = (self.model_extra or {}).get("description")
        return description.get("purpose") if isinstance(description, dict) else None

    @property
    def label(self) -> str:
        return f"'{self.name or self.id}' version {self.version}"


def parse(text: str, dataset: str) -> DataContract:
    """Parse and validate an ODCS document as the contract for ``dataset``."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ContractError([f"not valid YAML: {e}"]) from e
    if not isinstance(data, dict):
        raise ContractError(["an ODCS data contract must be a YAML mapping"])
    if str(data.get("apiVersion", "")).startswith("v2"):
        raise ContractError(["apiVersion: ODCS v2 is not supported; use v3 (e.g. v3.2.0)"])
    try:
        contract = DataContract.model_validate(data)
    except ValidationError as e:
        raise ContractError(_messages(e)) from e
    table = contract.table(dataset)
    if table is None:
        names = ", ".join(o.name for o in contract.schema_)
        raise ContractError(
            [f"schema: no object named '{dataset}' (found: {names}); the contract for a "
             "dataset must describe it by name or physicalName"]
        )  # fmt: skip
    if not table.properties:
        raise ContractError([f"schema.{table.name}.properties: list at least one property"])
    columns = [p.column for p in table.properties]
    if len(columns) != len(set(columns)):
        raise ContractError([f"schema.{table.name}.properties: column names must be unique"])
    return contract


def _base(duckdb_type: str) -> str:
    """TIMESTAMP WITH TIME ZONE -> TIMESTAMP, DECIMAL(18,2) -> DECIMAL, ENUM(...) -> ENUM."""
    return re.split(r"[(\s]", duckdb_type.strip().upper(), maxsplit=1)[0]


def _normalise(physical: str) -> str:
    text = re.sub(r"\s+", " ", physical.strip().upper())
    name, paren, params = text.partition("(")
    name = ALIASES.get(name.strip(), name.strip())
    if name == "VARCHAR":
        return name  # a length limit is not something Parquet records
    return name + (paren + params.replace(" ", "") if paren else "")


def type_problem(prop: Property, actual: str) -> str | None:
    """Why the column's actual DuckDB type does not satisfy the property, if it does not."""
    if prop.physicalType:
        wanted = _normalise(prop.physicalType)
        if wanted != _normalise(actual):
            return f"is {actual}, the contract says {prop.physicalType}"
        return None
    logical = prop.logicalType
    if logical is None or logical == "vector":
        return None  # nothing to compare against
    if logical == "array":
        ok = actual.rstrip().endswith("]")
    else:
        ok = _base(actual) in LOGICAL[logical] and not actual.rstrip().endswith("]")
    return None if ok else f"is {actual}, but the contract's logical type is {logical}"


def violations(
    contract: DataContract, dataset: str, columns: dict[str, str], nulls: dict[str, int]
) -> list[str]:
    """Compare what a run wrote with the contract. Empty list means it conforms.

    ``columns`` maps column name to DuckDB type; ``nulls`` maps a required column to the
    number of nulls found in it. Columns the contract does not mention are allowed.
    """
    table = contract.table(dataset)
    problems = []
    for prop in table.properties if table else []:
        if prop.column not in columns:
            problems.append(f"column '{prop.column}' is missing")
            continue
        problem = type_problem(prop, columns[prop.column])
        if problem:
            problems.append(f"column '{prop.column}' {problem}")
        if prop.required and nulls.get(prop.column):
            count = nulls[prop.column]
            problems.append(f"column '{prop.column}' is required but has {count:,} null values")
    return problems


def starter(dataset: str, schema: list[dict]) -> str:
    """A first contract for a dataset, written from the columns its latest run produced."""
    guess = {v: k for k in ("string", "boolean", "time") for v in LOGICAL[k]}
    guess |= dict.fromkeys(INTEGERS, "integer") | {"DATE": "date", "TIMESTAMP": "timestamp"}
    guess |= dict.fromkeys(("FLOAT", "DOUBLE", "DECIMAL"), "number")
    doc = {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": dataset,
        "name": dataset,
        "version": "1.0.0",
        "status": "draft",
        "description": {"purpose": f"Describe what the {dataset} dataset is for."},
        "schema": [
            {
                "name": dataset,
                "physicalType": "table",
                "properties": [
                    {
                        "name": c["name"],
                        "logicalType": guess.get(_base(c["type"]), "string"),
                        "physicalType": c["type"],
                        "required": False,
                    }
                    for c in schema
                ]
                or [{"name": "id", "logicalType": "string", "required": True}],
            }
        ],
    }
    return yaml.safe_dump(doc, sort_keys=False)
