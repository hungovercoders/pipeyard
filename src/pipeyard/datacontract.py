"""Data contracts in the Open Data Contract Standard (ODCS v3, https://bitol.io).

A dataset may have one contract. pipeyard stores the document exactly as given and enforces
the part of it that describes the schema: when a run is settled, the Parquet it wrote must
have every property the contract lists, with a compatible type, and no nulls where the
contract says ``required``; and its quality rules must hold: the standard's library
metrics, ``unique`` / ``primaryKey``, and SQL rules. The rest of the document (text and
custom rules, SLAs, team, pricing, servers...) is kept and shown, but not checked.

Not to be confused with contract.py, which is about how a *container* must behave.
"""

import re
from dataclasses import dataclass, field
from typing import Literal

import duckdb
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


class Quality(_Lenient):
    """One entry of a ``quality`` list. Its comparison operators are kept as extra fields."""

    id: str | None = None
    name: str | None = None
    description: str | None = None
    type: Literal["text", "library", "sql", "custom"] | None = None
    metric: str | None = None
    rule: str | None = None  # deprecated spelling of ``metric``
    arguments: dict = Field(default_factory=dict)
    unit: str | None = None
    severity: str | None = None
    query: str | None = None
    engine: str | None = None

    @property
    def kind(self) -> str:
        return self.type or "library"  # the standard's default


class Property(_Lenient):
    name: str = Field(min_length=1)
    physicalName: str | None = None
    logicalType: LogicalType | None = None
    physicalType: str | None = None
    required: bool = False
    unique: bool = False
    primaryKey: bool = False
    primaryKeyPosition: int = -1
    description: str | None = None
    quality: list[Quality] = Field(default_factory=list)

    @property
    def column(self) -> str:
        return self.physicalName or self.name


class SchemaObject(_Lenient):
    name: str = Field(min_length=1)
    physicalName: str | None = None
    description: str | None = None
    properties: list[Property] = Field(default_factory=list)
    quality: list[Quality] = Field(default_factory=list)


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
    rules(contract, dataset)  # raises ContractError if a quality rule cannot be understood
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


# --- quality rules -------------------------------------------------------------------------

VIEW = "run_output"  # what {object} in an SQL rule stands for: the run being checked
METRICS = ("nullValues", "missingValues", "invalidValues", "duplicateValues", "rowCount")
OPERATORS = {
    "mustBe": ("must be {}", lambda v, x: v == x),
    "mustNotBe": ("must not be {}", lambda v, x: v != x),
    "mustBeGreaterThan": ("must be greater than {}", lambda v, x: v > x),
    "mustBeGreaterOrEqualTo": ("must be at least {}", lambda v, x: v >= x),
    "mustBeLessThan": ("must be less than {}", lambda v, x: v < x),
    "mustBeLessOrEqualTo": ("must be at most {}", lambda v, x: v <= x),
    "mustBeBetween": ("must be between {} and {}", lambda v, x: x[0] <= v <= x[1]),
    "mustNotBeBetween": ("must not be between {} and {}", lambda v, x: not x[0] <= v <= x[1]),
}


@dataclass
class Rule:
    """A quality rule in runnable form, or the reason it is not run."""

    label: str  # how messages and the UI refer to it
    summary: str  # what it requires, in words
    column: str | None = None
    columns: list[str] = field(default_factory=list)  # every column it reads
    severity: str = "error"  # anything other than warning / info fails the run
    expression: str | None = None  # an aggregate over the run (library metrics)
    query: str | None = None  # a whole SELECT (sql rules)
    percent: bool = False
    operators: dict = field(default_factory=dict)
    skipped: str | None = None

    def requirement(self) -> str:
        unit = "%" if self.percent else ""
        parts = []
        for name, value in self.operators.items():
            values = value if isinstance(value, list) else [value]
            parts.append(OPERATORS[name][0].format(*(f"{_show(v)}{unit}" for v in values)))
        return " and ".join(parts)

    def holds(self, value) -> bool:
        return all(OPERATORS[name][1](value, x) for name, x in self.operators.items())


def _ident(column: str) -> str:
    return '"' + column.replace('"', '""') + '"'


def _lit(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _show(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.2f}"
    return f"{int(value):,}" if isinstance(value, int | float) else str(value)


def _operators(q: Quality, where: str, *, booleans: bool = False) -> dict:
    found = {k: v for k, v in (q.model_extra or {}).items() if k in OPERATORS}
    if not found:
        raise ValueError(f"{where}: needs a comparison such as 'mustBe: 0'")
    for name, value in found.items():
        between = name.endswith("Between")
        values = value if between and isinstance(value, list) else [value]
        numeric = all(isinstance(v, int | float) and not isinstance(v, bool) for v in values)
        if between and (not isinstance(value, list) or len(value) != 2 or not numeric):
            raise ValueError(f"{where}.{name}: give two numbers, smallest first")
        equality = name in ("mustBe", "mustNotBe")
        if not numeric and not (booleans and equality and isinstance(value, bool)):
            raise ValueError(f"{where}.{name}: must be a number")
    return found


def _library(q: Quality, where: str, prop: Property | None, known: set[str]) -> Rule:
    metric = q.metric or q.rule
    if metric not in METRICS:
        raise ValueError(f"{where}.metric: '{metric}' is not one of {', '.join(METRICS)}")
    if q.unit not in (None, "rows", "percent"):
        raise ValueError(f"{where}.unit: use 'rows' or 'percent', not '{q.unit}'")
    rule = Rule(label="", summary="", operators=_operators(q, where))
    rule.percent = q.unit == "percent"
    column = _ident(prop.column) if prop else None
    if metric == "rowCount":
        if prop is not None:
            raise ValueError(f"{where}: rowCount belongs on the schema object, not a property")
        if rule.percent:
            raise ValueError(f"{where}.unit: rowCount is counted in rows")
        rule.expression = "count(*)"
    elif metric == "duplicateValues":
        names = [prop.column] if prop else q.arguments.get("properties")
        if not isinstance(names, list) or not names or not set(names) <= known:
            raise ValueError(
                f"{where}.arguments.properties: list the columns to check, from this contract"
            )
        rule.columns = names
        key = ", ".join(_ident(n) for n in names)
        # Rows beyond the first for each distinct value; rows with a null in the key are ignored.
        present = " AND ".join(f"{_ident(n)} IS NOT NULL" for n in names)
        rule.expression = f"count(*) FILTER (WHERE {present}) - count(DISTINCT ({key}))"
        if len(names) > 1:
            rule.expression = (
                f"count(*) FILTER (WHERE {present}) - "
                f"count(DISTINCT CASE WHEN {present} THEN ({key}) END)"
            )
    else:
        if prop is None:
            raise ValueError(f"{where}: {metric} belongs on a property, not the schema object")
        if metric == "nullValues":
            condition = f"{column} IS NULL"
        elif metric == "missingValues":
            listed = q.arguments.get("missingValues", [None])
            if not isinstance(listed, list) or not listed:
                raise ValueError(f"{where}.arguments.missingValues: give a list of values")
            texts = ", ".join(_lit(v) for v in listed if v is not None)
            tests = [f"{column}::VARCHAR IN ({texts})"] if texts else []
            tests += [f"{column} IS NULL"] if None in listed else []
            condition = " OR ".join(tests)
        else:  # invalidValues: a value that is there, but is not allowed
            tests = []
            valid, pattern = q.arguments.get("validValues"), q.arguments.get("pattern")
            if valid is not None:
                if not isinstance(valid, list) or not valid:
                    raise ValueError(f"{where}.arguments.validValues: give a list of values")
                tests.append(f"{column}::VARCHAR NOT IN ({', '.join(_lit(v) for v in valid)})")
            if pattern is not None:
                try:
                    with duckdb.connect(":memory:") as scratch:  # never the shared default
                        scratch.execute("SELECT regexp_matches('', ?)", [str(pattern)]).fetchall()
                except duckdb.Error as e:
                    raise ValueError(f"{where}.arguments.pattern: not a valid regular expression "
                                     f"({str(e).splitlines()[0]})") from e  # fmt: skip
                tests.append(f"NOT regexp_matches({column}::VARCHAR, {_lit(pattern)})")
            if not tests:
                raise ValueError(f"{where}.arguments: give validValues, a pattern, or both")
            condition = f"{column} IS NOT NULL AND ({' OR '.join(tests)})"
        rule.expression = f"count(*) FILTER (WHERE {condition})"
    rule.summary = f"{metric} {rule.requirement()}"
    return rule


def _sql(q: Quality, where: str, prop: Property | None) -> Rule:
    if not q.query or not q.query.strip():
        raise ValueError(f"{where}.query: an sql rule needs a query")
    query = q.query
    for placeholder in ("{object}", "${object}", "${table}", "{table}"):
        query = query.replace(placeholder, VIEW)
    for placeholder in ("{property}", "${property}", "${column}", "{column}"):
        if placeholder in query:
            if prop is None:
                raise ValueError(f"{where}.query: {placeholder} only means something on a property")
            query = query.replace(placeholder, _ident(prop.column))
    try:
        statements = duckdb.extract_statements(query)
    except duckdb.Error as e:
        raise ValueError(f"{where}.query: does not parse: {str(e).splitlines()[0]}") from e
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise ValueError(f"{where}.query: must be exactly one SELECT statement")
    rule = Rule(label="", summary="", query=query, operators=_operators(q, where, booleans=True))
    rule.summary = f"query result {rule.requirement()}"
    return rule


def rules(contract: DataContract, dataset: str) -> list[Rule]:
    """Every quality rule of the dataset's schema object, compiled. Raises ContractError."""
    table = contract.table(dataset)
    known = {p.column for p in table.properties}
    out: list[Rule] = []
    errors: list[str] = []

    for prop in table.properties:
        if prop.unique and not prop.primaryKey:
            key = _ident(prop.column)
            out.append(
                Rule(
                    label=f"unique on column '{prop.column}'",
                    summary="values must be unique",
                    column=prop.column,
                    columns=[prop.column],
                    expression=f"count({key}) - count(DISTINCT {key})",
                    operators={"mustBe": 0},
                )
            )
    keys = sorted((p for p in table.properties if p.primaryKey), key=lambda p: p.primaryKeyPosition)
    if keys:
        names = [p.column for p in keys]
        key = ", ".join(_ident(n) for n in names)
        distinct = f"({key})" if len(names) > 1 else key
        out.append(
            Rule(
                label=f"primary key ({', '.join(names)})",
                summary="values must be unique",
                columns=names,
                expression=f"count(*) - count(DISTINCT {distinct})",
                operators={"mustBe": 0},
            )
        )

    scopes = [(None, table.quality, f"schema.{table.name}.quality")]
    scopes += [
        (p, p.quality, f"schema.{table.name}.properties.{p.name}.quality") for p in table.properties
    ]
    for prop, entries, path in scopes:
        for index, q in enumerate(entries):
            where = f"{path}.{index}"
            on = f" on column '{prop.column}'" if prop else ""
            try:
                if q.kind == "text":
                    rule = Rule(
                        "", q.description or "", skipped="a description for people, not a check"
                    )
                elif q.kind == "custom":
                    engine = q.engine or "custom"
                    rule = Rule(
                        "", f"{engine} rule", skipped=f"pipeyard does not run {engine} rules"
                    )
                elif q.kind == "sql":
                    rule = _sql(q, where, prop)
                else:
                    rule = _library(q, where, prop, known)
            except ValueError as e:
                errors.append(str(e))
                continue
            name = q.name or q.id or q.metric or q.rule or f"{q.kind} rule"
            rule.label = f"'{name}'{on}"
            rule.column = prop.column if prop else None
            rule.columns = rule.columns or ([prop.column] if prop else [])
            if (q.severity or "").lower() in ("warning", "info"):
                rule.severity = q.severity.lower()
            out.append(rule)
    if errors:
        raise ContractError(errors)
    return out


def check(contract: DataContract, dataset: str, out) -> tuple[list[str], list[str]]:
    """Compare what a run wrote with the contract: (problems, warnings).

    ``out`` is the run's data: ``columns`` (name -> DuckDB type), ``aggregates(exprs)`` and
    ``scalar(sql)``; see query.RunOutput. Problems fail the run. Warnings are rules of
    severity warning or info that did not hold. Columns the contract does not mention are
    allowed.
    """
    table = contract.table(dataset)
    columns: dict[str, str] = out.columns
    problems: list[str] = []
    warnings: list[str] = []

    # 1. The schema: every property present, of the right type, and filled in if required.
    required = [p.column for p in table.properties if p.required and p.column in columns]
    counts = {
        f"null_{i}": f"count(*) FILTER (WHERE {_ident(c)} IS NULL)" for i, c in enumerate(required)
    }
    measured = out.aggregates({"total": "count(*)", **counts})
    total = measured["total"]
    nulls = {c: measured[f"null_{i}"] for i, c in enumerate(required)}
    for prop in table.properties:
        if prop.column not in columns:
            problems.append(f"column '{prop.column}' is missing")
            continue
        problem = type_problem(prop, columns[prop.column])
        if problem:
            problems.append(f"column '{prop.column}' {problem}")
        if nulls.get(prop.column):
            problems.append(
                f"column '{prop.column}' is required but has {nulls[prop.column]:,} null values"
            )

    # 2. The quality rules whose columns exist (a missing column is already reported above).
    runnable = [
        r for r in rules(contract, dataset) if not r.skipped and set(r.columns) <= set(columns)
    ]
    library = {f"rule_{i}": r for i, r in enumerate(runnable) if r.expression}
    results: dict[int, object] = {}
    failures: dict[int, str] = {}

    def evaluate(batch: dict[str, Rule]) -> None:
        values = out.aggregates({k: r.expression for k, r in batch.items()})
        for key, rule in batch.items():
            results[id(rule)] = values[key]

    try:
        evaluate(library)
    except duckdb.Error:
        for key, rule in library.items():  # find which rule cannot be evaluated
            try:
                evaluate({key: rule})
            except duckdb.Error as e:
                failures[id(rule)] = str(e).splitlines()[0]
    for rule in runnable:
        if rule.query:
            try:
                results[id(rule)] = out.scalar(rule.query)
            except duckdb.Error as e:
                failures[id(rule)] = str(e).splitlines()[0]

    for rule in runnable:
        if id(rule) in failures:
            message = f"quality rule {rule.label} could not be evaluated: {failures[id(rule)]}"
        else:
            value = results[id(rule)]
            if value is None:
                message = f"quality rule {rule.label} returned no value"
            else:
                if hasattr(value, "is_integer") or not isinstance(value, bool | int):
                    value = value if isinstance(value, bool) else float(value)
                if rule.percent:
                    value = 100 * value / total if total else 0.0
                if rule.holds(value):
                    continue
                found = f"{_show(value)}{'%' if rule.percent else ''}"
                message = f"quality rule {rule.label} is not met: {rule.summary}, but it is {found}"
        (warnings if rule.severity != "error" else problems).append(message)
    return problems, warnings


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
                "quality": [
                    {
                        "name": "not empty",
                        "description": "Every run must write at least one row.",
                        "metric": "rowCount",
                        "mustBeGreaterThan": 0,
                    }
                ],
            }
        ],
    }
    return yaml.safe_dump(doc, sort_keys=False)
