"""DuckDB over S3. Every connection is in-memory, short-lived and bound to one org.

Three things keep a connection inside ``orgs/{org_id}/``:
  1. its S3 secret is scoped to that prefix (and in AWS the credentials themselves are),
  2. after setup, external access is disabled except for that prefix and config is locked,
  3. report SQL is validated as a single SELECT before it runs.
"""

import re
from dataclasses import dataclass
from urllib.parse import urlparse

import duckdb

from . import contract
from .config import Settings
from .storage import org_credentials

MAX_ROWS = 5000

RUN_COLUMNS = {
    "run_id": "VARCHAR",
    "pipeline": "VARCHAR",
    "dataset": "VARCHAR",
    "status": "VARCHAR",
    "started_at": "TIMESTAMPTZ",
    "finished_at": "TIMESTAMPTZ",
    "row_count": "BIGINT",
    "error": "VARCHAR",
}
MANIFEST_COLUMNS = {
    "dataset": "VARCHAR",
    "run_id": "VARCHAR",
    "row_count": "BIGINT",
    "schema": "STRUCT(name VARCHAR, type VARCHAR)[]",
    "created_at": "TIMESTAMPTZ",
}


@dataclass
class Result:
    columns: list[str]
    types: list[str]
    rows: list[tuple]
    truncated: bool = False

    def dicts(self) -> list[dict]:
        return [dict(zip(self.columns, r, strict=True)) for r in self.rows]


def install_extensions() -> None:
    """Once per machine (baked into the control-plane image). Connections only LOAD."""
    duckdb.connect(":memory:").execute("INSTALL httpfs")


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _connect(settings: Settings, org_id: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    """Returns a locked-down connection and the org's S3 root, e.g. s3://bucket/orgs/demo/."""
    root = f"s3://{settings.bucket}/{contract.org_prefix(org_id)}"
    creds = org_credentials(settings, org_id)
    con = duckdb.connect(":memory:")
    con.execute("LOAD httpfs")
    parts = [
        "TYPE s3",
        f"KEY_ID {_lit(creds.access_key)}",
        f"SECRET {_lit(creds.secret_key)}",
        f"REGION {_lit(settings.region)}",
        f"SCOPE {_lit(root)}",
    ]
    if creds.session_token:
        parts.append(f"SESSION_TOKEN {_lit(creds.session_token)}")
    if not settings.is_aws:
        endpoint = urlparse(settings.s3_endpoint_url)
        parts += [
            f"ENDPOINT {_lit(endpoint.netloc)}",
            "URL_STYLE 'path'",
            f"USE_SSL {'true' if endpoint.scheme == 'https' else 'false'}",
        ]
    con.execute(f"CREATE SECRET org_s3 ({', '.join(parts)})")
    con.execute(f"SET allowed_directories = [{_lit(root)}]")
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con, root


def _read_json(con: duckdb.DuckDBPyConnection, glob: str, columns: dict[str, str]) -> list[dict]:
    """All JSON objects matching the glob as dicts; an empty prefix is an empty list."""
    cols = ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in columns.items())
    try:
        cur = con.execute(f"SELECT * FROM read_json({_lit(glob)}, columns = {{{cols}}})")
    except duckdb.IOException as e:
        if "No files found" in str(e):
            return []
        raise
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def list_runs(settings: Settings, org_id: str, pipeline: str | None = None) -> list[dict]:
    con, root = _connect(settings, org_id)
    with con:
        runs = _read_json(con, f"{root}runs/*.json", RUN_COLUMNS)
    if pipeline is not None:
        runs = [r for r in runs if r["pipeline"] == pipeline]
    return sorted(runs, key=lambda r: r["run_id"], reverse=True)


def _manifests(con: duckdb.DuckDBPyConnection, root: str) -> list[dict]:
    """Manifests are written by tenant containers, so their names are checked before use."""
    found = _read_json(con, f"{root}datasets/*/run=*/{contract.MANIFEST_FILE}", MANIFEST_COLUMNS)
    return [
        m
        for m in found
        if re.fullmatch(contract.DATASET_NAME, m["dataset"] or "")
        and contract.RUN_ID_RE.fullmatch(m["run_id"] or "")
    ]


def list_datasets(settings: Settings, org_id: str) -> list[dict]:
    """One entry per dataset, built from the manifests of its committed runs."""
    con, root = _connect(settings, org_id)
    with con:
        manifests = _manifests(con, root)
    datasets: dict[str, dict] = {}
    for m in sorted(manifests, key=lambda m: m["run_id"]):
        d = datasets.setdefault(m["dataset"], {"dataset": m["dataset"], "runs": 0, "row_count": 0})
        d["runs"] += 1
        d["row_count"] += m["row_count"]
        d["schema"] = m["schema"]  # latest run wins
        d["updated_at"] = m["created_at"]
        d["latest_run_id"] = m["run_id"]
    return sorted(datasets.values(), key=lambda d: d["dataset"])


def run_report(settings: Settings, org_id: str, report: contract.Report) -> Result:
    """Runs a report defined in pipeline.yaml. This is the only path that executes tenant SQL.

    Each dataset in the org becomes a view named after it, reading only run directories that
    have a manifest (the manifest is the commit marker). ``{{dataset}}`` is the report's own.
    """
    contract.check_report_sql(report.sql)
    con, root = _connect(settings, org_id)
    with con:
        runs_by_dataset: dict[str, list[str]] = {}
        for m in _manifests(con, root):
            runs_by_dataset.setdefault(m["dataset"], []).append(m["run_id"])
        if report.dataset not in runs_by_dataset:
            raise LookupError(f"dataset '{report.dataset}' has no successful runs yet")
        for dataset, run_ids in runs_by_dataset.items():
            globs = ", ".join(
                _lit(f"{root}datasets/{dataset}/run={run_id}/*.parquet") for run_id in run_ids
            )
            con.execute(
                f'CREATE VIEW "{dataset}" AS SELECT * FROM read_parquet([{globs}], '
                "hive_partitioning = true, union_by_name = true)"
            )
        sql = contract.DATASET_PLACEHOLDER.sub(f'"{report.dataset}"', report.sql)
        cur = con.execute(sql)
        rows = cur.fetchmany(MAX_ROWS + 1)
        return Result(
            columns=[d[0] for d in cur.description],
            types=[str(d[1]) for d in cur.description],
            rows=rows[:MAX_ROWS],
            truncated=len(rows) > MAX_ROWS,
        )
