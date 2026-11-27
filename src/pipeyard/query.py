"""DuckDB over S3. Every connection is in-memory, short-lived and bound to one org.

These keep a connection inside ``orgs/{org_id}/``:
  1. its S3 secret is scoped to that prefix (and in AWS the credentials themselves are),
  2. after setup, external access is disabled except for that prefix and config is locked,
  3. it only ever runs queries written in this module: there is no tenant SQL.
"""

import os
import re
import threading
from urllib.parse import urlparse

import duckdb

from . import contract
from .config import Settings
from .storage import org_credentials

RUN_COLUMNS = {
    "warnings": "VARCHAR",
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
    if not settings.is_aws and settings.s3_ca_bundle:
        # Trust the local emulator's certificate. The file is a public certificate and the
        # only local path the sandbox may read.
        ca = os.path.abspath(settings.s3_ca_bundle)
        con.execute(f"SET ca_cert_file = {_lit(ca)}")
        con.execute("SET enable_server_cert_verification = true")
        con.execute(f"SET allowed_paths = [{_lit(ca)}]")
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


def _committed(runs: list[dict]) -> list[dict]:
    """The runs whose output may be read: settled as succeeded, oldest first.

    The run record is written by the control plane, so it is the source of truth. Manifests
    and Parquet are written by tenant containers and are only trusted through it.
    """
    ok = [
        r
        for r in runs
        if r["status"] == "succeeded"
        and re.fullmatch(contract.DATASET_NAME, r["dataset"] or "")
        and contract.RUN_ID_RE.fullmatch(r["run_id"] or "")
    ]
    return sorted(ok, key=lambda r: r["run_id"])


def list_datasets(settings: Settings, org_id: str, runs: list[dict]) -> list[dict]:
    """One entry per dataset with a successful run; schema from the latest run's manifest."""
    datasets: dict[str, dict] = {}
    for r in _committed(runs):
        d = datasets.setdefault(
            r["dataset"], {"dataset": r["dataset"], "runs": 0, "row_count": 0, "schema": []}
        )
        d["runs"] += 1
        d["row_count"] += r["row_count"] or 0
        d["updated_at"] = r["finished_at"]
        d["latest_run_id"] = r["run_id"]
    if not datasets:
        return []
    con, root = _connect(settings, org_id)
    with con:
        for d in datasets.values():
            path = (
                f"{root}datasets/{d['dataset']}/run={d['latest_run_id']}/{contract.MANIFEST_FILE}"
            )
            try:
                manifests = _read_json(con, path, MANIFEST_COLUMNS)
            except duckdb.Error:
                continue  # replaced by something unreadable after the run: show no schema
            if manifests and manifests[0]["schema"]:
                d["schema"] = manifests[0]["schema"]
    return sorted(datasets.values(), key=lambda d: d["dataset"])


class RunOutput:
    """One run's Parquet files, as the view ``run_output`` in a locked-down connection.

    Used to check a run against its data contract. The schema and every number come from
    the files themselves, not from what the container's manifest claims. The connection can
    only read the org's own prefix, and everything run through it is cut off after
    ``timeout`` seconds, because contract SQL rules are written by tenants.
    """

    VIEW = "run_output"

    def __init__(
        self, settings: Settings, org_id: str, dataset: str, run_id: str, timeout: float = 30
    ):
        self.con, root = _connect(settings, org_id)
        files = _lit(f"{root}datasets/{dataset}/run={run_id}/*.parquet")
        self.con.execute(
            f"CREATE VIEW {self.VIEW} AS SELECT * FROM read_parquet({files}, "
            "union_by_name = true, hive_partitioning = false)"
        )
        described = self.con.execute(f"DESCRIBE {self.VIEW}").fetchall()
        self.columns: dict[str, str] = {row[0]: row[1] for row in described}
        self._timer = threading.Timer(timeout, self.con.interrupt)
        self._timer.daemon = True
        self._timer.start()

    def aggregates(self, expressions: dict[str, str]) -> dict:
        """Evaluate named aggregate expressions over the run in one scan."""
        if not expressions:
            return {}
        select = ", ".join(expressions.values())
        row = self.con.execute(f"SELECT {select} FROM {self.VIEW}").fetchone()
        return dict(zip(expressions, row, strict=True))

    def scalar(self, sql: str):
        """The first value of the first row of one SELECT statement."""
        statements = duckdb.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise duckdb.InvalidInputException("only a single SELECT statement may be run")
        row = self.con.execute(statements[0]).fetchone()
        return row[0] if row else None

    def __enter__(self) -> "RunOutput":
        return self

    def __exit__(self, *exc) -> None:
        self._timer.cancel()
        self.con.close()
