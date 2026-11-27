# The container contract

A pipeyard pipeline is **a container image plus a `pipeline.yaml`**. pipeyard starts the
container, tells it where to write, and afterwards checks what it wrote. Anything that can
run in a container and write Parquet to S3 can be a pipeline.

The code that enforces this document is `src/pipeyard/contract.py` (models and S3 layout)
and `reconcile()` in `src/pipeyard/app.py` (the success check).

## pipeline.yaml

```yaml
name: fake-transactions            # ^[a-z][a-z0-9-]{0,62}$
image: <ecr-or-local-image>        # anything the runner can pull
cpu: 256                           # Fargate CPU units: 256 | 512 | 1024 | 2048 | 4096
memory: 512                        # MiB, must be a valid Fargate size for the cpu
timeout_seconds: 600               # 1-3600; the run is killed after this
outputs:
  - dataset: transactions          # ^[a-z][a-z0-9_]{0,62}$ ; exactly one in v0
inputs:                            # optional; at most one in v0
  - dataset: raw_events
reports:                           # optional
  - name: Daily volume
    dataset: transactions
    sql: |
      select date_trunc('day', ts) as day, sum(amount) as total
      from {{dataset}} group by 1 order by 1
    chart: { type: line, x: day, y: total }   # optional; type: line | bar | area | point
```

Rules checked at registration (`POST /api/pipelines`, errors come back as a list of
`field: message` strings with HTTP 422):

- Unknown keys are rejected.
- `cpu`/`memory` must be a valid Fargate combination, in local mode too, so a pipeline
  that validates on a laptop also runs in AWS.
- `timeout_seconds` is capped at 3600 because the run's S3 credentials last one hour.
- Each report's `sql` must parse as **exactly one `SELECT`**. `{{dataset}}` is replaced
  with the report's dataset. Every dataset in the org is also available as a view under its
  own name, so a report can join datasets. Report names must be unique within a pipeline.
- v0 runs **only** SQL defined in `pipeline.yaml`. There is no ad-hoc query endpoint.

## What the container receives

| Variable | Example | Notes |
|---|---|---|
| `ORG_ID` | `demo` | The org this run belongs to. |
| `RUN_ID` | `20261127t180305z-3e2ee9cc` | Unique, sortable by start time. |
| `OUTPUT_URI` | `s3://bucket/orgs/demo/datasets/transactions/run=20261127t180305z-3e2ee9cc/` | Always ends in `/`. Write here and nowhere else. |
| `INPUT_URI` | `s3://bucket/orgs/demo/datasets/raw_events/` | Only if the pipeline declares `inputs`. Contains `run=*/` directories. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | | Credentials for the org's prefix. No session token in local mode. |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | `eu-west-2` | |
| `AWS_ENDPOINT_URL` | `https://127.0.0.1:9297` | Local mode only: the AWS Emulator. AWS SDKs pick it up automatically. |
| `AWS_CA_BUNDLE` | `/etc/pipeyard/emulator-ca.pem` | Local mode only: the emulator's certificate, mounted read-only. AWS SDKs and the CLI pick it up automatically. |

Use any AWS SDK with its default configuration: it reads all of the above from the
environment, so the same image runs unchanged locally and in AWS.

Runtime environment:

- **The root filesystem is read-only.** `/tmp` is writable scratch space. Bake anything
  you need (packages, DuckDB extensions) into the image.
- The container gets `cpu` and `memory` from `pipeline.yaml` and no other privileges.
- In AWS the credentials are an STS session tagged with `org_id`; IAM only allows
  `orgs/{org_id}/*`. Writing outside your prefix fails with `AccessDenied`. They expire
  after one hour.
- There are no arguments and no stdin. The image's own `ENTRYPOINT`/`CMD` runs as is.

## What the container must do

1. Write one or more `*.parquet` files directly under `OUTPUT_URI`.
2. **Last**, write `OUTPUT_URI/_manifest.json`:

   ```json
   {
     "dataset": "transactions",
     "run_id": "20261127t180305z-3e2ee9cc",
     "row_count": 10000,
     "schema": [
       {"name": "transaction_id", "type": "VARCHAR"},
       {"name": "ts", "type": "TIMESTAMP"},
       {"name": "amount", "type": "DOUBLE"}
     ],
     "created_at": "2026-11-27T18:03:07+00:00"
   }
   ```

   `dataset` and `run_id` must match the run. `schema` is a non-empty list of
   `{name, type}`; type names are informational (DuckDB names are a good choice).
   `created_at` is ISO 8601. Unknown keys are rejected.
3. Exit `0`.

The manifest is the **commit marker**. Readers only look at `run=` directories that
contain one, so a run that dies half-way never shows up in a dataset or a report. That is
why it must be written last.

## Success and failure

A run **succeeds** only if all of these hold: exit code 0, `_manifest.json` exists and
validates, and at least one `.parquet` file exists. Otherwise it **fails**, and the run
record's `error` says which check failed:

| What happened | `error` |
|---|---|
| Non-zero exit | `container exited with code N` plus the last log lines (local) or the ECS stop reason (AWS) |
| Ran past `timeout_seconds` | local: `container exited with code 137`, `killed (timeout or out of memory)`; AWS: `timed out after Ns` |
| Exit 0, no manifest | `container exited 0 but wrote no _manifest.json` |
| Exit 0, no Parquet | `container exited 0 but wrote no .parquet files` |
| Manifest invalid or for another run | the validation message |
| Image could not be started | `could not start: ...` |

Logs: local runs include the log tail in `error`; in AWS, stdout/stderr go to the
CloudWatch log group named in the stack outputs.

## S3 layout

```
orgs/{org_id}/pipelines/{name}/pipeline.yaml
orgs/{org_id}/runs/{run_id}.json
orgs/{org_id}/datasets/{dataset}/run={run_id}/*.parquet + _manifest.json
```

`run={run_id}` is a Hive-style partition: reports see a `run` column and read **every
committed run** of a dataset (append semantics). To report on the latest run only, filter
on it, for example `where run = (select max(run) from {{dataset}})`.

A run record (`runs/{run_id}.json`) looks like:

```json
{
  "run_id": "20261127t180305z-3e2ee9cc", "org_id": "demo", "pipeline": "fake-transactions",
  "dataset": "transactions", "status": "succeeded",
  "started_at": "2026-11-27T18:03:05Z", "finished_at": "2026-11-27T18:03:07Z",
  "timeout_seconds": 600, "output_uri": "s3://...", "row_count": 10000, "error": null,
  "handle": "pipeyard-20261127t180305z-3e2ee9cc"
}
```

`status` is `running`, `succeeded` or `failed`.

## A minimal pipeline

See `examples/pipelines/fake-transactions/`: about 50 lines of Python that generate rows
with DuckDB into `/tmp`, upload the Parquet file with boto3, then write the manifest.
