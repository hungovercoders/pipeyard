# The container contract

This is the reference for everything pipeyard expects from a pipeline. If you would rather
learn by doing, start with [your first pipeline](your-first-pipeline.md) and come back
here when you need a detail.

A pipeline is **a container image plus a `pipeline.yaml`**, and optionally one or more
**report containers**. pipeyard starts the pipeline container, tells it where to write, and
afterwards checks what it wrote. It starts a report container when someone opens the
report, and passes their browser through to it.

## The contract at a glance

A **pipeline container** must:

- [ ] read `RUN_ID` and `OUTPUT_URI` from the environment
- [ ] write one or more `*.parquet` files directly under `OUTPUT_URI`
- [ ] then write `OUTPUT_URI/_manifest.json`
- [ ] exit with code `0`
- [ ] finish within `timeout_seconds`

A **report container** must:

- [ ] listen for HTTP on `PORT`, on all interfaces (`0.0.0.0`)
- [ ] serve everything under the URL prefix `BASE_PATH`
- [ ] read its data from `INPUT_URI`, and only from `run=` folders that contain a manifest

Both may write to local disk only under `/tmp`.

## pipeline.yaml

```yaml
name: fake-transactions            # ^[a-z][a-z0-9-]{0,62}$
image: <ecr-or-local-image>        # anything the runner can pull
cpu: 256                           # Fargate CPU units: 256 | 512 | 1024 | 2048 | 4096
memory: 512                        # MiB, must be a valid Fargate size for the cpu
timeout_seconds: 600               # 1-3000; the run is killed after this
outputs:
  - dataset: transactions          # ^[a-z][a-z0-9_]{0,62}$ ; exactly one in v0
inputs:                            # optional; at most one in v0
  - dataset: raw_events
reports:                           # optional
  - name: Sales dashboard          # shown in the UI; its lower-case-with-dashes form is the URL slug
    image: <ecr-or-local-image>    # a web app
    dataset: transactions          # the dataset it reads
    cpu: 256                       # same sizes as above
    memory: 512
    lifetime_seconds: 1800         # 60-3000; how long one start of the app runs
```

Only `name`, `image` and `outputs` are required, and for a report `name`, `image` and
`dataset`. The sizes, `timeout_seconds` and `lifetime_seconds` default to the values shown.

Rules checked when you register, in the UI or with `POST /api/pipelines`. Errors come back
as a list of `field: message` lines (HTTP 422 from the API), and nothing is saved until the
YAML is valid:

- Unknown keys are rejected.
- `cpu`/`memory` must be a valid Fargate combination, in local mode too, so a pipeline
  that validates on a laptop also runs in AWS.
- `timeout_seconds` is capped at 3000 because the run's S3 credentials last one hour from
  the moment of the trigger, and the task needs time to start before the timeout counts.
- `lifetime_seconds` is capped at 3000 for the same reason.
- Report names must be unique within a pipeline.

## Pipeline containers

### What the container receives

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

### What the container must do

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

The manifest is the **commit marker**: write it last. pipeyard checks it when the run
ends and records the outcome in the run record. If the run failed for any reason,
pipeyard deletes everything it wrote. So a `run=` folder that contains a manifest is a
finished, good run, and that is the rule report containers read by.

### Success and failure

A run **succeeds** only if all of these hold: exit code 0, `_manifest.json` exists and
validates, and at least one `.parquet` file exists. Otherwise it **fails**, and the run
record's `error` says which check failed:

| What happened | `error` |
|---|---|
| Non-zero exit | `container exited with code N` plus the last log lines (local) or the ECS stop reason (AWS) |
| Ran past `timeout_seconds` | `timed out after Ns` |
| Killed for using more than `memory` (local) | `container exited with code 137`, `out of memory` |
| The runner no longer knows the container (for example ECS forgets stopped tasks after about an hour) | Decided from the output alone: succeeded if a valid manifest and Parquet exist, otherwise `task no longer exists but wrote no _manifest.json` |
| Exit 0, no manifest | `container exited 0 but wrote no _manifest.json` |
| Exit 0, no Parquet | `container exited 0 but wrote no .parquet files` |
| Manifest invalid or for another run | the validation message |
| Image could not be started | `could not start: ...` |

Logs: local runs include the log tail in `error`; in AWS, stdout/stderr go to the
CloudWatch log group named in the stack outputs.

## Report containers

A report is a web app. It can be Streamlit, Dash, Shiny, a notebook server, or thirty lines
of Flask: pipeyard does not look inside. It starts the container when someone opens the
report, waits until it answers, then forwards that person's requests to it.

### What the container receives

| Variable | Example | Notes |
|---|---|---|
| `PORT` | `8600` | Listen here, on `0.0.0.0`. Do not hard-code it: it differs between local and AWS. |
| `BASE_PATH` | `/apps/coffee-orders/coffee-dashboard` | The URL prefix the app is served under. No trailing slash. |
| `INPUT_URI` | `s3://bucket/orgs/demo/datasets/orders/` | The report's dataset. Contains `run=*/` folders. |
| `ORG_ID` | `demo` | |
| `AWS_*` | | As for pipelines, but **read-only**: the app can read its org's data, not change it. |

### What the container must do

1. **Serve HTTP on `PORT`.** WebSockets on the same port are forwarded too.
2. **Serve under `BASE_PATH`.** pipeyard forwards requests with the path unchanged, so a
   browser asking for `/apps/coffee-orders/coffee-dashboard/static/app.js` reaches your
   app as exactly that path. Most frameworks have one setting for this:

   | Framework | Setting |
   |---|---|
   | Streamlit | `--server.baseUrlPath "$BASE_PATH" --server.port "$PORT" --server.address 0.0.0.0` |
   | Dash | `Dash(url_base_pathname=os.environ["BASE_PATH"] + "/")` |
   | Flask | mount the app under `BASE_PATH`, e.g. with `DispatcherMiddleware` |
   | Shiny, static sites | use relative links and they work under any prefix |

3. **Answer a `GET` on `BASE_PATH/`** with any HTTP status. pipeyard uses this to tell
   that the app has finished starting.
4. **Read only finished runs.** List `INPUT_URI`, and use the Parquet files of `run=`
   folders that contain `_manifest.json`. A run still in progress has no manifest yet.

Things to know:

- **Lifetime.** The container is stopped `lifetime_seconds` after it started, however busy
  it is, and is started afresh the next time someone presses **Start report**. One
  instance runs per report, shared by everyone in the org who opens it.
- **Read-only filesystem.** As for pipelines, only `/tmp` is writable. Many frameworks
  write to the home folder; setting `ENV HOME=/tmp` in the image takes care of it.
- **Sign-in is pipeyard's job.** Only signed-in members of the org can reach the app, and
  pipeyard removes its own cookie and any `Authorization` header before forwarding. Your
  app's own cookies pass through untouched.
- **Same origin.** The app is served from pipeyard's own address. Streamlit needs
  `--server.enableCORS false --server.enableXsrfProtection false` behind a proxy like this.
- **If it exits**, the report page shows the exit code and, locally, its last log lines.

Only the Streamlit row of that table is exercised by pipeyard's own example and tests; the
others are the frameworks' documented settings.

## S3 layout

```
orgs/{org_id}/pipelines/{name}/pipeline.yaml
orgs/{org_id}/runs/{run_id}.json
orgs/{org_id}/datasets/{dataset}/run={run_id}/*.parquet + _manifest.json
orgs/{org_id}/reports/{pipeline}/{slug}.json
```

`run={run_id}` is a Hive-style partition, and runs append: a dataset is every finished
run's folder read together.

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

`status` is `running`, `succeeded` or `failed`. `reports/{pipeline}/{slug}.json` is
pipeyard's note of a report's current app: `starting`, `running` or `stopped`, when it
expires, and where to reach it.

## Examples

- [Your first pipeline](your-first-pipeline.md) builds a complete one step by step.
- `examples/pipelines/fake-transactions/` is the pipeline behind `task demo`: about 50
  lines of Python that generate rows with DuckDB into `/tmp`, upload the Parquet file with
  boto3, then write the manifest. Its `report/` folder is a Streamlit dashboard.

The code that enforces this page is `src/pipeyard/contract.py` (the models and the S3
layout), `reconcile()` in `src/pipeyard/app.py` (the success check) and
`src/pipeyard/proxy.py` (what is forwarded to a report).
