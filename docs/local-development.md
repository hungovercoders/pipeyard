# Local development

Everything runs on your machine: pipeyard itself, your pipeline containers in Docker, and a
local stand-in for S3. No AWS account is needed.

## Commands

| Command | What it does |
|---|---|
| `task up` | Starts the local S3 emulator and pipeyard on <http://localhost:8000>. Leave it running. |
| `task demo` | Builds the example images, registers the pipeline and its data contract, runs it and starts its report |
| `task down` | Stops the emulator. This also deletes all local data. |
| `task test` | Runs the tests. Add pytest arguments after `--`, e.g. `task test -- -k timeout`. |
| `task lint` | Ruff lint and format check |
| `task fmt` | Auto-format and fix |
| `task build-example` | Builds only the example pipeline and report images |

Run `task` with no arguments to list them all, including the AWS ones.

## Signing in

Locally there is nothing to sign in to. Opening the UI signs you in as the org `demo`, and
`POST /dev/token` hands out an API token for it. This stub does not exist in AWS mode, and
it is switched off as soon as you configure a sign-in provider. To try real sign-in on your
machine, see [Signing in](sign-in.md#trying-it-locally-with-keycloak).

## Local S3: the AWS Emulator

pipeyard uses the [AWS Emulator](https://github.com/aws/aws-emulator), AWS's own local
emulator of its APIs, as its S3. AWS publishes it as a package rather than a container
image, so `emulator/Dockerfile` builds an image from the official package the first time
you run `task up`. That first build downloads about 200 MB.

What to know about it:

- **Your data is in memory.** `task down`, or any restart of the emulator, clears every
  pipeline, run and dataset. Run `task demo` to repopulate.
- **It speaks HTTPS** on `https://127.0.0.1:9297`, with a certificate it generates each
  time it starts and writes to `.emulator/9297/ca.pem`. pipeyard reads that file, so if
  the emulator restarts, restart `task up` too.
- **Run containers share its network.** The certificate is only valid for `localhost` and
  `127.0.0.1`, so pipeline containers are started inside the emulator's network namespace
  and reach it at the same address pipeyard does.
- **Telemetry is off.** The emulator reports usage to AWS by default; pipeyard starts it
  with `--telemetry off`. It also passes `--auth off`, so any credentials are accepted.

To look inside the bucket with the AWS CLI:

```sh
export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_REGION=us-east-1
export AWS_CA_BUNDLE=.emulator/9297/ca.pem
aws --endpoint-url https://127.0.0.1:9297 s3 ls --recursive s3://pipeyard/
```

## How local runs and reports work

When you trigger a run, pipeyard calls `docker run` with the image from your
`pipeline.yaml`. The container gets a read-only filesystem with a writable `/tmp`, the
`cpu` and `memory` limits you declared, and the environment variables from
[the contract](container-contract.md). Images come from your local Docker, so
`docker build` is all it takes to publish a change.

A report is started the same way when you open it, with one difference: it
is given a free port from the range 8600 to 8619 to listen on, which the emulator container
publishes on your machine's loopback. pipeyard proxies to it there. That allows 20 reports
at once locally. You can also open `http://localhost:8600` directly to see an app without
the proxy while debugging, at the path in its `BASE_PATH`.

## Tests

`task test` starts the emulator and runs about 290 tests against it. They use the real app
and real S3 calls, with a fake runner in place of Docker and a tiny stand-in web server in
place of a report container, so they finish in seconds. Only `task demo` exercises real
containers.

Every push to `main` and every pull request runs `task lint` and `task test` in GitHub
Actions (`.github/workflows/ci.yml`).

| File | What it covers |
|---|---|
| `tests/test_local_loop.py` | The main flow: register, run, datasets, UI pages, the register form |
| `tests/test_datacontracts.py` | ODCS data contracts: registering, type matching, runs that break one |
| `tests/test_dataquality.py` | ODCS quality rules: each metric and operator, SQL rules and their sandbox, warnings |
| `tests/test_lineage.py` | OpenLineage events for runs and reports, and posting them to a stand-in server |
| `tests/test_oidc.py` | Signing in through an OpenID Connect provider, and the ways a sign-in can be forged or go wrong |
| `tests/test_reports.py` | Report apps: start, stop, expiry, the HTTP and WebSocket proxy, what is never forwarded |
| `tests/test_org_scoping.py` | One org cannot see or reach another's data; bad tokens; path tricks |
| `tests/test_contract.py` | `pipeline.yaml` and manifest validation, and the error messages |
| `tests/test_aws_mode.py` | Fargate runner, credentials and Cognito tokens with stubbed clients; the synthesized stack |
| `tests/test_review_fixes.py` | Regressions: failed runs leaving files, concurrent readers, timeouts |

## Settings

pipeyard reads its configuration from environment variables. The defaults are right for
local use; you should rarely need these.

| Variable | Default | Meaning |
|---|---|---|
| `PIPEYARD_MODE` | `local` | `local` or `aws`. Anything else refuses to start. |
| `PIPEYARD_BUCKET` | `pipeyard` | The bucket name |
| `PIPEYARD_S3_ENDPOINT_URL` | `https://127.0.0.1:9297` | Where pipeyard reaches local S3 |
| `PIPEYARD_S3_CA_BUNDLE` | `.emulator/9297/ca.pem` | The emulator's certificate |
| `PIPEYARD_RUNNER_S3_ENDPOINT_URL` | `https://127.0.0.1:9297` | Where run containers reach local S3 |
| `PIPEYARD_DOCKER_NETWORK` | `container:pipeyard-aws` | The `--network` run containers use |
| `PIPEYARD_REPORT_PORTS` | `8600-8619` | Ports local reports listen on; must match `docker-compose.yml` |
| `PIPEYARD_DEV_ORG_ID` | `demo` | The org the local stub signs you in as |

## When something goes wrong

**Everything disappeared.** The emulator restarted, and it keeps data in memory. Running
reports stop too, because they share its network. Run
`task demo`, or register your pipelines again.

**Certificate or SSL errors after restarting the emulator.** It generated a new
certificate. Stop `task up` and start it again.

**A run fails with `could not start: docker run failed: Unable to find image`.** The image
named in `pipeline.yaml` is not in your local Docker. Build it with the same name and tag.

**`docker build` fails pulling `python:3.12-slim` with a rate-limit error.** Docker Hub
limits anonymous pulls. Use a mirror:
`BASE_IMAGE=public.ecr.aws/docker/library/python:3.12-slim task demo`.

**Tests stop with "AWS emulator not reachable".** Start it with `task emulator`, which
`task test` normally does for you.

**A report stays "starting".** pipeyard is waiting for the app to answer on `$PORT` under
`$BASE_PATH`. Check that the app uses both, and that it listens on `0.0.0.0`, not only on
`127.0.0.1`. `docker logs` on the `pipeyard-...` container shows what it is doing.

**A report shows a blank page or missing styles.** The app is not serving under
`$BASE_PATH`, so its links point at pipeyard instead of at itself. See the framework
settings in [the contract](container-contract.md#report-containers).

**A report stopped while I was away.** That is scale to zero: it had been unused for
`idle_timeout_seconds` (5 minutes by default). Open it again and it restarts.

**A report says all local report ports are in use.** Twenty are available. Stop a report
you are not using.

**A run stays "running".** Run status is settled when somebody looks at it, so reload the
page. If the container really is stuck, it is stopped at `timeout_seconds`.
