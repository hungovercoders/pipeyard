"""End-to-end demo: register the example pipeline, run it, then start its report app.

Local:  task demo          (token from the dev stub)
AWS:    task demo-aws      (token from Cognito; needs PIPEYARD_USERNAME / PIPEYARD_PASSWORD)
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PIPELINE = Path(__file__).parent.parent / "examples/pipelines/fake-transactions/pipeline.yaml"


def call(url: str, token: str | None = None, method: str = "GET", body: bytes | None = None):
    request = urllib.request.Request(url, data=body, method=method)  # noqa: S310
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    if body is not None:
        request.add_header("Content-Type", "application/yaml")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            return json.load(response)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {url} -> {e.code}: {e.read().decode()}")
    except urllib.error.URLError as e:
        sys.exit(f"cannot reach {url} ({e.reason}). Is the control plane running? (task up)")


def fetch(url: str, token: str) -> bytes:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})  # noqa: S310
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            return response.read()
    except urllib.error.HTTPError as e:
        sys.exit(f"GET {url} -> {e.code}: {e.read().decode()[:300]}")


def cognito_token(client_id: str, username: str, password: str) -> str:
    import boto3

    result = boto3.client("cognito-idp").initiate_auth(
        ClientId=client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": username, "PASSWORD": password},
    )
    return result["AuthenticationResult"]["IdToken"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=os.environ.get("PIPEYARD_URL", "http://localhost:8000"))
    parser.add_argument("--image", help="override the pipeline image (e.g. an ECR URI)")
    parser.add_argument("--report-image", help="override the report image (e.g. an ECR URI)")
    parser.add_argument("--cognito-client-id", default=os.environ.get("PIPEYARD_COGNITO_CLIENT_ID"))
    args = parser.parse_args()
    url = args.url.rstrip("/")

    if args.cognito_client_id:
        try:
            username, password = os.environ["PIPEYARD_USERNAME"], os.environ["PIPEYARD_PASSWORD"]
        except KeyError:
            sys.exit("set PIPEYARD_USERNAME and PIPEYARD_PASSWORD (see `task create-user`)")
        token = cognito_token(args.cognito_client_id, username, password)
    else:
        token = call(f"{url}/dev/token", method="POST")["token"]

    lines = PIPELINE.read_text().splitlines()
    for i, line in enumerate(lines):
        if args.image and line.startswith("image:"):
            lines[i] = f"image: {args.image}"
        if args.report_image and line.startswith("    image:"):
            lines[i] = f"    image: {args.report_image}"
    text = "\n".join(lines)
    spec = call(f"{url}/api/pipelines", token, "POST", text.encode())
    name = spec["name"]
    print(f"registered pipeline '{name}' ({spec['image']})")

    run = call(f"{url}/api/pipelines/{name}/runs", token, "POST")
    print(f"triggered run {run['run_id']}", end="", flush=True)
    deadline = time.time() + spec["timeout_seconds"] + 120
    while run["status"] == "running":
        if time.time() > deadline:
            sys.exit("\nrun is still running past its timeout; check the control plane")
        time.sleep(2)
        print(".", end="", flush=True)
        run = call(f"{url}/api/runs/{run['run_id']}", token)
    print(f" {run['status']}")
    if run["status"] != "succeeded":
        sys.exit(f"run failed: {run['error']}")
    print(f"  {run['row_count']} rows -> {run['output_uri']}")

    report = spec["reports"][0]
    slug = "transactions-dashboard"
    status = call(f"{url}/api/pipelines/{name}/reports/{slug}/start", token, "POST")
    print(f"\nstarting report '{report['name']}' ({report['image']})", end="", flush=True)
    deadline = time.time() + 300
    while status["status"] == "starting":
        if time.time() > deadline:
            sys.exit("\nthe report app is still starting after 5 minutes")
        time.sleep(2)
        print(".", end="", flush=True)
        status = call(f"{url}/api/pipelines/{name}/reports/{slug}", token)
    print(f" {status['status']}")
    if status["status"] != "running":
        sys.exit(f"report did not start: {status['error']}")
    served = fetch(f"{url}{status['url']}", token)
    print(f"  the app answered through pipeyard's proxy ({len(served):,} bytes of HTML)")
    print(f"  it stops by itself at {status['expires_at']}")
    print(f"\nopen {url}/pipelines/{name}/reports/{slug}")


if __name__ == "__main__":
    main()
