"""Runs one pipeline container. Two implementations of the same two-method interface.

The control plane never waits on a container: ``start`` returns a handle that is stored in
the run record, and ``poll`` is called when someone next reads a run that is still running.
"""

import json
import os
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import boto3

from .config import Settings
from .contract import PipelineSpec

TIMEOUT_EXIT_CODE = 124


@dataclass
class RunState:
    running: bool
    exit_code: int | None = None
    detail: str = ""  # why it failed, if it did: log tail or stop reason


class Runner(Protocol):
    def start(self, spec: PipelineSpec, org_id: str, run_id: str, env: dict[str, str]) -> str:
        """Start the container with ``env`` and return a handle for ``poll``."""

    def poll(self, handle: str) -> RunState:
        """Report whether the container is still running and, if not, how it ended."""


class DockerRunner:
    """Local mode: ``docker run`` in the AWS Emulator's network namespace."""

    CA_PATH = "/etc/pipeyard/emulator-ca.pem"

    def __init__(self, settings: Settings):
        self.network = settings.docker_network
        self.ca_bundle = settings.s3_ca_bundle

    def start(self, spec: PipelineSpec, org_id: str, run_id: str, env: dict[str, str]) -> str:
        name = f"pipeyard-{run_id}"
        cmd = [
            "docker",
            "run",
            "--detach",
            "--name",
            name,
            "--network",
            self.network,
            "--read-only",
            "--tmpfs",
            "/tmp",  # same shape as the Fargate task
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--cpus",
            str(spec.cpu / 1024),
            "--memory",
            f"{spec.memory}m",
            "--label",
            f"pipeyard.org_id={org_id}",
            "--label",
            f"pipeyard.run_id={run_id}",
        ]
        if self.ca_bundle:
            # Let the container's AWS SDK trust the emulator's certificate. The emulator
            # writes it owner-only and rewrites it at each start, so mount a fresh
            # world-readable copy (it is a public certificate).
            source = Path(self.ca_bundle).resolve()
            copy = source.parent / "ca-for-runs.pem"
            copy.write_bytes(source.read_bytes())
            copy.chmod(0o644)
            cmd += ["--volume", f"{copy}:{self.CA_PATH}:ro"]
            env = {**env, "AWS_CA_BUNDLE": self.CA_PATH}
        for key in env:
            cmd += ["--env", key]  # values come from the subprocess env, not the command line
        cmd.append(spec.image)
        proc = subprocess.run(cmd, env={**_base_env(), **env}, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"docker run failed: {proc.stderr.strip()}")
        # Hard timeout. `docker kill` on a finished container is a harmless error.
        timer = threading.Timer(
            spec.timeout_seconds,
            subprocess.run,
            [["docker", "kill", name]],
            {"capture_output": True},
        )
        timer.daemon = True
        timer.start()
        return name

    def poll(self, handle: str) -> RunState:
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{json .State}}", handle],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return RunState(False, None, "container no longer exists")
        state = json.loads(proc.stdout)
        if state["Status"] in ("created", "running"):
            return RunState(True)
        code = state["ExitCode"]
        detail = ""
        if code != 0:
            logs = subprocess.run(
                ["docker", "logs", "--tail", "15", handle], capture_output=True, text=True
            )
            detail = (logs.stdout + logs.stderr).strip()
            if code == 137:
                detail = f"killed (timeout or out of memory)\n{detail}".strip()
        subprocess.run(["docker", "rm", "--force", handle], capture_output=True)
        return RunState(False, code, detail)


def _base_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "DOCKER_HOST")}


class FargateRunner:
    """AWS mode: ECS ``RunTask`` on Fargate.

    The stack owns a template task definition (read-only root filesystem, /tmp scratch
    volume, logging, timeout sidecar). Images cannot be overridden at RunTask time, so each
    run registers a revision of ``pipeyard-{org}-{pipeline}`` copied from the template with
    the pipeline's image, cpu and memory.
    """

    def __init__(self, settings: Settings, ecs=None):
        self.settings = settings
        self.ecs = ecs or boto3.client("ecs", region_name=settings.region)

    def start(self, spec: PipelineSpec, org_id: str, run_id: str, env: dict[str, str]) -> str:
        template = self.ecs.describe_task_definition(
            taskDefinition=self.settings.run_task_template
        )["taskDefinition"]
        containers = template["containerDefinitions"]
        next(c for c in containers if c["name"] == "pipeline")["image"] = spec.image
        tags = [{"key": "org_id", "value": org_id}, {"key": "pipeline", "value": spec.name}]
        task_def = self.ecs.register_task_definition(
            family=f"pipeyard-{org_id}-{spec.name}",
            containerDefinitions=containers,
            cpu=str(spec.cpu),
            memory=str(spec.memory),
            networkMode="awsvpc",
            requiresCompatibilities=["FARGATE"],
            executionRoleArn=template["executionRoleArn"],
            volumes=template.get("volumes", []),
            runtimePlatform=template.get("runtimePlatform", {"operatingSystemFamily": "LINUX"}),
            tags=tags,
        )["taskDefinition"]["taskDefinitionArn"]
        result = self.ecs.run_task(
            cluster=self.settings.ecs_cluster,
            taskDefinition=task_def,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": self.settings.run_subnets.split(","),
                    "securityGroups": [self.settings.run_security_group],
                    "assignPublicIp": "ENABLED",
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": "pipeline",
                        "environment": [{"name": k, "value": v} for k, v in env.items()],
                    },
                    {
                        "name": "timeout",
                        "environment": [
                            {"name": "TIMEOUT_SECONDS", "value": str(spec.timeout_seconds)}
                        ],
                    },
                ]
            },
            tags=[*tags, {"key": "run_id", "value": run_id}],
        )
        if not result["tasks"]:
            reasons = "; ".join(f["reason"] for f in result.get("failures", [])) or "unknown"
            raise RuntimeError(f"ECS did not start the task: {reasons}")
        return result["tasks"][0]["taskArn"]

    def poll(self, handle: str) -> RunState:
        result = self.ecs.describe_tasks(cluster=self.settings.ecs_cluster, tasks=[handle])
        if not result["tasks"]:
            return RunState(False, None, "task no longer exists")
        task = result["tasks"][0]
        if task["lastStatus"] != "STOPPED":
            return RunState(True)
        codes = {c["name"]: c.get("exitCode") for c in task["containers"]}
        if codes.get("timeout") == TIMEOUT_EXIT_CODE:
            return RunState(False, TIMEOUT_EXIT_CODE, "timed out")
        code = codes.get("pipeline")
        detail = "" if code == 0 else task.get("stoppedReason", "")
        return RunState(False, code, detail)


def make_runner(settings: Settings) -> Runner:
    return FargateRunner(settings) if settings.is_aws else DockerRunner(settings)
