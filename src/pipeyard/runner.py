"""Runs one container: a pipeline run, or a report app. Two implementations, one interface.

The control plane never waits on a container: ``start`` returns a handle that is stored in
the run (or report app) record, and ``poll`` is called when someone next reads it.
"""

import json
import os
import subprocess
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import boto3

from .config import Settings

TIMEOUT_EXIT_CODE = 124
TIMEOUT_LABEL = "pipeyard.timeout_seconds"


@dataclass(frozen=True)
class Job:
    """What to run. A pipeline run exits by itself; a report app serves on ``port``."""

    name: str  # labels the container / names the task definition family
    image: str
    cpu: int
    memory: int
    timeout_seconds: int  # hard limit: a run's timeout, or a report app's lifetime
    port: int | None = None


@dataclass
class RunState:
    running: bool
    exit_code: int | None = None
    detail: str = ""  # why it failed, if it did: log tail or stop reason
    gone: bool = False  # the runner no longer knows this container; nothing can be said


class Runner(Protocol):
    def start(self, job: Job, org_id: str, run_id: str, env: dict[str, str]) -> str:
        """Start the container with ``env`` and return a handle for ``poll``."""

    def poll(self, handle: str) -> RunState:
        """Report whether the container is still running and, if not, how it ended.

        Must be safe to call repeatedly and concurrently: it never discards the outcome.
        """

    def cleanup(self, handle: str) -> None:
        """Called once the outcome has been saved to the run record."""

    def address(self, handle: str, port: int) -> str | None:
        """``host:port`` where the control plane can reach a report app, once it has one."""

    def stop(self, handle: str) -> None:
        """Stop a container now. Harmless if it has already gone."""


class DockerRunner:
    """Local mode: ``docker run`` in the AWS Emulator's network namespace."""

    CA_PATH = "/etc/pipeyard/emulator-ca.pem"

    def __init__(self, settings: Settings):
        self.network = settings.docker_network
        self.ca_bundle = settings.s3_ca_bundle

    def start(self, job: Job, org_id: str, run_id: str, env: dict[str, str]) -> str:
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
            str(job.cpu / 1024),
            "--memory",
            f"{job.memory}m",
            "--label",
            f"pipeyard.org_id={org_id}",
            "--label",
            f"pipeyard.run_id={run_id}",
            "--label",
            f"{TIMEOUT_LABEL}={job.timeout_seconds}",
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
        cmd.append(job.image)
        proc = subprocess.run(cmd, env={**_base_env(), **env}, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"docker run failed: {proc.stderr.strip()}")
        # Hard timeout, even if nobody is watching the run. `docker kill` on a finished
        # container is a harmless error. If this process restarts the timer is lost, so
        # poll() enforces the same deadline from the container's own start time.
        timer = threading.Timer(
            job.timeout_seconds,
            subprocess.run,
            [["docker", "kill", name]],
            {"capture_output": True},
        )
        timer.daemon = True
        timer.start()
        return name

    def poll(self, handle: str) -> RunState:
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{json .}}", handle],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return RunState(False, None, "container no longer exists", gone=True)
        info = json.loads(proc.stdout)
        state = info["State"]
        timeout = int(info["Config"]["Labels"].get(TIMEOUT_LABEL, 0))
        started = _docker_time(state["StartedAt"])
        if state["Status"] in ("created", "running"):
            overdue = (
                timeout and started and (datetime.now(UTC) - started).total_seconds() > timeout
            )
            if not overdue:
                return RunState(True)
            subprocess.run(["docker", "kill", handle], capture_output=True)
            return RunState(False, TIMEOUT_EXIT_CODE, "timed out")
        code = state["ExitCode"]
        if code == 137 and not state.get("OOMKilled"):
            finished = _docker_time(state["FinishedAt"])
            if timeout and started and finished and (finished - started).total_seconds() >= timeout:
                return RunState(False, TIMEOUT_EXIT_CODE, "timed out")
        detail = ""
        if code != 0:
            logs = subprocess.run(
                ["docker", "logs", "--tail", "15", handle], capture_output=True, text=True
            )
            detail = (logs.stdout + logs.stderr).strip()
            if state.get("OOMKilled"):
                detail = f"out of memory\n{detail}".strip()
        return RunState(False, code, detail)

    def cleanup(self, handle: str) -> None:
        subprocess.run(["docker", "rm", "--force", handle], capture_output=True)

    def address(self, handle: str, port: int) -> str | None:
        # The container shares the emulator's network namespace, and the emulator publishes
        # the report port range on the host's loopback (see docker-compose.yml).
        return f"127.0.0.1:{port}"

    def stop(self, handle: str) -> None:
        self.cleanup(handle)


def _docker_time(value: str) -> datetime | None:
    """Docker timestamps have nanoseconds; '0001-01-01...' means 'not yet'."""
    if value.startswith("0001"):
        return None
    head, _, fraction = value.rstrip("Z").partition(".")
    return datetime.fromisoformat(f"{head}.{(fraction + '000000')[:6]}+00:00")


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

    def start(self, job: Job, org_id: str, run_id: str, env: dict[str, str]) -> str:
        template = self.ecs.describe_task_definition(
            taskDefinition=self.settings.run_task_template
        )["taskDefinition"]
        containers = template["containerDefinitions"]
        main = next(c for c in containers if c["name"] == "pipeline")
        main["image"] = job.image
        if job.port:
            main["portMappings"] = [{"containerPort": job.port, "protocol": "tcp"}]
        # Report apps accept connections from the control plane; runs accept none.
        security_group = (
            self.settings.report_security_group if job.port else self.settings.run_security_group
        )
        tags = [{"key": "org_id", "value": org_id}, {"key": "pipeline", "value": job.name}]
        task_def = self.ecs.register_task_definition(
            family=f"pipeyard-{org_id}-{job.name}"[:255],
            containerDefinitions=containers,
            cpu=str(job.cpu),
            memory=str(job.memory),
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
                    "securityGroups": [security_group],
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
                            {"name": "TIMEOUT_SECONDS", "value": str(job.timeout_seconds)}
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
        if not result["tasks"]:  # ECS forgets stopped tasks after about an hour
            return RunState(False, None, "task no longer exists", gone=True)
        task = result["tasks"][0]
        if task["lastStatus"] != "STOPPED":
            return RunState(True)
        codes = {c["name"]: c.get("exitCode") for c in task["containers"]}
        if codes.get("timeout") == TIMEOUT_EXIT_CODE:
            return RunState(False, TIMEOUT_EXIT_CODE, "timed out")
        code = codes.get("pipeline")
        detail = "" if code == 0 else task.get("stoppedReason", "")
        return RunState(False, code, detail)

    def cleanup(self, handle: str) -> None:
        pass  # ECS removes stopped tasks itself

    def address(self, handle: str, port: int) -> str | None:
        result = self.ecs.describe_tasks(cluster=self.settings.ecs_cluster, tasks=[handle])
        for task in result["tasks"]:
            for attachment in task.get("attachments", []):
                for detail in attachment.get("details", []):
                    if detail["name"] == "privateIPv4Address":
                        return f"{detail['value']}:{port}"
        return None  # the task has no network interface yet

    def stop(self, handle: str) -> None:
        try:
            self.ecs.stop_task(
                cluster=self.settings.ecs_cluster, task=handle, reason="report stopped"
            )
        except self.ecs.exceptions.ClientError:
            pass  # already gone


def make_runner(settings: Settings) -> Runner:
    return FargateRunner(settings) if settings.is_aws else DockerRunner(settings)
