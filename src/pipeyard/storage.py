"""S3 access for the control plane. Keys come from contract.py, so every call is org-scoped."""

import time
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

from . import contract
from .config import Settings


@dataclass(frozen=True)
class Credentials:
    access_key: str
    secret_key: str
    session_token: str | None = None


_cache: dict[str, tuple[float, Credentials]] = {}


def org_credentials(settings: Settings, org_id: str, *, fresh: bool = False) -> Credentials:
    """Credentials that can only touch ``orgs/{org_id}/``.

    AWS: assume the run role with an ``org_id`` session tag; its policy is ABAC on
    ``${aws:PrincipalTag/org_id}``. The same credentials go to run containers and to DuckDB,
    so IAM enforces the org boundary even if application code gets it wrong.
    Local: the emulator accepts any credentials (no IAM to scope with).

    ``fresh=True`` skips the cache so a run container gets the full hour.
    """
    if not settings.is_aws:
        return Credentials(settings.s3_access_key, settings.s3_secret_key)
    cached = _cache.get(org_id)
    if cached and not fresh and cached[0] > time.time():
        return cached[1]
    raw = boto3.client("sts", region_name=settings.region).assume_role(
        RoleArn=settings.run_role_arn,
        RoleSessionName=f"org-{org_id}",
        Tags=[{"Key": "org_id", "Value": org_id}],
        DurationSeconds=3600,  # the maximum when one role assumes another
    )["Credentials"]
    creds = Credentials(raw["AccessKeyId"], raw["SecretAccessKey"], raw["SessionToken"])
    _cache[org_id] = (time.time() + 3000, creds)
    return creds


class Storage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.bucket = settings.bucket
        self._org_clients: dict[str, tuple[Credentials, object]] = {}
        # Local: one client for the emulator. AWS: no client of our own, only per-org ones (_s3).
        self.s3 = None
        if not settings.is_aws:
            self.s3 = boto3.client(
                "s3",
                endpoint_url=settings.s3_endpoint_url,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                region_name=settings.region,
            )

    def _s3(self, org_id: str):
        """The S3 client for one org. In AWS it carries that org's tagged credentials."""
        if self.s3 is not None:
            return self.s3
        creds = org_credentials(self.settings, org_id)
        cached = self._org_clients.get(org_id)
        if cached is None or cached[0] is not creds:
            client = boto3.client(
                "s3",
                region_name=self.settings.region,
                aws_access_key_id=creds.access_key,
                aws_secret_access_key=creds.secret_key,
                aws_session_token=creds.session_token,
            )
            cached = self._org_clients[org_id] = (creds, client)
        return cached[1]

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def ensure_bucket(self) -> None:
        """Local convenience; in AWS the bucket is created by the stack."""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.s3.create_bucket(Bucket=self.bucket)

    def _put(self, org_id: str, key: str, body: str, content_type: str) -> None:
        self._s3(org_id).put_object(
            Bucket=self.bucket, Key=key, Body=body.encode(), ContentType=content_type
        )

    def _get(self, org_id: str, key: str) -> str | None:
        try:
            return self._s3(org_id).get_object(Bucket=self.bucket, Key=key)["Body"].read().decode()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    # pipelines

    def put_pipeline(self, org_id: str, name: str, yaml_text: str) -> None:
        self._put(org_id, contract.pipeline_key(org_id, name), yaml_text, "application/yaml")

    def get_pipeline(self, org_id: str, name: str) -> contract.PipelineSpec | None:
        text = self._get(org_id, contract.pipeline_key(org_id, name))
        return contract.parse_pipeline(text) if text is not None else None

    def list_pipelines(self, org_id: str) -> list[contract.PipelineSpec]:
        prefix = contract.pipelines_prefix(org_id)
        names = []
        for page in (
            self._s3(org_id)
            .get_paginator("list_objects_v2")
            .paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/")
        ):
            names += [p["Prefix"][len(prefix) : -1] for p in page.get("CommonPrefixes", [])]
        specs = (self.get_pipeline(org_id, n) for n in sorted(names))
        return [s for s in specs if s is not None]

    # runs

    def put_run(self, run: contract.RunRecord) -> None:
        self._put(
            run.org_id,
            contract.run_key(run.org_id, run.run_id),
            run.model_dump_json(),
            "application/json",
        )

    def get_run(self, org_id: str, run_id: str) -> contract.RunRecord | None:
        text = self._get(org_id, contract.run_key(org_id, run_id))
        return contract.RunRecord.model_validate_json(text) if text is not None else None

    # datasets

    def get_manifest_text(self, org_id: str, dataset: str, run_id: str) -> str | None:
        return self._get(
            org_id, contract.output_prefix(org_id, dataset, run_id) + contract.MANIFEST_FILE
        )

    def has_parquet(self, org_id: str, dataset: str, run_id: str) -> bool:
        prefix = contract.output_prefix(org_id, dataset, run_id)
        for page in (
            self._s3(org_id)
            .get_paginator("list_objects_v2")
            .paginate(Bucket=self.bucket, Prefix=prefix)
        ):
            if any(o["Key"].endswith(".parquet") for o in page.get("Contents", [])):
                return True
        return False
