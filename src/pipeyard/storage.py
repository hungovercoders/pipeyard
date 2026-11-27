"""S3 access for the control plane. Keys come from contract.py, so every call is org-scoped."""

import json
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


READ_ONLY_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": "*"}
        ],
    }
)


def org_credentials(
    settings: Settings, org_id: str, *, fresh: bool = False, read_only: bool = False
) -> Credentials:
    """Credentials that can only touch ``orgs/{org_id}/``.

    AWS: assume the run role with an ``org_id`` session tag; its policy is ABAC on
    ``${aws:PrincipalTag/org_id}``. The same credentials go to run containers and to DuckDB,
    so IAM enforces the org boundary even if application code gets it wrong.
    Local: the emulator accepts any credentials (no IAM to scope with).

    ``fresh=True`` skips the cache so a container gets the full hour. ``read_only=True``
    (report apps) adds a session policy, so the credentials can read the org's prefix but
    not write to it; it implies ``fresh``.
    """
    if not settings.is_aws:
        return Credentials(settings.s3_access_key, settings.s3_secret_key)
    cached = _cache.get(org_id)
    if cached and not fresh and not read_only and cached[0] > time.time():
        return cached[1]
    request = {
        "RoleArn": settings.run_role_arn,
        "RoleSessionName": f"org-{org_id}",
        "Tags": [{"Key": "org_id", "Value": org_id}],
        "DurationSeconds": 3600,  # the maximum when one role assumes another
    }
    if read_only:
        request["Policy"] = READ_ONLY_POLICY
    raw = boto3.client("sts", region_name=settings.region).assume_role(**request)["Credentials"]
    creds = Credentials(raw["AccessKeyId"], raw["SecretAccessKey"], raw["SessionToken"])
    if not read_only:
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
                verify=settings.s3_ca_bundle or None,
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
            code = e.response["Error"]["Code"]
            if code in ("NoSuchKey", "404"):
                return None
            # The run role may only list under its org prefix, and without list permission
            # on the exact request S3 answers 403 for a missing key. Ask with a list call,
            # which the role is allowed to make, whether the key is really there.
            if code == "AccessDenied" and not self._exists(org_id, key):
                return None
            raise

    def _exists(self, org_id: str, key: str) -> bool:
        found = self._s3(org_id).list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)
        return any(o["Key"] == key for o in found.get("Contents", []))

    # pipelines

    def put_pipeline(self, org_id: str, name: str, yaml_text: str) -> None:
        self._put(org_id, contract.pipeline_key(org_id, name), yaml_text, "application/yaml")

    def get_pipeline_text(self, org_id: str, name: str) -> str | None:
        return self._get(org_id, contract.pipeline_key(org_id, name))

    def get_pipeline(self, org_id: str, name: str) -> contract.PipelineSpec | None:
        text = self.get_pipeline_text(org_id, name)
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

    def delete_output(self, org_id: str, dataset: str, run_id: str) -> None:
        """Remove what a failed run left behind, so readers of the dataset never see it."""
        prefix = contract.output_prefix(org_id, dataset, run_id)
        s3 = self._s3(org_id)
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=self.bucket, Key=obj["Key"])

    # report apps

    def put_report_app(self, app: contract.ReportApp) -> None:
        key = contract.report_app_key(app.org_id, app.pipeline, app.slug)
        self._put(app.org_id, key, app.model_dump_json(), "application/json")

    def get_report_app(self, org_id: str, pipeline: str, slug: str) -> contract.ReportApp | None:
        text = self._get(org_id, contract.report_app_key(org_id, pipeline, slug))
        return contract.ReportApp.model_validate_json(text) if text is not None else None

    # data contracts (ODCS documents, stored exactly as registered)

    def put_data_contract(self, org_id: str, dataset: str, text: str) -> None:
        self._put(org_id, contract.data_contract_key(org_id, dataset), text, "application/yaml")

    def get_data_contract(self, org_id: str, dataset: str) -> str | None:
        return self._get(org_id, contract.data_contract_key(org_id, dataset))

    def delete_data_contract(self, org_id: str, dataset: str) -> None:
        key = contract.data_contract_key(org_id, dataset)
        self._s3(org_id).delete_object(Bucket=self.bucket, Key=key)

    def list_data_contracts(self, org_id: str) -> list[str]:
        """Names of the datasets that have a contract."""
        prefix, suffix = contract.data_contracts_prefix(org_id), ".odcs.yaml"
        s3 = self._s3(org_id)
        return sorted(
            obj["Key"][len(prefix) : -len(suffix)]
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix
            )
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(suffix)
        )
