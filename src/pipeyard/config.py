"""Settings, read once from environment variables."""

import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # "local": Docker runner, AWS emulator, dev token stub. "aws": Fargate runner, S3, Cognito.
    mode: str = field(default_factory=lambda: _env("PIPEYARD_MODE", "local"))
    bucket: str = field(default_factory=lambda: _env("PIPEYARD_BUCKET", "pipeyard"))
    region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))

    # local mode
    s3_endpoint_url: str = field(
        default_factory=lambda: _env("PIPEYARD_S3_ENDPOINT_URL", "https://127.0.0.1:9297")
    )
    s3_access_key: str = field(default_factory=lambda: _env("PIPEYARD_S3_ACCESS_KEY", "pipeyard"))
    s3_secret_key: str = field(
        default_factory=lambda: _env("PIPEYARD_S3_SECRET_KEY", "pipeyard-secret")
    )
    # The emulator's certificate, rewritten each time it starts. Empty = system trust store.
    s3_ca_bundle: str = field(
        default_factory=lambda: _env("PIPEYARD_S3_CA_BUNDLE", ".emulator/9297/ca.pem")
    )
    # How run containers reach the emulator. Its certificate only covers 127.0.0.1, so they
    # share its network namespace and use the same URL as the host does.
    docker_network: str = field(
        default_factory=lambda: _env("PIPEYARD_DOCKER_NETWORK", "container:pipeyard-aws")
    )
    runner_s3_endpoint_url: str = field(
        default_factory=lambda: _env("PIPEYARD_RUNNER_S3_ENDPOINT_URL", "https://127.0.0.1:9297")
    )
    # Ports report apps listen on locally; docker-compose.yml publishes the same range.
    report_ports: str = field(default_factory=lambda: _env("PIPEYARD_REPORT_PORTS", "8600-8619"))
    dev_org_id: str = field(default_factory=lambda: _env("PIPEYARD_DEV_ORG_ID", "demo"))
    dev_secret: str = field(
        default_factory=lambda: _env("PIPEYARD_DEV_SECRET", "pipeyard-local-dev-secret-not-for-aws")
    )

    # aws mode (all set by the CDK stack)
    cognito_user_pool_id: str = field(default_factory=lambda: _env("PIPEYARD_COGNITO_POOL_ID"))
    cognito_client_id: str = field(default_factory=lambda: _env("PIPEYARD_COGNITO_CLIENT_ID"))
    ecs_cluster: str = field(default_factory=lambda: _env("PIPEYARD_ECS_CLUSTER"))
    run_task_template: str = field(default_factory=lambda: _env("PIPEYARD_RUN_TASK_TEMPLATE"))
    run_role_arn: str = field(default_factory=lambda: _env("PIPEYARD_RUN_ROLE_ARN"))
    run_subnets: str = field(default_factory=lambda: _env("PIPEYARD_RUN_SUBNETS"))
    run_security_group: str = field(default_factory=lambda: _env("PIPEYARD_RUN_SECURITY_GROUP"))
    report_security_group: str = field(
        default_factory=lambda: _env("PIPEYARD_REPORT_SECURITY_GROUP")
    )
    secure_cookies: bool = field(default_factory=lambda: _env("PIPEYARD_SECURE_COOKIES") == "true")

    # Sign-in with any OpenID Connect provider. Setting the issuer switches it on, in either
    # mode, and switches the dev stub and the Cognito password form off.
    oidc_issuer: str = field(default_factory=lambda: _env("PIPEYARD_OIDC_ISSUER"))
    oidc_client_id: str = field(default_factory=lambda: _env("PIPEYARD_OIDC_CLIENT_ID"))
    oidc_client_secret: str = field(default_factory=lambda: _env("PIPEYARD_OIDC_CLIENT_SECRET"))
    # The ID token claim that says which org the person belongs to.
    oidc_org_claim: str = field(default_factory=lambda: _env("PIPEYARD_OIDC_ORG_CLAIM", "org_id"))
    oidc_scopes: str = field(
        default_factory=lambda: _env("PIPEYARD_OIDC_SCOPES", "openid email profile")
    )
    # Where browsers reach pipeyard, e.g. https://pipeyard.example.com. Needed behind a load
    # balancer so the provider is given the right address to send people back to.
    public_url: str = field(default_factory=lambda: _env("PIPEYARD_PUBLIC_URL"))
    session_secret: str = field(default_factory=lambda: _env("PIPEYARD_SESSION_SECRET"))
    session_hours: float = field(default_factory=lambda: float(_env("PIPEYARD_SESSION_HOURS", "8")))

    # OpenLineage, using the standard client's variable names. Unset = events are only stored.
    openlineage_url: str = field(default_factory=lambda: _env("OPENLINEAGE_URL"))
    openlineage_endpoint: str = field(
        default_factory=lambda: _env("OPENLINEAGE_ENDPOINT", "api/v1/lineage")
    )
    openlineage_api_key: str = field(default_factory=lambda: _env("OPENLINEAGE_API_KEY"))
    openlineage_namespace: str = field(
        default_factory=lambda: _env("OPENLINEAGE_NAMESPACE", "pipeyard")
    )

    def __post_init__(self) -> None:
        # Anything other than "aws" used to mean local, which turns on the dev sign-in.
        if self.mode not in ("local", "aws"):
            raise ValueError(f"PIPEYARD_MODE must be 'local' or 'aws', got {self.mode!r}")
        if self.oidc_issuer and not self.oidc_client_id:
            raise ValueError(
                "PIPEYARD_OIDC_ISSUER is set, so PIPEYARD_OIDC_CLIENT_ID is needed too"
            )

    @property
    def is_aws(self) -> bool:
        return self.mode == "aws"
