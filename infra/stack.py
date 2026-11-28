"""The whole of pipeyard in one stack.

v0 tradeoff: everything runs in public subnets with public IPs, so there is no NAT gateway
and no VPC endpoints to pay for. Security groups allow no inbound traffic to tasks except
from the load balancer, but egress is open. Locking egress down (private subnets + VPC
endpoints for S3/ECR/Logs/STS) is the next step.
"""

from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_certificatemanager as acm
from aws_cdk import aws_cognito as cognito
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_ecs_patterns as ecs_patterns
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct

ROOT = str(Path(__file__).parent.parent)
BUSYBOX = "public.ecr.aws/docker/library/busybox:stable"
REPORT_PORT = 8080  # must match FARGATE_REPORT_PORT in src/pipeyard/app.py
ORG_TAG = "${aws:PrincipalTag/org_id}"  # resolved by IAM at request time, not by CDK


class PipeyardStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        org_id: str,
        certificate_arn: str | None = None,
        domain_name: str | None = None,
        oidc_issuer: str | None = None,
        oidc_client_id: str | None = None,
        oidc_org_claim: str | None = None,
        openlineage_url: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC)
            ],
        )
        bucket = s3.Bucket(
            self,
            "Data",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        repo = ecr.Repository(self, "Pipelines", removal_policy=RemovalPolicy.RETAIN)
        cluster = ecs.Cluster(self, "Cluster", vpc=vpc)

        # --- per-run task ------------------------------------------------------------
        # The template the control plane copies for each pipeline (it swaps in the image,
        # cpu and memory). Security settings live here, in IaC, not in application code.
        run_logs = logs.LogGroup(
            self,
            "RunLogs",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )
        execution_role = iam.Role(
            self,
            "RunExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        repo.grant_pull(execution_role)
        template = ecs.FargateTaskDefinition(
            self,
            "RunTemplate",
            cpu=256,
            memory_limit_mib=512,
            execution_role=execution_role,
            volumes=[ecs.Volume(name="tmp")],  # ephemeral scratch, mounted at /tmp
        )
        pipeline = template.add_container(
            "pipeline",
            image=ecs.ContainerImage.from_registry(BUSYBOX),  # placeholder, replaced per run
            essential=True,
            readonly_root_filesystem=True,
            logging=ecs.LogDrivers.aws_logs(stream_prefix="run", log_group=run_logs),
        )
        pipeline.add_mount_points(
            ecs.MountPoint(container_path="/tmp", source_volume="tmp", read_only=False)
        )
        # Hard timeout that does not depend on the control plane being alive: an essential
        # sidecar that exits after TIMEOUT_SECONDS, which makes ECS stop the whole task.
        template.add_container(
            "timeout",
            image=ecs.ContainerImage.from_registry(BUSYBOX),
            essential=True,
            readonly_root_filesystem=True,
            memory_reservation_mib=16,
            environment={"TIMEOUT_SECONDS": "600"},
            # The trap lets it stop at once when ECS ends the task; without it, PID 1 ignores
            # SIGTERM and every run would linger for the 30s stop timeout.
            command=[
                "sh",
                "-c",
                'trap "exit 0" TERM; sleep "$TIMEOUT_SECONDS" & wait $!; exit 124',
            ],
        )
        run_security_group = ec2.SecurityGroup(
            self,
            "RunSecurityGroup",
            vpc=vpc,
            description="pipeyard run tasks: no inbound",
            allow_all_outbound=True,
        )

        # Report apps are web servers: the control plane (and nothing else) may connect to
        # them, on the one port they are told to listen on.
        report_security_group = ec2.SecurityGroup(
            self,
            "ReportSecurityGroup",
            vpc=vpc,
            description="pipeyard report apps: inbound from the control plane only",
            allow_all_outbound=True,
        )

        # --- auth --------------------------------------------------------------------
        certificate = (
            acm.Certificate.from_certificate_arn(self, "Certificate", certificate_arn)
            if certificate_arn
            else None
        )
        # Where browsers reach pipeyard. Known only when you bring a domain and certificate.
        public_url = f"https://{domain_name}" if certificate and domain_name else None

        # Three ways to sign in, most to least preferable:
        #   1. your own OpenID Connect provider (-c oidc_issuer=... -c oidc_client_id=...),
        #   2. the Cognito pool below through its hosted sign-in page, which needs an HTTPS
        #      address to come back to (-c certificate_arn=... -c domain_name=...),
        #   3. the Cognito pool through pipeyard's own password form: the fallback for a
        #      first deploy with no domain.
        sign_in: dict[str, str] = {}
        user_pool = client = None
        if oidc_issuer:
            if not oidc_client_id:
                raise ValueError("oidc_issuer needs oidc_client_id")
            sign_in = {
                "PIPEYARD_OIDC_ISSUER": oidc_issuer,
                "PIPEYARD_OIDC_CLIENT_ID": oidc_client_id,
                "PIPEYARD_OIDC_ORG_CLAIM": oidc_org_claim or "org_id",
            }
        else:
            user_pool = cognito.UserPool(
                self,
                "Users",
                self_sign_up_enabled=False,
                sign_in_aliases=cognito.SignInAliases(email=True),
                # Immutable and not client-writable: nobody can move themselves to another org.
                custom_attributes={"org_id": cognito.StringAttribute(mutable=False, max_len=63)},
                removal_policy=RemovalPolicy.DESTROY,
            )
            client = user_pool.add_client(
                "App",
                auth_flows=cognito.AuthFlow(user_password=True),
                generate_secret=False,
                id_token_validity=Duration.hours(1),
                read_attributes=cognito.ClientAttributes()
                .with_standard_attributes(email=True, email_verified=True)
                .with_custom_attributes("org_id"),
                write_attributes=cognito.ClientAttributes().with_standard_attributes(email=True),
                disable_o_auth=not public_url,
                o_auth=cognito.OAuthSettings(
                    flows=cognito.OAuthFlows(authorization_code_grant=True),
                    scopes=[
                        cognito.OAuthScope.OPENID,
                        cognito.OAuthScope.EMAIL,
                        cognito.OAuthScope.PROFILE,
                    ],
                    callback_urls=[f"{public_url}/auth/callback"],
                    logout_urls=[f"{public_url}/login"],
                )
                if public_url
                else None,
            )
            sign_in = {
                "PIPEYARD_COGNITO_POOL_ID": user_pool.user_pool_id,
                "PIPEYARD_COGNITO_CLIENT_ID": client.user_pool_client_id,
            }
            if public_url:
                # The hosted sign-in page lives on a Cognito domain; the prefix must be
                # unique in the region, hence the account id.
                user_pool.add_domain(
                    "SignInDomain",
                    cognito_domain=cognito.CognitoDomainOptions(
                        domain_prefix=f"pipeyard-{self.account}"
                    ),
                )
                sign_in |= {
                    "PIPEYARD_OIDC_ISSUER": user_pool.user_pool_provider_url,
                    "PIPEYARD_OIDC_CLIENT_ID": client.user_pool_client_id,
                    "PIPEYARD_OIDC_ORG_CLAIM": "custom:org_id",
                }
        if public_url:
            sign_in["PIPEYARD_PUBLIC_URL"] = public_url

        # --- control plane -----------------------------------------------------------
        service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            "ControlPlane",
            cluster=cluster,
            cpu=512,
            memory_limit_mib=1024,
            desired_count=1,
            min_healthy_percent=100,
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
            assign_public_ip=True,
            task_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            public_load_balancer=True,
            certificate=certificate,
            protocol=(
                elbv2.ApplicationProtocol.HTTPS if certificate else elbv2.ApplicationProtocol.HTTP
            ),
            redirect_http=bool(certificate),
            task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                image=ecs.ContainerImage.from_asset(
                    ROOT,
                    platform=ecr_assets.Platform.LINUX_AMD64,
                    exclude=["cdk.out", ".venv", ".git", "node_modules"],
                ),
                container_port=8000,
                environment={
                    "PIPEYARD_MODE": "aws",
                    "PIPEYARD_BUCKET": bucket.bucket_name,
                    "AWS_REGION": self.region,
                    **sign_in,
                    "PIPEYARD_ECS_CLUSTER": cluster.cluster_name,
                    "PIPEYARD_RUN_TASK_TEMPLATE": template.family,
                    "PIPEYARD_RUN_SUBNETS": ",".join(s.subnet_id for s in vpc.public_subnets),
                    "PIPEYARD_RUN_SECURITY_GROUP": run_security_group.security_group_id,
                    "PIPEYARD_REPORT_SECURITY_GROUP": report_security_group.security_group_id,
                    "PIPEYARD_SECURE_COOKIES": "true" if certificate else "false",
                },
            ),
        )
        service.target_group.configure_health_check(path="/healthz")
        if openlineage_url:  # also send lineage events to an OpenLineage server
            service.task_definition.default_container.add_environment(
                "OPENLINEAGE_URL", openlineage_url
            )
        report_security_group.connections.allow_from(
            service.service, ec2.Port.tcp(REPORT_PORT), "control plane proxy"
        )
        control_role = service.task_definition.task_role

        # --- S3 access: ABAC on the org_id session tag ----------------------------------
        # One role for every org. The control plane assumes it with Tags=[org_id=...] and
        # hands the resulting credentials to the run container and to DuckDB. The control
        # plane's own role has no S3 permissions at all.
        run_role = iam.Role(
            self,
            "RunRole",
            assumed_by=iam.ArnPrincipal(control_role.role_arn).with_session_tags(),
            max_session_duration=Duration.hours(1),
        )
        run_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                ],
                resources=[bucket.arn_for_objects(f"orgs/{ORG_TAG}/*")],
            )
        )
        run_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": [f"orgs/{ORG_TAG}/*"]}},
            )
        )
        service.task_definition.default_container.add_environment(
            "PIPEYARD_RUN_ROLE_ARN", run_role.role_arn
        )
        control_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["sts:AssumeRole", "sts:TagSession"], resources=[run_role.role_arn]
            )
        )
        control_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["ecs:RegisterTaskDefinition", "ecs:DescribeTaskDefinition"],
                resources=["*"],  # these actions do not support resource-level permissions
            )
        )
        control_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask", "ecs:DescribeTasks", "ecs:StopTask"],
                resources=["*"],
                conditions={"ArnEquals": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        control_role.add_to_principal_policy(
            iam.PolicyStatement(  # tag-on-create only: org_id / run_id tags on runs
                actions=["ecs:TagResource"],
                resources=["*"],
                conditions={
                    "StringEquals": {"ecs:CreateAction": ["RunTask", "RegisterTaskDefinition"]}
                },
            )
        )
        control_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[execution_role.role_arn],
                conditions={"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
            )
        )

        scheme = "https://" if certificate else "http://"
        address = scheme + service.load_balancer.load_balancer_dns_name
        CfnOutput(self, "Url", value=public_url or address)
        if public_url:  # point your domain's DNS at this
            CfnOutput(
                self, "LoadBalancerDnsName", value=service.load_balancer.load_balancer_dns_name
            )
        CfnOutput(self, "OrgId", value=org_id)
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "EcrRepositoryUri", value=repo.repository_uri)
        if user_pool and client:
            CfnOutput(self, "UserPoolId", value=user_pool.user_pool_id)
            CfnOutput(self, "UserPoolClientId", value=client.user_pool_client_id)
        CfnOutput(self, "RunLogGroup", value=run_logs.log_group_name)
