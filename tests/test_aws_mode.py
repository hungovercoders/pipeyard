"""Phase 3, without an AWS account: stubbed clients for the runner and credentials, a local
RSA key for Cognito-style tokens, and assertions on the synthesized template."""

import json
import sys
import time
from pathlib import Path

import boto3
import jwt
import pytest
from botocore.stub import ANY, Stubber
from cryptography.hazmat.primitives.asymmetric import rsa

from pipeyard import storage
from pipeyard.auth import Auth, AuthError
from pipeyard.config import Settings
from pipeyard.runner import TIMEOUT_EXIT_CODE, FargateRunner, Job

AWS = Settings(
    mode="aws",
    bucket="data-bucket",
    region="eu-west-2",
    cognito_user_pool_id="eu-west-2_pool",
    cognito_client_id="client-123",
    ecs_cluster="cluster",
    run_task_template="pipeyard-template",
    run_role_arn="arn:aws:iam::111111111111:role/run",
    run_subnets="subnet-a,subnet-b",
    run_security_group="sg-1",
    report_security_group="sg-reports",
)
TASK = "arn:aws:ecs:eu-west-2:111111111111:task/cluster/abc"


@pytest.fixture
def ecs():
    client = boto3.client(
        "ecs", region_name="eu-west-2", aws_access_key_id="x", aws_secret_access_key="x"
    )
    with Stubber(client) as stub:
        yield client, stub
        stub.assert_no_pending_responses()


def test_fargate_start_copies_the_template_and_tags_the_run(ecs, example_yaml):
    client, stub = ecs
    spec = Job("fake-transactions", "repo/img:1", 512, 1024, 600)
    template_containers = [
        {
            "name": "pipeline",
            "image": "busybox",
            "essential": True,
            "readonlyRootFilesystem": True,
            "mountPoints": [{"sourceVolume": "tmp", "containerPath": "/tmp"}],
        },
        {"name": "timeout", "image": "busybox", "essential": True},
    ]
    stub.add_response(
        "describe_task_definition",
        {
            "taskDefinition": {
                "containerDefinitions": template_containers,
                "executionRoleArn": "arn:aws:iam::111111111111:role/exec",
                "volumes": [{"name": "tmp"}],
            }
        },
        {"taskDefinition": "pipeyard-template"},
    )
    expected_containers = [dict(template_containers[0], image=spec.image), template_containers[1]]
    stub.add_response(
        "register_task_definition",
        {"taskDefinition": {"taskDefinitionArn": "arn:td:7"}},
        {
            "family": "pipeyard-acme-fake-transactions",
            "containerDefinitions": expected_containers,  # read-only root and /tmp survive
            "cpu": "512",
            "memory": "1024",
            "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"],
            "executionRoleArn": "arn:aws:iam::111111111111:role/exec",
            "volumes": [{"name": "tmp"}],
            "runtimePlatform": ANY,
            "tags": ANY,
        },
    )
    stub.add_response(
        "run_task",
        {"tasks": [{"taskArn": TASK}], "failures": []},
        {
            "cluster": "cluster",
            "taskDefinition": "arn:td:7",
            "launchType": "FARGATE",
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": ["subnet-a", "subnet-b"],
                    "securityGroups": ["sg-1"],
                    "assignPublicIp": "ENABLED",
                }
            },
            "overrides": {
                "containerOverrides": [
                    {"name": "pipeline", "environment": [{"name": "ORG_ID", "value": "acme"}]},
                    {
                        "name": "timeout",
                        "environment": [{"name": "TIMEOUT_SECONDS", "value": "600"}],
                    },
                ]
            },
            "tags": [
                {"key": "org_id", "value": "acme"},
                {"key": "pipeline", "value": "fake-transactions"},
                {"key": "run_id", "value": "r1"},
            ],
        },
    )
    assert FargateRunner(AWS, client).start(spec, "acme", "r1", {"ORG_ID": "acme"}) == TASK


def test_fargate_start_reports_capacity_failures(ecs, example_yaml):
    client, stub = ecs
    stub.add_response(
        "describe_task_definition",
        {
            "taskDefinition": {
                "containerDefinitions": [{"name": "pipeline", "image": "b"}],
                "executionRoleArn": "arn:x",
            }
        },
    )
    stub.add_response("register_task_definition", {"taskDefinition": {"taskDefinitionArn": "a"}})
    stub.add_response(
        "run_task", {"tasks": [], "failures": [{"reason": "Capacity is unavailable"}]}
    )
    with pytest.raises(RuntimeError, match="Capacity is unavailable"):
        FargateRunner(AWS, client).start(Job("p", "img", 256, 512, 600), "acme", "r1", {})


@pytest.mark.parametrize(
    ("task", "expected"),
    [
        ({"lastStatus": "PENDING", "containers": []}, (True, None, "")),
        ({"lastStatus": "RUNNING", "containers": []}, (True, None, "")),
        (
            {
                "lastStatus": "STOPPED",
                "containers": [
                    {"name": "pipeline", "exitCode": 0},
                    {"name": "timeout", "exitCode": 137},
                ],
            },
            (False, 0, ""),
        ),
        (
            {
                "lastStatus": "STOPPED",
                "stoppedReason": "Essential container in task exited",
                "containers": [
                    {"name": "pipeline", "exitCode": 2},
                    {"name": "timeout", "exitCode": 137},
                ],
            },
            (False, 2, "Essential container in task exited"),
        ),
        (
            {
                "lastStatus": "STOPPED",
                "containers": [
                    {"name": "pipeline", "exitCode": 137},
                    {"name": "timeout", "exitCode": 124},
                ],
            },
            (False, TIMEOUT_EXIT_CODE, "timed out"),
        ),
        (
            {
                "lastStatus": "STOPPED",
                "stoppedReason": "CannotPullContainerError: not found",
                "containers": [{"name": "pipeline"}, {"name": "timeout"}],
            },
            (False, None, "CannotPullContainerError: not found"),
        ),
    ],
)
def test_fargate_poll(ecs, task, expected):
    client, stub = ecs
    stub.add_response("describe_tasks", {"tasks": [task]}, {"cluster": "cluster", "tasks": [TASK]})
    state = FargateRunner(AWS, client).poll(TASK)
    assert (state.running, state.exit_code, state.detail) == expected


def test_fargate_report_app_is_reachable_only_through_the_report_security_group(ecs):
    client, stub = ecs
    stub.add_response(
        "describe_task_definition",
        {
            "taskDefinition": {
                "containerDefinitions": [{"name": "pipeline", "image": "b"}],
                "executionRoleArn": "arn:x",
            }
        },
    )
    stub.add_response(
        "register_task_definition",
        {"taskDefinition": {"taskDefinitionArn": "arn:td:1"}},
        {
            "family": "pipeyard-acme-sales-dashboard",
            "containerDefinitions": [
                {
                    "name": "pipeline",
                    "image": "repo/dash:1",
                    "portMappings": [{"containerPort": 8080, "protocol": "tcp"}],
                }
            ],
            "cpu": ANY, "memory": ANY, "networkMode": ANY, "requiresCompatibilities": ANY,
            "executionRoleArn": ANY, "volumes": ANY, "runtimePlatform": ANY, "tags": ANY,
        },
    )  # fmt: skip
    stub.add_response(
        "run_task",
        {"tasks": [{"taskArn": TASK}], "failures": []},
        {
            "cluster": ANY, "taskDefinition": ANY, "launchType": ANY, "overrides": ANY,
            "tags": ANY,
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": ["subnet-a", "subnet-b"],
                    "securityGroups": ["sg-reports"],
                    "assignPublicIp": "ENABLED",
                }
            },
        },
    )  # fmt: skip
    job = Job("sales-dashboard", "repo/dash:1", 256, 512, 1800, port=8080)
    assert FargateRunner(AWS, client).start(job, "acme", "i1", {"PORT": "8080"}) == TASK


def test_fargate_report_address_and_stop(ecs):
    client, stub = ecs
    runner = FargateRunner(AWS, client)
    expected = {"cluster": "cluster", "tasks": [TASK]}
    stub.add_response("describe_tasks", {"tasks": [{"attachments": []}]}, expected)
    assert runner.address(TASK, 8080) is None  # no network interface yet
    eni = {"details": [{"name": "privateIPv4Address", "value": "10.0.1.23"}]}
    stub.add_response("describe_tasks", {"tasks": [{"attachments": [eni]}]}, expected)
    assert runner.address(TASK, 8080) == "10.0.1.23:8080"
    stub.add_response("stop_task", {}, {"cluster": "cluster", "task": TASK, "reason": ANY})
    runner.stop(TASK)
    stub.add_client_error("stop_task", "InvalidParameterException")
    runner.stop(TASK)  # already gone: not an error


def test_org_credentials_assume_the_run_role_with_an_org_tag(monkeypatch):
    calls = []

    class FakeSts:
        def assume_role(self, **kwargs):
            calls.append(kwargs)
            n = len(calls)
            return {
                "Credentials": {
                    "AccessKeyId": f"AK{n}",
                    "SecretAccessKey": "s",
                    "SessionToken": "t",
                }
            }

    monkeypatch.setattr(storage.boto3, "client", lambda service, **kw: FakeSts())
    monkeypatch.setattr(storage, "_cache", {})
    first = storage.org_credentials(AWS, "acme")
    assert calls[0]["RoleArn"] == AWS.run_role_arn
    assert calls[0]["Tags"] == [{"Key": "org_id", "Value": "acme"}]
    assert first.session_token == "t"
    assert storage.org_credentials(AWS, "acme") is first  # cached for queries
    assert (
        storage.org_credentials(AWS, "acme", fresh=True).access_key == "AK2"
    )  # full hour for runs
    assert storage.org_credentials(AWS, "other").access_key == "AK3"
    assert calls[2]["Tags"] == [{"Key": "org_id", "Value": "other"}]
    assert "Policy" not in calls[0]

    # Report apps get credentials that can read the org's prefix but not write to it.
    storage.org_credentials(AWS, "acme", read_only=True)
    policy = json.loads(calls[3]["Policy"])["Statement"][0]
    assert policy["Action"] == ["s3:GetObject", "s3:ListBucket"]
    assert calls[3]["Tags"] == [{"Key": "org_id", "Value": "acme"}]
    assert storage.org_credentials(AWS, "acme").access_key == "AK2"  # never cached


@pytest.fixture(scope="module")
def cognito():
    """An Auth in aws mode whose JWKS lookup returns a key we hold."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    auth = Auth(AWS)

    class Jwks:
        def get_signing_key_from_jwt(self, token):
            return type("Key", (), {"key": key.public_key()})

    auth._jwks = Jwks()

    def make(**overrides) -> str:
        claims = {
            "sub": "u",
            "iss": auth.issuer,
            "aud": "client-123",
            "token_use": "id",
            "custom:org_id": "acme",
            "exp": int(time.time()) + 60,
        } | overrides
        signer = claims.pop("signer", key)
        return jwt.encode(claims, signer, algorithm="RS256")

    return auth, make


def test_cognito_id_token_gives_the_org(cognito):
    auth, make = cognito
    assert auth.org_id(make()) == "acme"


@pytest.mark.parametrize(
    "overrides",
    [
        {"aud": "another-client"},
        {"iss": "https://cognito-idp.eu-west-2.amazonaws.com/another_pool"},
        {"token_use": "access"},
        {"exp": int(time.time()) - 5},
        {"custom:org_id": "Not Valid"},
        {"signer": rsa.generate_private_key(public_exponent=65537, key_size=2048)},
    ],
)
def test_cognito_rejects(cognito, overrides):
    auth, make = cognito
    with pytest.raises(AuthError):
        auth.org_id(make(**overrides))


def test_aws_mode_has_no_dev_tokens(cognito):
    auth, _ = cognito
    with pytest.raises(AuthError):
        auth.dev_token("acme")
    hs256 = jwt.encode(
        {"custom:org_id": "acme", "exp": int(time.time()) + 60}, AWS.dev_secret, algorithm="HS256"
    )
    with pytest.raises(AuthError):
        auth.org_id(hs256)


def test_aws_mode_login_form(cognito, monkeypatch):
    from fastapi.testclient import TestClient

    from pipeyard import app as app_module

    auth, make = cognito
    monkeypatch.setattr(app_module, "Auth", lambda settings: auth)
    monkeypatch.setattr(app_module.query, "install_extensions", lambda: None)
    client = TestClient(app_module.create_app(AWS, runner=object()), follow_redirects=False)

    assert client.post("/dev/token").status_code == 404
    assert client.get("/pipelines").headers["location"] == "/login?next=/pipelines"
    assert "Sign in" in client.get("/login").text  # a form, not an automatic sign-in

    def login(username, password):
        if password != "right":
            raise AuthError("Incorrect username or password")
        return make()

    monkeypatch.setattr(auth, "cognito_login", login)
    bad = client.post("/login", content="username=a%40b.c&password=wrong")
    assert bad.status_code == 401 and "Incorrect username or password" in bad.text
    good = client.post("/login?next=/datasets", content="username=a%40b.c&password=right")
    assert good.status_code == 303 and good.headers["location"] == "/datasets"
    assert "HttpOnly" in good.headers["set-cookie"]
    evil = client.post("/login?next=//evil.example", content="username=a&password=right")
    assert evil.headers["location"] == "/"


@pytest.fixture(scope="module")
def template():
    sys.path.insert(0, str(Path(__file__).parent.parent / "infra"))
    import aws_cdk as cdk
    from aws_cdk.assertions import Template
    from stack import PipeyardStack

    return Template.from_stack(PipeyardStack(cdk.App(), "t", org_id="demo")).to_json()


def _resources(template, type_):
    return [r["Properties"] for r in template["Resources"].values() if r["Type"] == type_]


def test_stack_s3_access_is_abac_on_the_org_tag(template):
    statements = [
        s
        for p in _resources(template, "AWS::IAM::Policy")
        for s in p["PolicyDocument"]["Statement"]
    ]
    s3_statements = [
        s
        for s in statements
        if any(
            a.startswith("s3:")
            for a in ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])
        )
    ]
    assert len(s3_statements) == 2  # only the run role touches S3
    for statement in s3_statements:
        assert "orgs/${aws:PrincipalTag/org_id}/*" in str(statement)
    # the run role can only be assumed by the control plane, and only with session tags
    trust = [
        r["AssumeRolePolicyDocument"]["Statement"] for r in _resources(template, "AWS::IAM::Role")
    ]
    tagged = [s for statements in trust for s in statements if "sts:TagSession" in s["Action"]]
    assert len(tagged) == 1 and "ControlPlaneTaskDefTaskRole" in str(tagged[0]["Principal"])


def test_stack_run_template_is_locked_down(template):
    run = next(
        t
        for t in _resources(template, "AWS::ECS::TaskDefinition")
        if {c["Name"] for c in t["ContainerDefinitions"]} == {"pipeline", "timeout"}
    )
    for container in run["ContainerDefinitions"]:
        assert container["ReadonlyRootFilesystem"] is True
        assert container["Essential"] is True
    pipeline = next(c for c in run["ContainerDefinitions"] if c["Name"] == "pipeline")
    assert pipeline["MountPoints"] == [
        {"ContainerPath": "/tmp", "ReadOnly": False, "SourceVolume": "tmp"}
    ]
    timeout = next(c for c in run["ContainerDefinitions"] if c["Name"] == "timeout")
    assert "exit 124" in timeout["Command"][-1]


def test_stack_report_apps_accept_only_the_control_plane(template):
    ingress = _resources(template, "AWS::EC2::SecurityGroupIngress")
    (rule,) = [r for r in ingress if "ReportSecurityGroup" in str(r["GroupId"])]
    assert (rule["FromPort"], rule["ToPort"]) == (8080, 8080)
    assert "ControlPlaneServiceSecurityGroup" in str(rule["SourceSecurityGroupId"])
    run_rules = [r for r in ingress if "RunSecurityGroup" in str(r["GroupId"])]
    assert run_rules == []  # pipeline runs still accept nothing


def test_stack_openlineage_url_is_optional(template):
    def env(tpl) -> dict:
        control = next(
            t for t in _resources(tpl, "AWS::ECS::TaskDefinition")
            if {c["Name"] for c in t["ContainerDefinitions"]} == {"web"}
        )  # fmt: skip
        return {e["Name"]: e["Value"] for e in control["ContainerDefinitions"][0]["Environment"]}

    assert "OPENLINEAGE_URL" not in env(template)

    import aws_cdk as cdk
    from aws_cdk.assertions import Template
    from stack import PipeyardStack

    stack = PipeyardStack(cdk.App(), "t2", org_id="demo", openlineage_url="https://lineage.example")
    assert env(Template.from_stack(stack).to_json())["OPENLINEAGE_URL"] == "https://lineage.example"


def _stack(**kwargs) -> dict:
    import aws_cdk as cdk
    from aws_cdk.assertions import Template
    from stack import PipeyardStack

    return Template.from_stack(PipeyardStack(cdk.App(), "s", org_id="demo", **kwargs)).to_json()


def _control_plane_env(tpl) -> dict:
    control = next(
        t for t in _resources(tpl, "AWS::ECS::TaskDefinition")
        if {c["Name"] for c in t["ContainerDefinitions"]} == {"web"}
    )  # fmt: skip
    return {e["Name"]: e["Value"] for e in control["ContainerDefinitions"][0]["Environment"]}


def test_stack_sign_in_defaults_to_the_password_form(template):
    """No domain, so nowhere for a provider to redirect back to: Cognito password form."""
    env = _control_plane_env(template)
    assert "PIPEYARD_COGNITO_POOL_ID" in env and not any("OIDC" in k for k in env)
    (client,) = _resources(template, "AWS::Cognito::UserPoolClient")
    assert client.get("AllowedOAuthFlowsUserPoolClient") is False  # hosted sign-in is off
    assert "CallbackURLs" not in client
    assert _resources(template, "AWS::Cognito::UserPoolDomain") == []


def test_stack_sign_in_uses_cognitos_hosted_page_when_there_is_a_domain(template):
    tpl = _stack(
        certificate_arn="arn:aws:acm:eu-west-2:111111111111:certificate/abc",
        domain_name="pipeyard.example.com",
    )
    env = _control_plane_env(tpl)
    assert env["PIPEYARD_PUBLIC_URL"] == "https://pipeyard.example.com"
    assert env["PIPEYARD_OIDC_ORG_CLAIM"] == "custom:org_id"
    assert "ProviderURL" in str(env["PIPEYARD_OIDC_ISSUER"])  # the pool's own issuer address
    assert env["PIPEYARD_OIDC_CLIENT_ID"] == env["PIPEYARD_COGNITO_CLIENT_ID"]
    (client,) = _resources(tpl, "AWS::Cognito::UserPoolClient")
    assert client["AllowedOAuthFlows"] == ["code"]  # no implicit flow
    assert client["CallbackURLs"] == ["https://pipeyard.example.com/auth/callback"]
    assert set(client["AllowedOAuthScopes"]) == {"openid", "email", "profile"}
    assert "custom:org_id" not in client["WriteAttributes"]
    assert len(_resources(tpl, "AWS::Cognito::UserPoolDomain")) == 1
    assert tpl["Outputs"]["Url"]["Value"] == "https://pipeyard.example.com"


def test_stack_sign_in_with_your_own_provider_creates_no_user_pool():
    tpl = _stack(
        oidc_issuer="https://idp.example.com/realms/acme", oidc_client_id="pipeyard",
        oidc_org_claim="tenant",
    )  # fmt: skip
    env = _control_plane_env(tpl)
    assert env["PIPEYARD_OIDC_ISSUER"] == "https://idp.example.com/realms/acme"
    assert (env["PIPEYARD_OIDC_CLIENT_ID"], env["PIPEYARD_OIDC_ORG_CLAIM"]) == (
        "pipeyard",
        "tenant",
    )
    assert not any("COGNITO" in k for k in env)
    assert not [r for r in tpl["Resources"].values() if r["Type"].startswith("AWS::Cognito")]
    assert "UserPoolId" not in tpl["Outputs"]
    with pytest.raises(ValueError, match="oidc_issuer needs oidc_client_id"):
        _stack(oidc_issuer="https://idp.example.com")


def test_stack_shape(template):
    types = [r["Type"] for r in template["Resources"].values()]
    assert "AWS::EC2::NatGateway" not in types  # the documented v0 tradeoff
    assert "AWS::Lambda::Function" not in types  # nothing beyond the listed services
    assert types.count("AWS::S3::Bucket") == 1 and types.count("AWS::ECR::Repository") == 1
    (bucket,) = _resources(template, "AWS::S3::Bucket")
    assert all(bucket["PublicAccessBlockConfiguration"].values())
    (pool,) = _resources(template, "AWS::Cognito::UserPool")
    assert pool["AdminCreateUserConfig"]["AllowAdminCreateUserOnly"] is True
    (org_attr,) = [a for a in pool["Schema"] if a["Name"] == "org_id"]
    assert org_attr["Mutable"] is False
    (client,) = _resources(template, "AWS::Cognito::UserPoolClient")
    assert "custom:org_id" in client["ReadAttributes"]
    assert "custom:org_id" not in client["WriteAttributes"]
    assert set(template["Outputs"]) >= {
        "Url",
        "BucketName",
        "EcrRepositoryUri",
        "UserPoolId",
        "UserPoolClientId",
        "OrgId",
    }
