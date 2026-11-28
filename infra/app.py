"""CDK entry point.

Context (all optional), passed as -c name=value:
  org_id, certificate_arn, domain_name, oidc_issuer, oidc_client_id, oidc_org_claim,
  openlineage_url. See docs/deploy-aws.md and docs/sign-in.md.
"""

import aws_cdk as cdk
from stack import PipeyardStack

app = cdk.App()
PipeyardStack(
    app,
    "pipeyard",
    org_id=app.node.try_get_context("org_id") or "demo",
    certificate_arn=app.node.try_get_context("certificate_arn"),
    openlineage_url=app.node.try_get_context("openlineage_url"),
    domain_name=app.node.try_get_context("domain_name"),
    oidc_issuer=app.node.try_get_context("oidc_issuer"),
    oidc_client_id=app.node.try_get_context("oidc_client_id"),
    oidc_org_claim=app.node.try_get_context("oidc_org_claim"),
)
app.synth()
