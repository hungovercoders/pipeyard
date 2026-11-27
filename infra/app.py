"""CDK entry point. Context: -c org_id=acme  -c certificate_arn=arn:aws:acm:..."""

import aws_cdk as cdk
from stack import PipeyardStack

app = cdk.App()
PipeyardStack(
    app,
    "pipeyard",
    org_id=app.node.try_get_context("org_id") or "demo",
    certificate_arn=app.node.try_get_context("certificate_arn"),
)
app.synth()
