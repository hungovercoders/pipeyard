"""CDK entry point.

Context: -c org_id=acme  -c certificate_arn=arn:aws:acm:...  -c openlineage_url=https://...
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
)
app.synth()
