# Deploying to AWS

One command deploys pipeyard into your own AWS account as a single CloudFormation stack.

> **Status: not yet proven on a real account.** The stack synthesizes, its template is
> checked by tests, and the AWS code paths are tested against stubbed clients, but nobody
> has run `task deploy` for real yet. Treat the first deploy as a shakedown and expect to
> fix a detail or two.

## Before you start

- AWS credentials in your shell that can create IAM roles, a VPC, ECS, S3, ECR, Cognito
  and a load balancer
- Docker running, because CDK builds pipeyard's image during the deploy
- `mise install` done, which provides the CDK and AWS command-line tools

## Deploy

```sh
task bootstrap                    # once per account and region: prepares CDK
task deploy -- -c org_id=acme     # 5 to 10 minutes. org_id defaults to "demo"
task outputs                      # the URL, bucket, ECR repository and user pool
```

Create yourself a user. There is no self sign-up, and the org is fixed when the user is
created:

```sh
task create-user EMAIL=you@example.com PASSWORD='Choose-a-strong-1!'
```

Then run the same demo as on your laptop, this time on Fargate:

```sh
task push-example                 # build the example image and push it to ECR
export PIPEYARD_USERNAME=you@example.com PIPEYARD_PASSWORD='Choose-a-strong-1!'
task demo-aws
```

Open the `Url` from `task outputs` and sign in.

## Running your own pipeline in AWS

It is the same pipeline as locally, with two differences: the image must live somewhere
Fargate can pull it, and it must be built for `linux/amd64`.

```sh
REPO=<EcrRepositoryUri from task outputs>
aws ecr get-login-password | docker login --username AWS --password-stdin ${REPO%%/*}
docker build --platform linux/amd64 -t $REPO:coffee-orders .
docker push $REPO:coffee-orders
```

Set `image:` in your `pipeline.yaml` to `<EcrRepositoryUri>:coffee-orders` and register it
in the UI as usual. Container output goes to the CloudWatch log group named by the
`RunLogGroup` output.

## What gets created

| Resource | Notes |
|---|---|
| S3 bucket | Private, encrypted, TLS only. Kept if you destroy the stack. |
| ECR repository | For your pipeline images. Also kept on destroy. |
| ECS cluster | Fargate only; nothing runs when no pipeline is running, except pipeyard itself |
| pipeyard service | One Fargate task behind an Application Load Balancer |
| Run task template | The security settings every run inherits: read-only filesystem, `/tmp` scratch volume, logging, and the timeout sidecar. pipeyard copies it per pipeline, swapping in your image, cpu and memory. |
| Cognito user pool | Sign-in. Each user has an `org_id` that cannot be changed after creation, by the user or by the app. |
| Run role | The one IAM role allowed to touch data, and only under `orgs/{org_id}/` |
| VPC | Two public subnets, no NAT gateway |

## Tradeoffs in v0, and how to undo them

v0 optimises for a cheap, simple first deployment. Each shortcut below is deliberate.

**Public subnets with open egress.** Tasks need to reach ECR, S3, STS and CloudWatch. A
NAT gateway or VPC endpoints would cost more than everything else in an idle stack, so
tasks get public IPs instead. Nothing can connect *in* to a run container, but a pipeline
container can call *out* to anywhere on the internet. To tighten: private subnets, VPC
endpoints, and an egress allow-list.

**Plain HTTP unless you bring a certificate.** A bare load balancer has no certificate, so
by default the sign-in form sends your password unencrypted. For anything beyond a quick
trial, create an ACM certificate for a domain you control, point the domain at the load
balancer, and deploy with:

```sh
task deploy -- -c certificate_arn=arn:aws:acm:...
```

That switches to HTTPS, redirects HTTP, and marks the session cookie `Secure`.

**Run credentials travel as environment variables.** pipeyard assumes the run role with
your org's tag and passes the temporary credentials to the container. They only work for
your org's prefix and expire after an hour, but anyone in the account who can describe ECS
tasks can read them while they are valid. This is also why `timeout_seconds` is capped at
3000. The alternative is one IAM role per org, passed as the task's own role.

**Runs are settled when someone looks.** ECS forgets a stopped task after about an hour.
If nobody has looked at a run by then, pipeyard decides its outcome from its output alone:
a valid manifest plus Parquet means success. In the rare case that a container wrote a
complete, valid manifest and *then* exited non-zero, that run would be recorded as
succeeded. A background poller would close the gap.

**One task definition revision per run.** Harmless, but they accumulate in ECS.

**A single pipeyard task.** A redeploy causes a brief gap in the UI. Runs in flight are
not lost: their records are in S3 and are settled on the next read.

## Tear down

```sh
task destroy
```

The S3 bucket and ECR repository are kept on purpose, so your data and images survive.
Delete them by hand if you want them gone.
