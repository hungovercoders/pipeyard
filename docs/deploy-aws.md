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
task push-example                 # build the example images and push them to ECR
export PIPEYARD_USERNAME=you@example.com PIPEYARD_PASSWORD='Choose-a-strong-1!'
task demo-aws
```

Open the `Url` from `task outputs` and sign in.

To also send [lineage](lineage.md) events to an OpenLineage server, add
`-c openlineage_url=https://...` to the deploy command.

## Signing in

The stack can sign people in three ways. Which one you get depends on what you pass to
`task deploy`. [Signing in](sign-in.md) explains the mechanics.

| You pass | You get |
|---|---|
| nothing | A Cognito user pool and pipeyard's own password form, over plain HTTP. For a first look only. |
| `-c certificate_arn=... -c domain_name=pipeyard.example.com` | HTTPS, and sign-in through Cognito's hosted page using OpenID Connect. pipeyard never sees a password. |
| `-c oidc_issuer=... -c oidc_client_id=...` (with the certificate and domain) | Sign-in through your own provider (Okta, Entra ID, Keycloak...). No Cognito user pool is created. Add `-c oidc_org_claim=...` if your org claim is not called `org_id`. |

For either of the last two:

1. Create an ACM certificate for a domain you control, in the region you deploy to.
2. Deploy with `-c certificate_arn=<its ARN> -c domain_name=<the domain>`.
3. Point the domain's DNS at the `LoadBalancerDnsName` output.
4. With your own provider, register `https://<the domain>/auth/callback` as the redirect
   address there.

`task create-user` and `task demo-aws` use the Cognito user pool, so they apply to the
first two options only. With your own provider, users and their org are managed there.

## Running your own pipeline in AWS

It is the same pipeline as locally, with two differences: the images must live somewhere
Fargate can pull them, and they must be built for `linux/amd64`.

```sh
REPO=<EcrRepositoryUri from task outputs>
aws ecr get-login-password | docker login --username AWS --password-stdin ${REPO%%/*}
docker build --platform linux/amd64 -t $REPO:coffee-orders .
docker push $REPO:coffee-orders
docker build --platform linux/amd64 -t $REPO:coffee-orders-report report
docker push $REPO:coffee-orders-report
```

Set the two `image:` lines in your `pipeline.yaml` to `<EcrRepositoryUri>:coffee-orders`
and `<EcrRepositoryUri>:coffee-orders-report`, and register it in the UI as usual. In AWS
a report container is told to listen on port 8080, and takes a little longer to start than
locally, typically under a minute, while Fargate provisions the task. Container output goes to the CloudWatch log group named by the
`RunLogGroup` output.

## What gets created

| Resource | Notes |
|---|---|
| S3 bucket | Private, encrypted, TLS only. Kept if you destroy the stack. |
| ECR repository | For your pipeline images. Also kept on destroy. |
| ECS cluster | Fargate only; nothing runs when no pipeline is running, except pipeyard itself |
| pipeyard service | One Fargate task behind an Application Load Balancer |
| Run task template | The security settings every run inherits: read-only filesystem, `/tmp` scratch volume, logging, and the timeout sidecar. pipeyard copies it per pipeline, swapping in your image, cpu and memory. |
| Cognito user pool | Sign-in, unless you bring your own provider. Each user has an `org_id` that cannot be changed after creation, by the user or by the app. |
| Run role | The one IAM role allowed to touch data, and only under `orgs/{org_id}/` |
| Two security groups | Pipeline runs accept no connections at all. Report apps accept connections from pipeyard only, on port 8080. |
| VPC | Two public subnets, no NAT gateway |

## Tradeoffs in v0, and how to undo them

v0 optimises for a cheap, simple first deployment. Each shortcut below is deliberate.

**Public subnets with open egress.** Tasks need to reach ECR, S3, STS and CloudWatch. A
NAT gateway or VPC endpoints would cost more than everything else in an idle stack, so
tasks get public IPs instead. Nothing can connect *in* to a pipeline container, and only
pipeyard can connect to a report container, but both can call *out* to anywhere on the
internet. To tighten: private subnets, VPC
endpoints, and an egress allow-list.

**Plain HTTP and a password form unless you bring a domain.** A bare load balancer has no
certificate, so by default pipeyard is served over HTTP and signs people in with its own
form, which sends the password unencrypted. That is for a first look only. See
[Signing in](#signing-in) below for the two proper options.
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

**Reports scale to zero, with two rough edges.** A report is a Fargate task that exists
only while it is in use: pipeyard stops it after `idle_timeout_seconds` (5 minutes by
default) without a request or an open WebSocket, so an unused report costs nothing. The
rough edges: waking one takes as long as Fargate takes to start a task, typically under a
minute; and a report in continuous use is still restarted every 50 minutes, because its
credentials expire. The traffic between pipeyard and a report task stays inside the VPC
but is plain HTTP.

**One task definition revision per run or report start.** Harmless, but they accumulate in
ECS.

**A single pipeyard task.** A redeploy causes a brief gap in the UI. Runs in flight are
not lost: their records are in S3 and are settled on the next read.

## Tear down

```sh
task destroy
```

The S3 bucket and ECR repository are kept on purpose, so your data and images survive.
Delete them by hand if you want them gone.
