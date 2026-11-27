# Your first pipeline

In about ten minutes you will build a container that produces data, register it with
pipeyard, run it, and see the result as a chart. Then you will break it on purpose, because
knowing what failure looks like is half of trusting a tool.

The example is a coffee shop: each run generates 500 orders.

**Before you start**, have pipeyard running locally:

```sh
task up
```

and <http://localhost:8000> open in your browser. (New here? The
[README quickstart](../README.md#try-it-in-5-minutes) gets you to this point.)

## 1. Write the code

Make a new folder anywhere, called `coffee-orders`, with three files in it.

The first is the program, `generate.py`:

```python
import json
import os
from datetime import UTC, datetime
from urllib.parse import urlparse

import boto3
import duckdb

# pipeyard tells the container where to write.
run_id = os.environ["RUN_ID"]
output = urlparse(os.environ["OUTPUT_URI"])  # s3://bucket/orgs/demo/datasets/orders/run=.../
bucket, prefix = output.netloc, output.path.lstrip("/")

# 1. Produce some data as a Parquet file. Only /tmp is writable.
con = duckdb.connect()
con.execute("""
    copy (
        select
            i as order_id,
            now()::timestamp - to_minutes((random() * 7 * 24 * 60)::bigint) as ordered_at,
            ['espresso', 'flat white', 'latte', 'filter', 'mocha']
                [1 + floor(random() * 5)::int] as drink,
            round(2.5 + random() * 3, 2) as price
        from range(500) t(i)
    ) to '/tmp/orders.parquet' (format parquet)
""")
columns = con.execute("describe select * from '/tmp/orders.parquet'").fetchall()
row_count = con.execute("select count(*) from '/tmp/orders.parquet'").fetchone()[0]

# 2. Upload it. boto3 finds the credentials and endpoint in the environment.
s3 = boto3.client("s3")
s3.upload_file("/tmp/orders.parquet", bucket, prefix + "orders.parquet")

# 3. Last of all, write the manifest. It tells pipeyard the run is complete.
manifest = {
    "dataset": "orders",
    "run_id": run_id,
    "row_count": row_count,
    "schema": [{"name": name, "type": type_} for name, type_, *_ in columns],
    "created_at": datetime.now(UTC).isoformat(),
}
s3.put_object(Bucket=bucket, Key=prefix + "_manifest.json", Body=json.dumps(manifest))
print(f"wrote {row_count} orders")
```

Notice what is *not* in there: no pipeyard library, no decorators, no config file. The
program reads two environment variables and writes two files. This example happens to use
Python and DuckDB, but any language that can write Parquet to S3 works the same way.

The second file is the `Dockerfile`:

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb boto3
COPY generate.py /app/generate.py
USER 1000
ENTRYPOINT ["python", "/app/generate.py"]
```

## 2. Describe it

The third file is `pipeline.yaml`. It tells pipeyard which image to run, what dataset it
produces, and what you want to see:

```yaml
name: coffee-orders
image: coffee-orders:latest
outputs:
  - dataset: orders
reports:
  - name: Orders by drink
    dataset: orders
    sql: |
      select drink, count(*) as orders
      from {{dataset}}
      group by drink
      order by orders desc
    chart: { type: bar, x: drink, y: orders }
```

`{{dataset}}` stands for the report's dataset, here `orders`. The SQL is ordinary DuckDB
SQL and must be a single `SELECT`.

## 3. Build the image

From inside the `coffee-orders` folder:

```sh
docker build -t coffee-orders:latest .
```

Locally, pipeyard runs images straight from your machine's Docker, so there is nothing to
push.

## 4. Register it

In the UI, go to **Pipelines** and press **Register pipeline**. Replace the starter text
with your `pipeline.yaml` and press **Save pipeline**.

You land on the pipeline's page. If you had made a mistake, say `cpu: 300` or a report that
tries to `DROP TABLE`, you would instead see a list of what to fix, with your text still in
the box.

<details>
<summary>Prefer the command line?</summary>

```sh
TOKEN=$(curl -s -X POST localhost:8000/dev/token | python3 -c "import sys, json; print(json.load(sys.stdin)['token'])")
curl -X POST localhost:8000/api/pipelines \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/yaml" \
  --data-binary @pipeline.yaml
```

</details>

## 5. Run it

Press **Trigger run**. A row appears with the status **running**, and a few seconds later
it turns to **succeeded** with `500` in the Rows column. The page updates by itself.

Now look around:

- Click **Orders by drink** under Reports. There is your bar chart, and under it the table
  and the SQL that produced it.
- Open **Datasets**. `orders` is listed with the schema your manifest declared.
- Press **Trigger run** again, then reload the report. The total doubles to 1,000 orders. Every
  successful run *adds* to the dataset; nothing is overwritten.

## 6. Add a report without touching the container

Reports are just SQL over data you already have, so adding one needs no rebuild and no new
run. On the pipeline page press **Edit YAML**, add this to the end of `reports`, and save:

```yaml
  - name: Revenue per day
    dataset: orders
    sql: |
      select date_trunc('day', ordered_at) as day, round(sum(price), 2) as revenue
      from {{dataset}}
      group by day
      order by day
    chart: { type: line, x: day, y: revenue }
```

**Revenue per day** appears under Reports straight away, charting the runs you already did.

Want only the most recent run instead of all of them? Every row carries a `run` column:

```sql
select drink, count(*) as orders
from {{dataset}}
where run = (select max(run) from {{dataset}})
group by drink
```

## 7. Break it on purpose

A pipeline tool earns its keep when things go wrong. Try two failures.

**Crash.** Add `raise SystemExit("the espresso machine is on fire")` as the first line of
`generate.py` after the imports, rebuild with `docker build -t coffee-orders:latest .`, and
trigger a run. It ends as **failed**, and the row shows the exit code and your message, so
you do not need to go hunting for logs.

**Forget the manifest.** Remove that line again, delete the `s3.put_object(...)` call at
the bottom instead, rebuild, and trigger a run. The Parquet file is uploaded, but the run
fails with:

```
container exited 0 but wrote no _manifest.json
```

Now check the report: its numbers have not moved. A run that does not finish properly never
reaches your datasets, even when it leaves files behind. That is what the manifest is for,
and why it is written last.

Put the `put_object` call back and rebuild, and you are in working order again.

## What you just learned

- A pipeline is a container plus a `pipeline.yaml`. The container's whole job is: write
  Parquet to `OUTPUT_URI`, write `_manifest.json`, exit 0.
- Runs append. Reports are SQL over every successful run.
- Reports can be added and changed without rerunning anything.
- Failed runs explain themselves and never pollute your data.

## Where next

- **The rules in full**: [the container contract](container-contract.md) lists every
  field, variable and error message. Two worth knowing now: the container's filesystem is
  read-only apart from `/tmp`, and a run is stopped after `timeout_seconds` (10 minutes by
  default).
- **Reading another dataset**: declare `inputs` in `pipeline.yaml` and your container
  receives an `INPUT_URI` to read from.
- **Running it in AWS**: push the image to the stack's ECR repository and use that URI as
  `image`. See [deploying to AWS](deploy-aws.md).
- **What happens behind the button**: [how it works](how-it-works.md).
