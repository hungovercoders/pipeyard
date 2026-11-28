# Your first pipeline

In about twenty minutes you will build a container that produces data, register it with
pipeyard and run it. Then you will build a second container, a small dashboard, and let
pipeyard serve it. Finally you will break things on purpose, because knowing what failure
looks like is half of trusting a tool.

The example is a coffee shop: each run generates 500 orders.

**Before you start**, have pipeyard running locally:

```sh
task up
```

and <http://localhost:8000> open in your browser. (New here? The
[README quickstart](../README.md#try-it-in-5-minutes) gets you to this point.)

## 1. Write the code

Make a new folder anywhere, called `coffee-orders`. You will put three files in it now and a
sub-folder for the dashboard later.

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

The third file is `pipeline.yaml`. It tells pipeyard which image to run and what dataset it
produces:

```yaml
name: coffee-orders
image: coffee-orders:latest
outputs:
  - dataset: orders
```

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

You land on the pipeline's page. If you had made a mistake, say `cpu: 300` or a misspelt
field, you would instead see a list of what to fix, with your text still in the box.

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

- Open **Datasets**. `orders` is listed with 500 rows and the schema your manifest declared.
- Press **Trigger run** again. Datasets now shows 2 runs and 1,000 rows. Every successful
  run *adds* to the dataset; nothing is overwritten.

You have data. Time to look at it.

## 6. Add a report

A report in pipeyard is also a container: a small web app. It can be built with anything
that serves HTTP, such as Streamlit, Dash, Shiny or a hand-written Flask app. pipeyard
starts it when someone opens the report, puts it behind the same sign-in as everything
else, and stops it again when nobody is using it.

Make a sub-folder `coffee-orders/report` with two files. First the app, `app.py`:

```python
import os
from pathlib import Path
from urllib.parse import urlparse

import boto3
import duckdb
import streamlit as st


@st.cache_data(ttl=60)
def load():
    # pipeyard tells the report where its dataset lives.
    uri = urlparse(os.environ["INPUT_URI"])  # s3://bucket/orgs/demo/datasets/orders/
    bucket, prefix = uri.netloc, uri.path.lstrip("/")
    s3 = boto3.client("s3")
    keys = [
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    # Only read runs that finished: a run= folder with a manifest in it.
    finished = {key.rsplit("/", 1)[0] for key in keys if key.endswith("/_manifest.json")}
    data = Path("/tmp/data")
    data.mkdir(exist_ok=True)
    for i, key in enumerate(k for k in keys if k.endswith(".parquet")):
        if key.rsplit("/", 1)[0] in finished:
            s3.download_file(bucket, key, str(data / f"{i}.parquet"))
    return duckdb.sql(f"select * from read_parquet('{data}/*.parquet')").df()


st.title("Coffee orders")
orders = load()
drink = st.selectbox("Drink", ["all", *sorted(orders["drink"].unique())])
if drink != "all":
    orders = orders[orders["drink"] == drink]

st.metric("Orders", f"{len(orders):,}")
st.subheader("Revenue per day")
st.bar_chart(orders.set_index("ordered_at")["price"].resample("D").sum())
```

Then its `Dockerfile`:

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir streamlit duckdb boto3
COPY app.py /app/app.py
ENV HOME=/tmp
USER 1000
ENTRYPOINT ["sh", "-c", "exec streamlit run /app/app.py --server.port \"$PORT\" --server.address 0.0.0.0 --server.baseUrlPath \"$BASE_PATH\" --server.headless true --server.enableCORS false --server.enableXsrfProtection false"]
```

The last line is the whole of the report contract: **listen on `$PORT`, and serve under
`$BASE_PATH`**. pipeyard sets both. Every web framework has a pair of settings for this; the
line above is Streamlit's. (`HOME=/tmp` is there because the container's filesystem is
read-only apart from `/tmp`, and Streamlit keeps a few files in its home folder.)

Build it, from inside `coffee-orders`:

```sh
docker build -t coffee-orders-report:latest report
```

Now tell pipeyard about it. On the pipeline page press **Edit YAML**, add a `reports`
section so the whole file reads as below, and save:

```yaml
name: coffee-orders
image: coffee-orders:latest
outputs:
  - dataset: orders
reports:
  - name: Coffee dashboard
    image: coffee-orders-report:latest
    dataset: orders
```

**Coffee dashboard** appears under Reports. Click it. Opening a report wakes its container,
and after a few seconds the dashboard appears inside the page. Pick a drink from the dropdown and watch
the numbers change: that is your container, live, with pipeyard passing your browser's
requests through to it.

Go back to the pipeline page and look at its **Lineage** section: `coffee-orders` writes
`orders`, and **Coffee dashboard** reads it. pipeyard worked that out from your YAML, and
it is how you will later see what a change to a pipeline would affect.

A few things to notice:

- **You did not rerun the pipeline.** Reports read the data that is already there.
- **It scales to zero.** Close the tab, wait five minutes, and the container is gone
  (`docker ps` will show you). Open the report again and it starts again. The five minutes
  is `idle_timeout_seconds`; an abandoned dashboard costs nothing.
- **It is private.** The app is only reachable through pipeyard, by signed-in members of
  your org. The container itself never receives your sign-in token.

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

Now check **Datasets**: the row count has not moved. A run that does not finish properly
never reaches your datasets, and pipeyard deletes the files it left behind, so your
dashboard never sees them either. That is what the manifest is for, and why it is written
last.

Put the `put_object` call back and rebuild, and you are in working order again.

**Break the report.** Change `image:` under `reports` to something that does not exist,
such as `coffee-orders-report:nope`, save, and open the report. Instead of a dashboard you
get the reason it could not start. Change it back and it works again.

## 8. Make a promise about the data

Your dashboard assumes `orders` has a `drink` and a `price`. Nothing guarantees that yet.
A data contract does.

Open **Datasets**, click **orders**, and press **Add a data contract**. pipeyard has
drafted one from the columns your runs produced, in the
[Open Data Contract Standard](data-contracts.md). Change `required: false` to
`required: true` on `order_id` and `price`, and press **Save contract**. The dataset page
now shows the contract, and **Trigger run** on the pipeline still succeeds: your pipeline
keeps its promise.

Now break the promise. In `generate.py`, change `as price` to `as cost`, rebuild with
`docker build -t coffee-orders:latest .`, and trigger a run. It fails:

```
data contract 'orders' version 1.0.0 is not met:
- column 'price' is missing
```

and the dashboard, which would have crashed on the missing column, carries on showing the
good data. Change `cost` back to `price` and rebuild.

Contracts can also make promises about values, not only columns: "no price above 20",
"every drink is one we sell", "at least one row". The draft you saved already contains the
last of those. [Data contracts](data-contracts.md#quality-rules) lists what you can ask for.

## What you just learned

- A pipeline is a container plus a `pipeline.yaml`. The container's whole job is: write
  Parquet to `OUTPUT_URI`, write `_manifest.json`, exit 0.
- Runs append to a dataset. Failed runs explain themselves and leave nothing behind.
- A report is a second container: any web app that listens on `$PORT` under `$BASE_PATH`
  and reads the dataset at `INPUT_URI`.
- pipeyard starts a report when it is opened, keeps it behind sign-in, and stops it when
  nobody is using it. Each page shows the lineage from pipeline to dataset to report.
- A data contract turns "the columns I hope are there" into something every run must meet.

## Where next

- **The rules in full**: [the container contract](container-contract.md) lists every
  field, variable and error message. Two worth knowing now: the container's filesystem is
  read-only apart from `/tmp`, and a run is stopped after `timeout_seconds` (10 minutes by
  default). Report containers have the same read-only filesystem.
- **Reading another dataset**: declare `inputs` in `pipeline.yaml` and your container
  receives an `INPUT_URI` to read from.
- **Running it in AWS**: push both images to the stack's ECR repository and use those URIs
  as `image`. See [deploying to AWS](deploy-aws.md).
- **What happens behind the button**: [how it works](how-it-works.md).
