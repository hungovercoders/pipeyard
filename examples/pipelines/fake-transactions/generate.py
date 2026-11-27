"""Example pipeyard pipeline: ~10k fake card transactions over the last 30 days.

Follows docs/container-contract.md: write Parquet under OUTPUT_URI, then _manifest.json
last, then exit 0. Only /tmp is writable; S3 credentials and endpoint come from the
standard AWS_* environment variables, which boto3 picks up on its own.
"""

import json
import os
from datetime import UTC, datetime
from urllib.parse import urlparse

import boto3
import duckdb

ROWS = 10_000
LOCAL = "/tmp/part-0.parquet"  # noqa: S108

run_id = os.environ["RUN_ID"]
output = urlparse(os.environ["OUTPUT_URI"])
bucket, prefix = output.netloc, output.path.lstrip("/")

con = duckdb.connect()
con.execute(f"""
    copy (
        select
            uuid()::varchar as transaction_id,
            now()::timestamp - to_seconds((random() * 30 * 86400)::bigint) as ts,
            'cust-' || lpad((1 + floor(random() * 500))::int::varchar, 4, '0') as customer_id,
            ['groceries', 'transport', 'dining', 'travel', 'utilities', 'shopping']
                [1 + floor(random() * 6)::int] as category,
            round(exp(2.5 + random() * 2.5), 2)::double as amount
        from range({ROWS})
    ) to '{LOCAL}' (format parquet)
""")
schema = [
    {"name": name, "type": type_}
    for name, type_, *_ in con.execute(f"describe select * from '{LOCAL}'").fetchall()
]
row_count = con.execute(f"select count(*) from '{LOCAL}'").fetchone()[0]

s3 = boto3.client("s3")
s3.upload_file(LOCAL, bucket, f"{prefix}part-0.parquet")
manifest = {
    "dataset": "transactions",
    "run_id": run_id,
    "row_count": row_count,
    "schema": schema,
    "created_at": datetime.now(UTC).isoformat(),
}
s3.put_object(Bucket=bucket, Key=f"{prefix}_manifest.json", Body=json.dumps(manifest))
print(f"wrote {row_count} rows to {os.environ['OUTPUT_URI']}")
