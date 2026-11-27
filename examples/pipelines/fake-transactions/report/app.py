"""Example pipeyard report: a Streamlit dashboard over the transactions dataset.

A report is just a web app. pipeyard starts this container when someone opens the report,
passes it PORT, BASE_PATH and INPUT_URI, and proxies the signed-in browser to it. See
docs/container-contract.md.
"""

import os
from pathlib import Path
from urllib.parse import urlparse

import boto3
import duckdb
import streamlit as st

DATA = Path("/tmp/data")  # noqa: S108 - the only writable place in the container


@st.cache_data(ttl=60)
def load():
    """Download the Parquet of every committed run (a run= folder that has a manifest)."""
    uri = urlparse(os.environ["INPUT_URI"])
    bucket, prefix = uri.netloc, uri.path.lstrip("/")
    s3 = boto3.client("s3")  # credentials and endpoint come from the environment
    keys = [
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    committed = {key.rsplit("/", 1)[0] for key in keys if key.endswith("/_manifest.json")}
    DATA.mkdir(exist_ok=True)
    for old in DATA.glob("*.parquet"):
        old.unlink()
    wanted = [k for k in keys if k.endswith(".parquet") and k.rsplit("/", 1)[0] in committed]
    for i, key in enumerate(wanted):
        s3.download_file(bucket, key, str(DATA / f"{i}.parquet"))
    if not wanted:
        return None, 0
    return duckdb.sql(f"select * from read_parquet('{DATA}/*.parquet')").df(), len(committed)


st.set_page_config(page_title="Transactions", layout="wide")
st.title("Transactions")
df, runs = load()
if df is None:
    st.info("No data yet. Trigger a run of the pipeline, then reload.")
    st.stop()

categories = sorted(df["category"].unique())
chosen = st.multiselect("Categories", categories, default=categories)
df = df[df["category"].isin(chosen)]

left, middle, right = st.columns(3)
left.metric("Transactions", f"{len(df):,}")
middle.metric("Volume", f"{df['amount'].sum():,.0f}")
right.metric("Runs", runs)

st.subheader("Daily volume")
st.line_chart(df.set_index("ts")["amount"].resample("D").sum(), y_label="amount")

st.subheader("Volume by category")
st.bar_chart(df.groupby("category")["amount"].sum(), y_label="amount")

st.subheader("Largest transactions")
st.dataframe(df.nlargest(20, "amount"), hide_index=True, use_container_width=True)
