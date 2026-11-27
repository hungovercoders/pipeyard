# Data contracts

A data contract is a written agreement about what a dataset contains: its columns, their
types, which ones must always be filled in, who owns it and what it is for. pipeyard uses
the [Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/latest/)
(ODCS), an open, vendor-neutral YAML format from the Linux Foundation's Bitol project, so
your contracts are not tied to pipeyard and can be read by other ODCS tools.

Give a dataset a contract and pipeyard holds every pipeline run to it. A run whose output
does not match **fails, with the reason, and adds nothing to the dataset**. The people
building reports can rely on the columns being what the contract says.

A contract is optional. A dataset without one accepts whatever a run writes, as before.

## A contract

```yaml
apiVersion: v3.2.0
kind: DataContract
id: coffee-orders
name: Coffee orders
version: 1.0.0
status: active
description:
  purpose: Every order taken at the till.
schema:
  - name: orders                  # must match the dataset's name
    properties:
      - name: order_id
        logicalType: integer
        required: true
        description: Unique per order.
      - name: ordered_at
        logicalType: timestamp
        required: true
      - name: drink
        logicalType: string
        required: true
      - name: price
        logicalType: number
        physicalType: DOUBLE
```

`apiVersion`, `kind`, `id`, `version` and a `schema` are needed. Everything else in the
standard (quality rules, service levels, team, pricing, servers, custom properties) is
accepted, stored untouched and returned when you ask for the contract back.

The example pipeline's contract is at
`examples/pipelines/fake-transactions/transactions.odcs.yaml`.

## Adding one

**In the UI:** open **Datasets**, click the dataset, and press **Add a data contract**. If
the dataset already has data, the form starts with a draft written from the columns of its
latest successful run, so you are editing rather than typing from scratch. Mistakes come
back as a list of what to fix.

**From a script:**

```sh
curl -X PUT localhost:8000/api/datasets/orders/contract \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/yaml" \
  --data-binary @orders.odcs.yaml
```

`GET` on the same address returns the stored document, and `DELETE` removes it.

You can register a contract before the dataset has any data.

## What is checked

When a run finishes, pipeyard reads the schema from the Parquet files the run actually
wrote (not from what the container's manifest claims) and compares it with the `schema`
object named after the dataset. For each property in the contract:

| Check | Rule |
|---|---|
| **The column exists** | Looked up by `physicalName` if given, otherwise `name`. |
| **The type matches** | If `physicalType` is given, the column's type must equal it. Otherwise it must belong to the `logicalType`. With neither, only existence is checked. |
| **No nulls where required** | If `required: true`, the column must have no null values in this run. |

Columns in the data that the contract does not mention are allowed.

How logical types map to the types in your Parquet files:

| `logicalType` | Accepts |
|---|---|
| `string` | `VARCHAR`, `UUID`, `ENUM` |
| `integer` | any integer type |
| `number` | any integer, `FLOAT`, `DOUBLE`, `DECIMAL` |
| `boolean` | `BOOLEAN` |
| `date` | `DATE`, and timestamps |
| `timestamp` | timestamps, with or without a time zone |
| `time` | `TIME` |
| `array`, `object`, `map` | lists, structs, maps |

`physicalType` is compared by name, ignoring case, and understands other systems' spellings
of the same type: `string` and `varchar(18)` equal `VARCHAR`, `int` equals `INTEGER`,
`double precision` equals `DOUBLE`, and so on. `DECIMAL(18,2)` must match exactly.

## When a run breaks the contract

The run is marked failed and says exactly what was wrong:

```
data contract 'Coffee orders' version 1.0.0 is not met:
- column 'drink' is missing
- column 'price' is VARCHAR, the contract says DOUBLE
- column 'order_id' is required but has 12 null values
```

As with any failed run, pipeyard deletes what it wrote, so report containers never see it.
The run record keeps the contract version it was checked against.

## Changing a contract

Save a new document over the old one; bump `version` so run records show which one applied.
A change takes effect for runs that finish afterwards. **Runs already in the dataset are
not rechecked**, so if you tighten a contract, older runs may not meet it.

## What is not enforced yet

Be clear-eyed about this: pipeyard enforces the schema part of ODCS and stores the rest.

- **Quality rules** (`quality:` blocks, such as row counts or value ranges) are not run.
- **`unique` and `primaryKey`** are not checked.
- **Service levels** (`slaProperties`, such as freshness) are not monitored.
- **`logicalTypeOptions`** (formats, minimums, patterns) are not applied.
- Documents are checked for the fields pipeyard needs, not validated against the full
  official ODCS JSON Schema. The example contract in this repository has been validated
  against it.
- ODCS v2 documents are rejected; use v3.
