# Interactive vs Standard: Snowflake parallel load test

Compares query latency and query per second between a standard warehouse (`STD_WH`) and an
interactive warehouse (`IW_WH`) by running a chosen a sample **workload**
(one of two query shapes) in parallel across N simulated users.

> **End-to-end test:** This benchmark measures the full client-to-Snowflake round trip, not
> server-side query time alone. Results are therefore affected by **network latency** and
> **current CPU usage** on your machine (thread scheduling, connector overhead, etc.).
> Run under similar conditions when comparing warehouses or repeating a run.

## Workloads (`--workload`)

Both workloads join `CATALOG_SALES_IT` to `DATE_DIM_IT` and filter on a bound
`cs_item_sk` (and `d_year = 1999` for query1).

| value    | default | purpose |
| -------- | ------- | ------- |
| `query1` | **yes** | Full analytical path: join, 1999 filter, `GROUP BY d_date`, `ORDER BY d_date` — primary comparison workload. |
| `query2` | no      | Same join/filter as query1 but `LIMIT 1` — stops after the first matching row. |

Omit `--workload` to use **query1**.

## Setup

This project uses [uv](https://docs.astral.sh/uv/). Dependencies are declared
in `pyproject.toml` and locked in `uv.lock`. `uv run` syncs the environment
automatically on first use.

Make sure your `~/.snowflake/connections.toml` has a working connection (the
default this script targets is `PM`).

### Snowflake test objects (`sql/setup-test.sql`)

Run [`sql/setup-test.sql`](sql/setup-test.sql) once in Snowflake to create the
database, schema, warehouses, and interactive tables the benchmark expects. In
brief, it:

1. Creates `IW_PLAYGROUND.IW_TEST` and a standard warehouse `STD_WH`.
2. Builds two **interactive tables** from TPC-DS 10TB (`CATALOG_SALES_IT` —
   1999 sales only, clustered on `CS_ITEM_SK`; `DATE_DIM_IT` — full date
   dimension, clustered on `D_YEAR`, `D_MOY`). Table creation uses a large
   `STD_WH` size, then scales it back to XSMALL.
3. Creates an **interactive warehouse** `IW_WH` (XSMALL) and attaches both
   tables to it.

The script assumes source data in `NC_TPCDS_10TB.TPCDS_SF10TCL`; adjust those
references if your TPC-DS database/schema names differ. The TPC-DS 10TB dataset
is available for free on the [Snowflake Marketplace](https://app.snowflake.com/marketplace/listing/GZSTZTP0KNB/snowflake-tpc-ds-10tb).

The target schema must contain `CATALOG_SALES_IT` and `DATE_DIM_IT` (defaults:
database `IW_PLAYGROUND`, schema `IW_TEST`).

## Usage

Single warehouse:

```bash
uv run iwtest.py --users 20 --iterations 10 --warehouse STD_WH
uv run iwtest.py --users 20 --iterations 10 --warehouse IW_WH
```

Pick a workload (default is query1):

```bash
uv run iwtest.py --users 20 --iterations 10 --warehouse STD_WH --workload query1
uv run iwtest.py --users 20 --iterations 10 --warehouse STD_WH --workload query2
```

Side-by-side comparison:

```bash
uv run iwtest.py --users 20 --iterations 100 --compare STD_WH,IW_WH
```

Example `--compare` summary (20 users × 100 iterations, query1):

```
metric (s)                   STD_WH (X-Small)                  IW_WH (X-Small)    delta (b-a)
---------------------------------------------------------------------------------------------
avg                                     0.326                            0.117         -0.210
p50                                     0.299                            0.102         -0.197
p95                                     0.615                            0.230         -0.385
p99                                     0.946                            0.318         -0.628
min                                     0.133                            0.064         -0.069
max                                     1.378                            0.370         -1.008
throughput                              55.31                           151.36         +96.05
avg rows/q                             364.00                           364.00          +0.00

errors                                       0                                0
wall seconds                            36.157                           13.213
```

Non-reproducible run (new RNG seed each time):

```bash
uv run iwtest.py --users 20 --iterations 10 --compare STD_WH,IW_WH --seed random
```

### Flags

| flag             | default              | description |
| ---------------- | -------------------- | ----------- |
| `--users`        | 10                   | concurrent users (threads) |
| `--iterations`   | 10                   | queries per user |
| `--warehouse`    | —                    | `STD_WH` or `IW_WH` (single mode) |
| `--compare`      | —                    | compare two warehouses back-to-back, e.g. `STD_WH,IW_WH` |
| `--workload`     | `query1`             | `query1` or `query2` (see table above) |
| `--connection`   | `PM`                 | Snowflake connection name |
| `--database`     | `IW_PLAYGROUND`      | database containing TPC-DS tables |
| `--schema`       | `IW_TEST`            | schema with `CATALOG_SALES_IT` / `DATE_DIM_IT` |
| `--sample-size`  | 5000                 | distinct `cs_item_sk` values to preload |
| `--seed`         | `42`                 | RNG seed; use `random` for a different seed each run |

`--warehouse` and `--compare` are mutually exclusive; one is required.

## Run inside Snowpark Container Services

For more precise latency numbers, run the benchmark from inside Snowflake
itself. The [`spcs/`](spcs/) folder ships a uv-based container image, a job
service spec, and helper scripts that build the image, push it to an image
repository, launch the benchmark as an SPCS job service, and fetch its logs.
Running there removes laptop CPU jitter and WAN round-trip time from the
measurement. See [`spcs/README.md`](spcs/README.md) for the quickstart.
