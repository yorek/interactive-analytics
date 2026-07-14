# TPC-H Benchmark on Snowflake Interactive Warehouse

A small Python harness that runs the [TPC-H query set](https://github.com/ClickHouse/tpc-h-openhouse/tree/main/snowflake/queries) against a Snowflake **Interactive Warehouse**. Setup copies TPC-H tables from Snowflake's shared [`SNOWFLAKE_SAMPLE_DATA`](https://docs.snowflake.com/en/user-guide/sample-data-tpch) database into your own benchmark database.

## Data source

This project reads from [`SNOWFLAKE_SAMPLE_DATA`](https://docs.snowflake.com/en/user-guide/sample-data-tpch), Snowflake's read-only shared database of sample datasets. Within that database, TPC-H data lives in the schemas `TPCH_SF1`, `TPCH_SF10`, `TPCH_SF100`, and `TPCH_SF1000` (scale factors 1, 10, 100, and 1000). Because `SNOWFLAKE_SAMPLE_DATA` is read-only, `setup` CTAS-copies the eight TPC-H tables from the chosen source schema into `IW_TPCH_BENCH` so you can create standard and interactive tables for benchmarking.

See [Sample data: TPC-H](https://docs.snowflake.com/en/user-guide/sample-data-tpch) for schema details, query definitions, and Snowflake's benchmarking recommendations.

## Layout

```
.
├── iwtpch.sh           # convenience wrapper (same as `uv run iw-tpch`)
├── pyproject.toml          # uv project (snowflake-connector-python)
├── queries/
│   ├── original/           # 22 TPC-H queries 
│   └── modern/             # rewritten variants (window functions / QUALIFY)
├── tpc-h-results-1GB.json  # SF1 reference rows for optional result validation
├── sql/
│   ├── setup.sql           # template: standard + interactive tables ({{SCALE}} placeholders)
│   └── teardown.sql        # template: drops per-scale warehouses ({{SCALE}} placeholders)
├── src/
│   ├── tpch_runner.py      # CLI entry shim
│   └── tpch/
│       ├── cli.py          # argparse + main
│       ├── commands.py     # setup / run / teardown
│       ├── config.py       # paths, env, target context
│       ├── connection.py   # Snowflake connect + session
│       ├── execution.py    # query runs
│       ├── queries.py      # load query files
│       ├── results.py      # stats, print, JSON/CSV output
│       ├── sql_scripts.py  # DDL script runner
│       └── validation.py   # SF1 result validation against tpc-h-results-1GB.json
└── results/                # JSON + CSV per benchmark run
```

## Prerequisites

- `uv` (the rule for this repo) — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- A Snowflake connection in `~/.snowflake/connections.toml`, with a role (`SYSADMIN` is used in the setup scripts) that can create databases and warehouses, and read the source schemas in `SNOWFLAKE_SAMPLE_DATA`: `TPCH_SF1`, `TPCH_SF10`, `TPCH_SF100`, and `TPCH_SF1000`.
- Copy `.env.example` to `.env` and set at least `CONNECTION_NAME` (the connection name from `connections.toml`).

```bash
uv sync
cp .env.example .env   # then edit CONNECTION_NAME, etc.
```

## Scales

The benchmark supports four TPC-H scale factors, selected with `--scale {1,10,100,1000}` (default from `DEFAULT_SCALE` in `.env`, or `10` if unset):

| Scale | Standard target | Interactive target |
|---|---|---|
| `1` | `IW_TPCH_BENCH.TPCH_SF1` | `IW_TPCH_BENCH.TPCH_SF1_IT` |
| `10` | `IW_TPCH_BENCH.TPCH_SF10` | `IW_TPCH_BENCH.TPCH_SF10_IT` |
| `100` | `IW_TPCH_BENCH.TPCH_SF100` | `IW_TPCH_BENCH.TPCH_SF100_IT` |
| `1000` | `IW_TPCH_BENCH.TPCH_SF1000` | `IW_TPCH_BENCH.TPCH_SF1000_IT` |

Standard tables live in `TPCH_SF<scale>`; interactive tables live in `TPCH_SF<scale>_IT`. Both targets use the `IW_TPCH_BENCH` database unless overridden via CLI flags.

## Snowflake objects created

`setup --scale N` creates (or reuses) objects in `IW_TPCH_BENCH`. Run setup separately for each scale you want to benchmark.

| Object | Type | Notes |
|---|---|---|
| `IW_TPCH_BENCH` | Database | shared across scales; created if not exists |
| `TPCH_SF<scale>` | Schema | 8 standard tables (CTAS from `SNOWFLAKE_SAMPLE_DATA.TPCH_SF<scale>`) |
| `TPCH_SF<scale>_IT` | Schema | 8 interactive tables, clustered for the benchmark workload |
| `IW_TPCH_LOAD_WH` | Standard warehouse | temporary; sized for the CTAS load and dropped at the end of setup (interactive warehouses cannot run DDL) |
| `TPCH_BENCH_WH_<scale>` | Standard warehouse | used by `--target standard` |
| `IW_TPCH_BENCH_WH_<scale>` | Interactive warehouse | attached to the eight `TPCH_SF<scale>_IT` tables; `FALLBACK_WAREHOUSE` is `TPCH_BENCH_WH_<scale>` |

Per scale:

| Scale | Schemas | Load warehouse | Standard warehouse | Interactive warehouse |
|---|---|---|---|---|
| `1` | `TPCH_SF1`, `TPCH_SF1_IT` | `IW_TPCH_LOAD_WH` (SMALL) | `TPCH_BENCH_WH_1` (SMALL) | `IW_TPCH_BENCH_WH_1` (SMALL) |
| `10` | `TPCH_SF10`, `TPCH_SF10_IT` | `IW_TPCH_LOAD_WH` (LARGE) | `TPCH_BENCH_WH_10` (MEDIUM) | `IW_TPCH_BENCH_WH_10` (MEDIUM) |
| `100` | `TPCH_SF100`, `TPCH_SF100_IT` | `IW_TPCH_LOAD_WH` (XLARGE) | `TPCH_BENCH_WH_100` (LARGE) | `IW_TPCH_BENCH_WH_100` (LARGE) |
| `1000` | `TPCH_SF1000`, `TPCH_SF1000_IT` | `IW_TPCH_LOAD_WH` (XXLARGE) | `TPCH_BENCH_WH_1000` (XXLARGE) | `IW_TPCH_BENCH_WH_1000` (XXLARGE) |

## Usage

Use `./iwtpch.sh` as a shorthand for `uv run iw-tpch` (both accept the same arguments):

```bash
# 1. Create interactive tables + warehouse for a scale (default scale 10)
./iwtpch.sh setup --scale 1
./iwtpch.sh setup --scale 10
./iwtpch.sh setup --scale 100
./iwtpch.sh setup --scale 1000   # run separately for each scale you want to benchmark

# 2. Run the 22 TPC-H queries (interactive target, scale from DEFAULT_SCALE, original workload are the defaults)
./iwtpch.sh run
./iwtpch.sh run --target interactive --scale 10  --workload original
./iwtpch.sh run --target interactive --scale 100 --workload modern

# Same queries on a standard (non-interactive) warehouse, using the standard tables in IW_TPCH_BENCH
./iwtpch.sh run --target standard --scale 10  --workload original
./iwtpch.sh run --target standard --scale 100 --workload modern

# Single query / subset / repeated runs
./iwtpch.sh run --query 17
./iwtpch.sh run --target standard --queries 2,11,15,17,22
./iwtpch.sh run --repeats 5      # best of 5 executions per query
./iwtpch.sh run --iterations 3   # 3 full workload passes

# 3. Optional cleanup (drops benchmark warehouses for a scale)
./iwtpch.sh teardown --scale 10

# Override connection, database, schema, or warehouse (any subset)
./iwtpch.sh run \
  --connection PM \
  --database IW_TPCH_BENCH \
  --schema TPCH_SF100 \
  --warehouse TPCH_BENCH_WH_100

# Same connection override on setup / teardown
./iwtpch.sh setup --scale 10 --connection PM
./iwtpch.sh teardown --scale 10 --connection PM
```

Each run writes `results/run_<target>_sf<scale>_<workload>_<UTC-timestamp>.json` and `.csv` containing per-query rows (status, row count, client elapsed, server elapsed from `INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION`, query_id, error) and a summary (connection, database, schema, warehouse, warehouse size, and timing stats). When running at SF1 (`--scale 1`), see [Result validation (SF1)](#result-validation-sf1) below.

## Result validation (SF1)

When you run the benchmark with `--scale 1`, each successful query is checked against the reference rows in `tpc-h-results-1GB.json`. Validation runs automatically after all queries finish; no extra flag is required.

### What is checked

The JSON file defines **one reference row per query** (e.g. Q1, Q2, … Q22). For each query, validation succeeds if that row **appears somewhere in the result set**. Extra rows are allowed — a query that returns 100 rows passes as long as the reference row is among them.

Matching rules (`src/tpch/validation.py`):

- **Column names** — case-insensitive; supports aliases (`YEAR` vs `O_YEAR`, `SUM(L_QUANTITY)` vs `SUM_L_QUANTITY`).
- **Strings** — trailing spaces from TPC-H `CHAR` padding are ignored.
- **Numbers** — `Decimal` and float values are compared with tolerance; numeric strings (e.g. `'13'` vs `13`) are coerced.
- **Dates** — compared as ISO date strings.

The results table gains `valid` (PASS / FAIL / SKIP) and `validation_error` columns. The process exit code is `1` if any query fails execution or validation.

### Reference data source

`tpc-h-results-1GB.json` holds expected output for the **1 GB qualification database** (SF1), using the default substitution parameters from the TPC-H specification (e.g. Q9 with `COLOR = green`). Values were taken from running the queries against this project's Snowflake SF1 tables and cross-checked against the TPC-H spec where possible.

### Q9: two published reference values

Q9 (Product Type Profit Measure) is the one query where published references disagree. For **ALGERIA / 1998** with `p_name LIKE '%green%'`:

| Source | `sum_profit` |
|---|---|
| **This repo** (`tpc-h-results-1GB.json`) | **27136900.18** |
| Snowflake SF1 (this project) | 27136900.1803 |
| [TPC-H v3.0.1 spec](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf) §2.4.9.5 | 31342867.24 |
| [Older spec mirror](https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml) §9.4 | 27136900.1803000 |

The current [TPC.org specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf) lists **31342867.24**, but that value does not match what Snowflake returns for the SF1 data loaded by this harness (**27136900.18**). The older figure aligns with our measured output; it also appears in pre–answer-set-regeneration copies of the spec (TPC-H v2.13.0 regenerated answer sets after dbgen fixes — see the spec revision history).

**We keep 27136900.18** in `tpc-h-results-1GB.json` so validation reflects the results this project actually produces on Snowflake SF1, not the current TPC.org sample row. If you load SF1 with a different dbgen version or source, Q9 is the query most likely to disagree with the reference file.

To inspect the Q9 row manually:

```sql
-- substitution parameter: COLOR = green
SELECT nation, o_year, sum_profit
FROM (
  SELECT n_name AS nation,
         EXTRACT(year FROM o_orderdate) AS o_year,
         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
  FROM part, supplier, lineitem, partsupp, orders, nation
  WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
) profit
GROUP BY nation, o_year
HAVING nation = 'ALGERIA' AND o_year = 1998;
```

## Configuration

| Setting | Default | Override |
|---|---|---|
| Connection | `CONNECTION_NAME` in `.env` | `--connection` on `setup`, `run`, `teardown` |
| Scale | `DEFAULT_SCALE` in `.env` (fallback `10`) | `--scale` on `setup`, `run` |
| Database | `IW_TPCH_BENCH` | `--database` on `run` |
| Schema | `TPCH_SF<scale>` (standard) or `TPCH_SF<scale>_IT` (interactive) | `--schema` on `run` |
| Warehouse | `<prefix>_<scale>` for the target (e.g. `IW_TPCH_BENCH_WH_10`) | `--warehouse` on `run` |

CLI flags take precedence over defaults derived from `--target` and `--scale`. Omit them to keep the built-in naming above.

## Targets (engines)

`--target` selects which engine the same query files run against:

| Target | Database | Schema | Warehouse | Tables |
|---|---|---|---|---|
| `interactive` (default) | `IW_TPCH_BENCH` | `TPCH_SF<scale>_IT` | `IW_TPCH_BENCH_WH_<scale>` (interactive) | interactive copies created by `setup` |
| `standard` | `IW_TPCH_BENCH` | `TPCH_SF<scale>` | `TPCH_BENCH_WH_<scale>` (standard) | standard copies created by `setup` |

The query files use unqualified table names, so they resolve via `USE DATABASE`/`USE SCHEMA` and run unchanged on either target and scale. Use `--database`, `--schema`, and `--warehouse` to point at a different Snowflake context without changing the query files.

## Modern query rewrites

Each query was analysed for opportunities to use modern Snowflake SQL (window functions, `QUALIFY`, single-pass CTEs) to make the SQL code more compact and easy to understand. All rewrites were verified to return identical result sets.

| Query | Rewrite | Why it's faster |
|---|---|---|
| Q02 | `QUALIFY ps_supplycost = MIN(ps_supplycost) OVER (PARTITION BY p_partkey)` | Removes the correlated `min()` subquery that re-joined partsupp/supplier/nation/region a second time |
| Q11 | `QUALIFY value > SUM(value) OVER () * 0.0001` | Computes the grand total in a single pass instead of a correlated `HAVING` subquery that repeated the full join |
| Q15 | `QUALIFY total_revenue = MAX(total_revenue) OVER ()` | The CTE is scanned once instead of being referenced again in a `MAX` subquery |
| Q17 | `AVG(l_quantity) OVER (PARTITION BY l_partkey)` | Single `lineitem` pass instead of a correlated per-part `AVG` subquery (largest single-query win) |
| Q18 | CTE computes per-order `SUM(l_quantity)` once and reuses it | Original scans `lineitem` twice (the `IN` subquery's per-order sum equals the final `SUM(l_quantity)`); the rewrite scans it once|
| Q22 | single-pass CTE + `AVG(...) OVER ()` | Scans `customer` once instead of twice (threshold + outer) |

## Notes

- Interactive warehouses can only query interactive tables, so a standard warehouse (`IW_TPCH_LOAD_WH`) is created for the CTAS load.
- `client_elapsed_s` is wall-clock timing measured around `cur.execute` + `fetchall` (so it includes result transfer); `server_elapsed_s` is `TOTAL_ELAPSED_TIME` from Snowflake's query history.
- Each query is executed `--repeats` times (default 3) and the **best** (minimum) client and server times are kept. The first execution warms the warehouse's local cache, so the best of the later runs reflects warm-cache performance (this replaces a separate warm-up phase). The JSON output also records every attempt's `query_id`. Use `--repeats 1` for a single execution per query.
