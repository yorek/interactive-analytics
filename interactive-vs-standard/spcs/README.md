# Run the benchmark inside Snowpark Container Services (SPCS)

Running the load test client inside Snowflake's own infrastructure removes the
two largest sources of noise the root [`README.md`](../README.md) calls out:

- **Network latency.** A laptop on Wi-Fi adds tens of milliseconds of RTT to
  every round trip; SPCS containers talk to the Snowflake service over the
  internal network.
- **Local CPU contention.** Thread scheduling, connector overhead, and other
  apps on your machine perturb client-side timing. SPCS gives the benchmark a
  dedicated, sized compute pool node.

Together this makes the latency / throughput numbers you see for `STD_WH` vs.
`IW_WH` more precise and more reproducible.

## Layout

```
spcs/
├── Dockerfile               # uv-based image
├── .dockerignore
├── entrypoint.sh            # BENCH_* env vars → iwtest.py CLI flags
├── service-spec.yaml        # reference job-service spec
├── sql/
│   ├── 00_setup_spcs.sql    # compute pool, image repo, grants checklist
│   └── 10_run_benchmark.sql # manual EXECUTE JOB SERVICE template
└── scripts/
    ├── build_and_push.sh    # docker build + push to image repo
    ├── run_benchmark.sh     # EXECUTE JOB SERVICE + status polling
    ├── get_logs.sh          # SYSTEM$GET_SERVICE_LOGS
    └── get_metrics.sh       # SYSTEM$GET_SERVICE_METRICS (CPU/mem/net/storage)
```

## Prerequisites

1. The base benchmark objects already exist (run [`../sql/setup-test.sql`](../sql/setup-test.sql) once).
2. Snowflake CLI (`snow`) configured with a connection — defaults to `PM`,
   override with `SF_CONNECTION=<name>`.
3. Docker with `linux/amd64` build support (Apple Silicon: enabled by default
   via buildx).

## Quickstart

```bash
# 1. One-time SPCS infra (compute pool, image repository).
snow sql -c PM -f spcs/sql/00_setup_spcs.sql

# 2. Build and push the image.
./spcs/scripts/build_and_push.sh

# 3. Run a comparison benchmark.
BENCH_COMPARE=STD_WH,IW_WH BENCH_USERS=20 BENCH_ITERATIONS=100 \
  ./spcs/scripts/run_benchmark.sh

# 4. Re-fetch logs anytime.
./spcs/scripts/get_logs.sh
```

`run_benchmark.sh` polls until the job reports `DONE`/`FAILED` and prints the
container's stdout (the same `[init] / [tables] / [warehouse] / [interactive] /
metric (s)` output the local CLI prints).

## Configuration

The container reads benchmark options from `BENCH_*` env vars:

| env var             | default          | maps to              |
| ------------------- | ---------------- | -------------------- |
| `BENCH_USERS`       | `20`             | `--users`            |
| `BENCH_ITERATIONS`  | `100`            | `--iterations`       |
| `BENCH_WORKLOAD`    | `query1`         | `--workload`         |
| `BENCH_DATABASE`    | `IW_PLAYGROUND`  | `--database`         |
| `BENCH_SCHEMA`      | `IW_TEST`        | `--schema`           |
| `BENCH_SAMPLE_SIZE` | `5000`           | `--sample-size`      |
| `BENCH_SEED`        | `42`             | `--seed`             |
| `BENCH_WAREHOUSE`   | —                | `--warehouse`        |
| `BENCH_COMPARE`     | `STD_WH,IW_WH` (when `BENCH_WAREHOUSE` is unset) | `--compare` |
| `BENCH_BOOTSTRAP_WAREHOUSE` | `STD_WH`  | session warehouse for OAuth helper queries (row counts, `SHOW WAREHOUSES`, preload). Per-thread benchmark code still issues its own `USE WAREHOUSE`, so this only affects setup/probe queries. |

`BENCH_WAREHOUSE` and `BENCH_COMPARE` are mutually exclusive, mirroring the
CLI.

## Authentication inside the container

`iwtest.py` auto-detects SPCS by checking for `/snowflake/session/token`. When
present, it connects via OAuth using `SNOWFLAKE_HOST` / `SNOWFLAKE_ACCOUNT` (set
by SPCS automatically) and ignores `--connection`. Outside SPCS, the original
`connections.toml`-based flow is unchanged.

## Container metrics (CPU, memory, network, storage)

The service spec has `platformMonitor.metricConfig` enabled with the `system`,
`system_limits`, `status`, `network`, and `storage` groups. While the job is
running (and for a short window after) you can fetch a snapshot with:

```bash
./spcs/scripts/get_metrics.sh
```

or directly:

```sql
SELECT SYSTEM$GET_SERVICE_METRICS('IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB');
```

For historical analysis (e.g. plotting CPU over time), point the account event
table at the service and query it:

```sql
SELECT timestamp, record:metric:name::string AS metric, value
FROM <event_table>
WHERE resource_attributes:"snow.service.name" = 'IW_BENCH_JOB'
  AND record_type = 'METRIC'
ORDER BY timestamp;
```

## Updating the image

```bash
./spcs/scripts/build_and_push.sh
# Then re-run; SPCS will pull the new :latest on the next EXECUTE JOB SERVICE.
```

## Cleanup

```sql
DROP SERVICE IF EXISTS IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB;
DROP COMPUTE POOL IF EXISTS IW_BENCH_POOL;
DROP IMAGE REPOSITORY IF EXISTS IW_PLAYGROUND.IW_TEST.IW_REPO;
```
