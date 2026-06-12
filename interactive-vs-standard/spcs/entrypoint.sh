#!/usr/bin/env bash
# Translate BENCH_* env vars into iwtest.py CLI flags and exec the benchmark.
# Inside SPCS, snowflake_connect auto-detects /snowflake/session/token and ignores --connection.
set -euo pipefail

cd /app

: "${BENCH_CPU_REQUEST:=4000m}"
: "${BENCH_MEMORY_REQUEST:=4Gi}"
: "${BENCH_CPU_LIMIT:=${BENCH_CPU_REQUEST}}"
: "${BENCH_MEMORY_LIMIT:=${BENCH_MEMORY_REQUEST}}"

# Bootstrap warehouse for OAuth-based SPCS connections (no default in connections.toml).
# Per-thread benchmark code issues its own USE WAREHOUSE; this only covers helper
# queries (row counts, SHOW WAREHOUSES, preload, cache-warm sizing, etc.).
: "${BENCH_BOOTSTRAP_WAREHOUSE:=STD_WH}"
export SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-${BENCH_BOOTSTRAP_WAREHOUSE}}"
export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-${BENCH_DATABASE:-IW_PLAYGROUND}}"
export SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-${BENCH_SCHEMA:-IW_TEST}}"

args=()

# Either single-warehouse mode or compare mode (mutually exclusive, like the CLI).
if [[ -n "${BENCH_COMPARE:-}" ]]; then
  args+=(--compare "${BENCH_COMPARE}")
elif [[ -n "${BENCH_WAREHOUSE:-}" ]]; then
  args+=(--warehouse "${BENCH_WAREHOUSE}")
else
  echo "[entrypoint] error: set BENCH_WAREHOUSE or BENCH_COMPARE" >&2
  exit 2
fi

[[ -n "${BENCH_USERS:-}" ]]       && args+=(--users "${BENCH_USERS}")
[[ -n "${BENCH_ITERATIONS:-}" ]]  && args+=(--iterations "${BENCH_ITERATIONS}")
[[ -n "${BENCH_WORKLOAD:-}" ]]    && args+=(--workload "${BENCH_WORKLOAD}")
[[ -n "${BENCH_DATABASE:-}" ]]    && args+=(--database "${BENCH_DATABASE}")
[[ -n "${BENCH_SCHEMA:-}" ]]      && args+=(--schema "${BENCH_SCHEMA}")
[[ -n "${BENCH_SEED:-}" ]]        && args+=(--seed "${BENCH_SEED}")
# --connection is ignored when SPCS token is present, but pass it through if set.
[[ -n "${BENCH_CONNECTION:-}" ]]  && args+=(--connection "${BENCH_CONNECTION}")

echo "[entrypoint] resources: cpu_request=${BENCH_CPU_REQUEST} memory_request=${BENCH_MEMORY_REQUEST} cpu_limit=${BENCH_CPU_LIMIT} memory_limit=${BENCH_MEMORY_LIMIT}" >&2
echo "[entrypoint] python iwtest.py ${args[*]}" >&2
exec python /app/iwtest.py "${args[@]}"
