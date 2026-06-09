#!/usr/bin/env bash
# Launch the benchmark job service and stream logs until it finishes.
#
# Env overrides (passed to the container as BENCH_*):
#   BENCH_USERS        default 20
#   BENCH_ITERATIONS   default 100
#   BENCH_WORKLOAD     default query1
#   BENCH_DATABASE     default IW_PLAYGROUND
#   BENCH_SCHEMA       default IW_TEST
#   BENCH_SAMPLE_SIZE  default 5000
#   BENCH_SEED         default 42
#   BENCH_WAREHOUSE    set for single-warehouse mode (mutually exclusive with BENCH_COMPARE)
#   BENCH_COMPARE      set for compare mode, e.g. STD_WH,IW_WH (default if neither is set)
#
# Infra overrides:
#   SF_CONNECTION  Snowflake connection name (default: PM)
#   POOL_NAME      Compute pool              (default: IW_BENCH_POOL)
#   SERVICE_NAME   Job service name          (default: IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB)
#   IMAGE_PATH     Image inside the registry (default: /iw_playground/iw_test/iw_repo/iwbench:latest)
set -euo pipefail

SF_CONNECTION="${SF_CONNECTION:-PM}"
POOL_NAME="${POOL_NAME:-IW_BENCH_POOL}"
SERVICE_NAME="${SERVICE_NAME:-IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB}"
IMAGE_PATH="${IMAGE_PATH:-/iw_playground/iw_test/iw_repo/iwbench:latest}"

BENCH_USERS="${BENCH_USERS:-20}"
BENCH_ITERATIONS="${BENCH_ITERATIONS:-100}"
BENCH_WORKLOAD="${BENCH_WORKLOAD:-query1}"
BENCH_DATABASE="${BENCH_DATABASE:-IW_PLAYGROUND}"
BENCH_SCHEMA="${BENCH_SCHEMA:-IW_TEST}"
BENCH_SAMPLE_SIZE="${BENCH_SAMPLE_SIZE:-5000}"
BENCH_SEED="${BENCH_SEED:-42}"
BENCH_BOOTSTRAP_WAREHOUSE="${BENCH_BOOTSTRAP_WAREHOUSE:-STD_WH}"

if [[ -n "${BENCH_WAREHOUSE:-}" && -n "${BENCH_COMPARE:-}" ]]; then
  echo "[run] error: set BENCH_WAREHOUSE OR BENCH_COMPARE, not both" >&2
  exit 2
fi
if [[ -z "${BENCH_WAREHOUSE:-}" && -z "${BENCH_COMPARE:-}" ]]; then
  BENCH_COMPARE="STD_WH,IW_WH"
fi

# Build the env block for the spec.
env_block=""
add_env() { env_block+="        ${1}: \"${2}\"
"; }
add_env BENCH_USERS       "${BENCH_USERS}"
add_env BENCH_ITERATIONS  "${BENCH_ITERATIONS}"
add_env BENCH_WORKLOAD    "${BENCH_WORKLOAD}"
add_env BENCH_DATABASE    "${BENCH_DATABASE}"
add_env BENCH_SCHEMA      "${BENCH_SCHEMA}"
add_env BENCH_SAMPLE_SIZE "${BENCH_SAMPLE_SIZE}"
add_env BENCH_SEED        "${BENCH_SEED}"
add_env BENCH_BOOTSTRAP_WAREHOUSE "${BENCH_BOOTSTRAP_WAREHOUSE}"
[[ -n "${BENCH_WAREHOUSE:-}" ]] && add_env BENCH_WAREHOUSE "${BENCH_WAREHOUSE}"
[[ -n "${BENCH_COMPARE:-}"   ]] && add_env BENCH_COMPARE   "${BENCH_COMPARE}"

read -r -d '' SPEC <<EOF || true
spec:
  containers:
    - name: iwbench
      image: ${IMAGE_PATH}
      env:
${env_block}      resources:
        requests:
          memory: 2Gi
          cpu: 2000m
        limits:
          memory: 4Gi
          cpu: 4000m
  platformMonitor:
    metricConfig:
      groups:
        - system
        - system_limits
        - status
        - network
        - storage
EOF

run_sql() {
  snow sql -c "${SF_CONNECTION}" --silent -q "$1"
}

# JSON output makes the status payload greppable (default TABLE format wraps it).
run_sql_json() {
  snow sql -c "${SF_CONNECTION}" --silent --format JSON -q "$1"
}

echo "[run] dropping previous job (if any): ${SERVICE_NAME}"
run_sql "DROP SERVICE IF EXISTS ${SERVICE_NAME};"

echo "[run] launching job service ${SERVICE_NAME} on ${POOL_NAME}"
run_sql "EXECUTE JOB SERVICE
  IN COMPUTE POOL ${POOL_NAME}
  NAME = ${SERVICE_NAME}
  ASYNC = TRUE
  FROM SPECIFICATION \$\$
${SPEC}
\$\$;"

echo "[run] polling status (Ctrl-C to stop polling; the job keeps running)"
# Disable pipefail just for the loop so a non-matching grep doesn't kill the script.
set +o pipefail
# SYSTEM$GET_SERVICE_STATUS returns JSON; --format JSON wraps it again so quotes are
# escaped (\"status\":\"PENDING\"). Stripping backslashes makes the inner JSON
# greppable as plain "status":"..." / "message":"...".
_TERMINAL_RE='"status"[[:space:]]*:[[:space:]]*"(DONE|FAILED|INTERNAL_ERROR)"'
while true; do
  status_json="$(run_sql_json "SELECT SYSTEM\$GET_SERVICE_STATUS('${SERVICE_NAME}') AS S;" 2>/dev/null)" || status_json=""
  unesc="$(printf '%s' "${status_json}" | tr -d '\\')"
  status="$(printf '%s' "${unesc}" | grep -oE '"status"[[:space:]]*:[[:space:]]*"[^"]+"' | head -n1 | sed -E 's/.*"([^"]+)"$/\1/')" || status=""
  message="$(printf '%s' "${unesc}" | grep -oE '"message"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n1 | sed -E 's/.*"([^"]*)"$/\1/')" || message=""
  echo "[run] $(date +%H:%M:%S) status=${status:-?}${message:+ message=\"${message}\"}"
  if printf '%s' "${unesc}" | grep -qE "${_TERMINAL_RE}"; then
    break
  fi
  sleep 5
done
set -o pipefail

echo "[run] final logs:"
run_sql "SELECT SYSTEM\$GET_SERVICE_LOGS('${SERVICE_NAME}', 0, 'iwbench', 1000);"
