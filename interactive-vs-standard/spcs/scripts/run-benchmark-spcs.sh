#!/usr/bin/env bash
# Launch the benchmark job service and stream logs until it finishes.
#
# Benchmark options are passed to the container as BENCH_* env vars (no image rebuild).
# Set them via CLI flags (preferred), BENCH_* env vars, or project .env — later wins over earlier.
#
#   ./spcs/scripts/run-benchmark-spcs.sh --users 20 --iterations 100 --workload query1 --compare DM_STANDARD,DM_INTERACTIVE
#
# Infra overrides (laptop-side only, not passed to the container):
#   SF_CONNECTION  Snowflake connection name (default: PM)
#   POOL_NAME      Compute pool              (default: IW_BENCH_POOL)
#   SERVICE_NAME   Job service name          (default: IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB)
#   IMAGE_PATH     Image inside the registry (default: /iw_playground/iw_test/iw_repo/iwbench:latest)
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run-benchmark-spcs.sh [options]

Benchmark options (mirrors iwtest.py; executed inside SPCS):
  --users N              Concurrent users (default: 20)
  --iterations N         Queries per user (default: 100)
  --workload query1|query2
  --database NAME
  --schema NAME
  --seed N|random
  --warehouse WH         Single-warehouse mode (mutually exclusive with --compare)
  --compare WH1,WH2      Compare two warehouses (default from .env warehouses)
  -h, --help             Show this help

Infra overrides (env vars only): SF_CONNECTION, POOL_NAME, SERVICE_NAME, IMAGE_PATH
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [[ -f "${PROJECT_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/.env"
  set +a
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --users)        BENCH_USERS="$2"; shift 2 ;;
    --iterations)   BENCH_ITERATIONS="$2"; shift 2 ;;
    --workload)     BENCH_WORKLOAD="$2"; shift 2 ;;
    --database)     BENCH_DATABASE="$2"; shift 2 ;;
    --schema)       BENCH_SCHEMA="$2"; shift 2 ;;
    --seed)         BENCH_SEED="$2"; shift 2 ;;
    --warehouse)    BENCH_WAREHOUSE="$2"; shift 2 ;;
    --compare)      BENCH_COMPARE="$2"; shift 2 ;;
    -h|--help)      usage; exit 0 ;;
    *)
      echo "[run] error: unknown option: $1 (try --help)" >&2
      exit 2
      ;;
  esac
done

SF_CONNECTION="${SF_CONNECTION:-PM}"
POOL_NAME="${POOL_NAME:-IW_BENCH_POOL}"
SERVICE_NAME="${SERVICE_NAME:-IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB}"
IMAGE_PATH="${IMAGE_PATH:-/iw_playground/iw_test/iw_repo/iwbench:latest}"

BENCH_USERS="${BENCH_USERS:-20}"
BENCH_ITERATIONS="${BENCH_ITERATIONS:-100}"
BENCH_WORKLOAD="${BENCH_WORKLOAD:-query1}"
BENCH_DATABASE="${BENCH_DATABASE:-DMAURI_PLAYGROUND}"
BENCH_SCHEMA="${BENCH_SCHEMA:-ZUTEST}"
BENCH_SEED="${BENCH_SEED:-42}"
BENCH_STANDARD_WAREHOUSE="${BENCH_STANDARD_WAREHOUSE:-DM_STANDARD}"
BENCH_INTERACTIVE_WAREHOUSE="${BENCH_INTERACTIVE_WAREHOUSE:-DM_INTERACTIVE}"
BENCH_BOOTSTRAP_WAREHOUSE="${BENCH_BOOTSTRAP_WAREHOUSE:-${BENCH_STANDARD_WAREHOUSE}}"

if [[ -n "${BENCH_WAREHOUSE:-}" && -n "${BENCH_COMPARE:-}" ]]; then
  echo "[run] error: set BENCH_WAREHOUSE OR BENCH_COMPARE, not both" >&2
  exit 2
fi
if [[ -z "${BENCH_WAREHOUSE:-}" && -z "${BENCH_COMPARE:-}" ]]; then
  BENCH_COMPARE="${BENCH_STANDARD_WAREHOUSE},${BENCH_INTERACTIVE_WAREHOUSE}"
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
add_env BENCH_SEED        "${BENCH_SEED}"
add_env BENCH_BOOTSTRAP_WAREHOUSE "${BENCH_BOOTSTRAP_WAREHOUSE}"
add_env BENCH_STANDARD_WAREHOUSE   "${BENCH_STANDARD_WAREHOUSE}"
add_env BENCH_INTERACTIVE_WAREHOUSE "${BENCH_INTERACTIVE_WAREHOUSE}"
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

SETUP_HINT="run spcs/sql/00_setup_spcs.sql then spcs/scripts/build-and-push.sh"

run_sql() {
  snow sql -c "${SF_CONNECTION}" --silent -q "$1"
}

# JSON output makes the status payload greppable (default TABLE format wraps it).
run_sql_json() {
  snow sql -c "${SF_CONNECTION}" --silent --format JSON -q "$1"
}

sql_json_matches() {
  local sql="$1"
  local pattern="$2"
  local json
  json="$(run_sql_json "${sql}" 2>/dev/null)" || return 1
  printf '%s' "${json}" | tr -d '\\' | grep -qiE "${pattern}"
}

preflight_fail() {
  echo "[preflight] error: $1" >&2
  echo "[preflight] hint: ${SETUP_HINT}" >&2
  exit 1
}

parse_image_path() {
  local p="${IMAGE_PATH#/}"
  if [[ "${p}" != */*/*/*:* ]]; then
    preflight_fail "IMAGE_PATH must look like /db/schema/repo/image:tag (got ${IMAGE_PATH})"
  fi
  IMAGE_NAME="${p##*/}"
  IMAGE_NAME="${IMAGE_NAME%%:*}"
  IMAGE_TAG="${p##*/}"
  IMAGE_TAG="${IMAGE_TAG##*:}"
  local repo_path="${p%/*}"
  IFS='/' read -r _db _schema _repo <<< "${repo_path}"
  IMAGE_REPO_FQN="$(printf '%s.%s.%s' \
    "$(echo "${_db}" | tr '[:lower:]' '[:upper:]')" \
    "$(echo "${_schema}" | tr '[:lower:]' '[:upper:]')" \
    "$(echo "${_repo}" | tr '[:lower:]' '[:upper:]')")"
}

verify_spcs_setup() {
  echo "[preflight] checking SPCS prerequisites (connection=${SF_CONNECTION})"

  if ! command -v snow >/dev/null 2>&1; then
    preflight_fail "snow CLI not found (install Snowflake CLI and configure connection ${SF_CONNECTION})"
  fi

  if ! run_sql "SELECT 1;" >/dev/null 2>&1; then
    preflight_fail "cannot connect with snow sql -c ${SF_CONNECTION}"
  fi
  echo "[preflight] ok: snowflake connection"

  if ! sql_json_matches "SHOW COMPUTE POOLS LIKE '${POOL_NAME}';" \
      "\"name\"[[:space:]]*:[[:space:]]*\"${POOL_NAME}\""; then
    preflight_fail "compute pool ${POOL_NAME} not found"
  fi
  echo "[preflight] ok: compute pool ${POOL_NAME}"

  parse_image_path
  if ! sql_json_matches "SHOW IMAGE REPOSITORIES IN SCHEMA ${IMAGE_REPO_FQN%.*};" \
      "\"name\"[[:space:]]*:[[:space:]]*\"${IMAGE_REPO_FQN##*.}\""; then
    preflight_fail "image repository ${IMAGE_REPO_FQN} not found"
  fi
  echo "[preflight] ok: image repository ${IMAGE_REPO_FQN}"

  local images_json unesc_images
  images_json="$(run_sql_json "SHOW IMAGES IN IMAGE REPOSITORY ${IMAGE_REPO_FQN};" 2>/dev/null)" \
    || preflight_fail "cannot list images in ${IMAGE_REPO_FQN}"
  unesc_images="$(printf '%s' "${images_json}" | tr -d '\\')"
  if ! printf '%s' "${unesc_images}" | grep -qi "${IMAGE_NAME}"; then
    preflight_fail "image ${IMAGE_NAME}:${IMAGE_TAG} not found in ${IMAGE_REPO_FQN} (push with build-and-push.sh)"
  fi
  if ! printf '%s' "${unesc_images}" | grep -qi "${IMAGE_TAG}"; then
    preflight_fail "image tag ${IMAGE_TAG} not found for ${IMAGE_NAME} in ${IMAGE_REPO_FQN} (push with build-and-push.sh)"
  fi
  echo "[preflight] ok: container image ${IMAGE_NAME}:${IMAGE_TAG}"

  echo "[preflight] SPCS setup looks good"
}

log_remote_benchmark() {
  echo "[run] remote benchmark: workload runs inside Snowpark Container Services (not on this machine)"
  echo "[run] compute pool: ${POOL_NAME}"
  echo "[run] service: ${SERVICE_NAME}"
  echo "[run] image: ${IMAGE_PATH}"
  if [[ -n "${BENCH_COMPARE:-}" ]]; then
    echo "[run] mode: compare ${BENCH_COMPARE}"
  else
    echo "[run] mode: single warehouse ${BENCH_WAREHOUSE}"
  fi
  echo "[run] params: users=${BENCH_USERS} iterations=${BENCH_ITERATIONS} workload=${BENCH_WORKLOAD} database=${BENCH_DATABASE} schema=${BENCH_SCHEMA} seed=${BENCH_SEED}"
}

verify_spcs_setup
log_remote_benchmark

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
