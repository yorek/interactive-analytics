#!/usr/bin/env bash
# Print the latest container logs from the benchmark job service.
#
# Env overrides:
#   SF_CONNECTION  Snowflake connection name (default: PM)
#   SERVICE_NAME   Job service name          (default: IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB)
#   LOG_LINES      Number of log lines       (default: 1000)
set -euo pipefail

SF_CONNECTION="${SF_CONNECTION:-PM}"
SERVICE_NAME="${SERVICE_NAME:-IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB}"
LOG_LINES="${LOG_LINES:-1000}"

snow sql -c "${SF_CONNECTION}" --silent -q \
  "SELECT SYSTEM\$GET_SERVICE_LOGS('${SERVICE_NAME}', 0, 'iwbench', ${LOG_LINES});"
