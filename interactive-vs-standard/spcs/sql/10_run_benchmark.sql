-- Run the benchmark inside SPCS as a one-shot job service.
-- Prefer using spcs/scripts/run_benchmark.sh (envsubst over service-spec.yaml).
-- Use this file when you want to launch a run manually from a SQL worksheet.

USE DATABASE IW_PLAYGROUND;
USE SCHEMA IW_TEST;

-- Job services persist after they finish; drop a previous run first.
DROP SERVICE IF EXISTS IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB;

EXECUTE JOB SERVICE
  IN COMPUTE POOL IW_BENCH_POOL
  NAME = IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB
  ASYNC = TRUE
  FROM SPECIFICATION $$
spec:
  containers:
    - name: iwbench
      image: /iw_playground/iw_test/iw_repo/iwbench:latest
      env:
        BENCH_USERS: "20"
        BENCH_ITERATIONS: "100"
        BENCH_COMPARE: "STD_WH,IW_WH"
        BENCH_WORKLOAD: "query1"
        BENCH_DATABASE: "IW_PLAYGROUND"
        BENCH_SCHEMA: "IW_TEST"
        BENCH_SAMPLE_SIZE: "5000"
        BENCH_SEED: "42"
        BENCH_BOOTSTRAP_WAREHOUSE: "STD_WH"
      resources:
        requests:
          memory: 2Gi
          cpu: 1000m
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
$$;

-- Track the job.
SELECT SYSTEM$GET_SERVICE_STATUS('IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB');

-- Tail the container logs (re-run until DONE).
SELECT SYSTEM$GET_SERVICE_LOGS('IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB', 0, 'iwbench', 1000);

-- Live container metrics (CPU/memory/network/storage) emitted by platformMonitor.
SELECT SYSTEM$GET_SERVICE_METRICS('IW_PLAYGROUND.IW_TEST.IW_BENCH_JOB');
