-- One-time SPCS infrastructure for the interactive-vs-standard benchmark.
-- Prerequisite: sql/setup-test.sql has already created IW_PLAYGROUND.IW_TEST,
-- the two interactive tables, STD_WH, and IW_WH.
--
-- Run as ACCOUNTADMIN (or a role with CREATE COMPUTE POOL / CREATE IMAGE REPOSITORY).

USE DATABASE IW_PLAYGROUND;
USE SCHEMA IW_TEST;

-- Compute pool that hosts the benchmark container.
-- CPU_X64_M gives plenty of headroom so client-side threading isn't the bottleneck
-- when --users is high.
CREATE COMPUTE POOL IF NOT EXISTS IW_BENCH_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_M
  AUTO_RESUME = TRUE;

-- Image repository for the benchmark container.
CREATE IMAGE REPOSITORY IF NOT EXISTS IW_PLAYGROUND.IW_TEST.IW_REPO;

-- Print the registry URL — paste it into spcs/scripts/build_and_push.sh
-- (or the script will resolve it automatically with `snow spcs image-registry url`).
SHOW IMAGE REPOSITORIES IN SCHEMA IW_PLAYGROUND.IW_TEST;

-- ---------------------------------------------------------------------------
-- Required grants for the role that will EXECUTE JOB SERVICE.
-- Replace <ROLE> with the actual role and run the block below.
-- ---------------------------------------------------------------------------
-- GRANT USAGE   ON DATABASE IW_PLAYGROUND                       TO ROLE <ROLE>;
-- GRANT USAGE   ON SCHEMA   IW_PLAYGROUND.IW_TEST               TO ROLE <ROLE>;
-- GRANT USAGE   ON COMPUTE POOL IW_BENCH_POOL                   TO ROLE <ROLE>;
-- GRANT READ, WRITE ON IMAGE REPOSITORY IW_PLAYGROUND.IW_TEST.IW_REPO TO ROLE <ROLE>;
-- GRANT CREATE SERVICE ON SCHEMA IW_PLAYGROUND.IW_TEST          TO ROLE <ROLE>;
-- GRANT USAGE, OPERATE ON WAREHOUSE STD_WH                      TO ROLE <ROLE>;
-- GRANT USAGE, OPERATE ON WAREHOUSE IW_WH                       TO ROLE <ROLE>;
-- GRANT SELECT  ON TABLE IW_PLAYGROUND.IW_TEST.CATALOG_SALES_IT TO ROLE <ROLE>;
-- GRANT SELECT  ON TABLE IW_PLAYGROUND.IW_TEST.DATE_DIM_IT      TO ROLE <ROLE>;
