CREATE DATABASE IF NOT EXISTS IW_PLAYGROUND;
USE DATABASE IW_PLAYGROUND;

CREATE SCHEMA IF NOT EXISTS IW_TEST;
USE SCHEMA IW_TEST;

-- Move to 3XL to drastically decrease create interactive table times
CREATE OR REPLACE WAREHOUSE STD_WH
WITH WAREHOUSE_SIZE = 'XXXLARGE';

-- Drop any prior STANDARD tables so we can re-create as interactive. If
-- they don't exist (or already are interactive), these are no-ops.
DROP TABLE IF EXISTS PRODUCTS;
DROP TABLE IF EXISTS "EVENTS";
DROP TABLE IF EXISTS PRODUCTS_IT;
DROP TABLE IF EXISTS EVENTS_IT;

-- Use the standard warehouse to create the standard and interactive tables
USE WAREHOUSE STD_WH;

-- Create standard table
-- 100K rows across 10 categories.
CREATE OR REPLACE TABLE PRODUCTS
AS
SELECT
    SEQ4() AS PRODUCT_ID,
    'Product-' || SEQ4()::STRING AS NAME,
    'Category-' || (MOD(SEQ4(), 100) + 1)::STRING AS CATEGORY,
    ROUND(UNIFORM(5, 500, RANDOM()), 2)::NUMBER(10,2) AS PRICE
FROM TABLE(GENERATOR(ROWCOUNT => 100000));

-- Create interactive table
CREATE OR REPLACE INTERACTIVE TABLE PRODUCTS_IT
CLUSTER BY (CATEGORY)
AS
SELECT
    *
FROM 
    PRODUCTS;

-- Create standard table
-- 1B rows with random data
CREATE OR REPLACE TABLE "EVENTS"
AS
SELECT
    EVENT_ID,
    TENANT_ID,
    USER_ID,
    EVENT_DATE,
    EVENT_TS,
    EVENT_TYPE,
    PRODUCT_ID,
    AMOUNT,
    QUANTITY,
    DECODE(REGION_IDX,
           0,  'us-east',
           1,  'us-west',
           2,  'us-central',
           3,  'ca-central',
           4,  'eu-west',
           5,  'eu-central',
           6,  'eu-north',
           7,  'ap-south',
           8,  'ap-southeast',
           9,  'ap-northeast',
           10, 'ap-east',
                'sa-east') AS REGION,
    DECODE(REGION_IDX,
           0,  'us-east',
           1,  'us-west',
           2,  'us-central',
           3,  'ca-central',
           4,  'eu-west',
           5,  'eu-central',
           6,  'eu-north',
           7,  'ap-south',
           8,  'ap-southeast',
           9,  'ap-northeast',
           10, 'ap-east',
                'sa-east') || '-' || (SUB_REGION_IDX + 1)::STRING AS SUB_REGION
FROM (
    SELECT
        SEQ8()::NUMBER AS EVENT_ID,
        UNIFORM(1, 10000,   RANDOM(1))::NUMBER AS TENANT_ID,
        UNIFORM(1, 1000000, RANDOM(2))::NUMBER AS USER_ID,
        DATEADD(day, -UNIFORM(0, 364, RANDOM(3)), CURRENT_DATE()) AS EVENT_DATE,
        DATEADD(second,
                -UNIFORM(0, 86399, RANDOM(4)),
                DATEADD(day, -UNIFORM(0, 364, RANDOM(5)), CURRENT_TIMESTAMP()))::TIMESTAMP_NTZ AS EVENT_TS,
        DECODE(MOD(ABS(RANDOM(6))::NUMBER, 12),
               0,  'view',
               1,  'click',
               2,  'search',
               3,  'add_to_cart',
               4,  'remove_from_cart',
               5,  'update_quantity',
               6,  'add_to_wishlist',
               7,  'begin_checkout',
               8,  'purchase',
               9,  'refund',
               10, 'sign_up',
                    'login') AS EVENT_TYPE,
        UNIFORM(0, 99999, RANDOM(7))::NUMBER AS PRODUCT_ID,
        ROUND(UNIFORM(1, 50000, RANDOM(8)) / 100.0, 2)::NUMBER(12,2) AS AMOUNT,
        UNIFORM(1, 10, RANDOM(9))::NUMBER AS QUANTITY,
        MOD(ABS(RANDOM(10))::NUMBER, 12) AS REGION_IDX,
        MOD(ABS(RANDOM(11))::NUMBER, 4) AS SUB_REGION_IDX
    FROM TABLE(GENERATOR(ROWCOUNT => 1000000000))
);

SELECT ROW_COUNT, BYTES / 1024 / 1024 / 1024 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'EVENTS';

-- Create interactive table
CREATE OR REPLACE INTERACTIVE TABLE "EVENTS_IT"
CLUSTER BY (TENANT_ID, EVENT_DATE)
AS
SELECT
    *
FROM 
    "EVENTS";

SELECT ROW_COUNT, BYTES / 1024 / 1024 / 1024 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'EVENTS_IT';

-- Search Optimization
ALTER TABLE "EVENTS" ADD SEARCH OPTIMIZATION ON EQUALITY(EVENT_ID);
ALTER TABLE "EVENTS_IT" ADD SEARCH OPTIMIZATION ON EQUALITY(EVENT_ID);

-- Back to XSMALL 
ALTER WAREHOUSE STD_WH
SET WAREHOUSE_SIZE = 'XSMALL';

-- Check warehouse details
SHOW WAREHOUSES LIKE 'STD_WH'
->> SELECT "state", "type", "size", "min_cluster_count", "max_cluster_count", "started_clusters" FROM $1;

-- View created tables
SELECT TABLE_NAME, TABLE_TYPE, CLUSTERING_KEY, ROW_COUNT, BYTES / 1024.0 / 1024 / 1024.0 AS SIZE_IN_GB 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE <> 'VIEW'
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Create interactive warehouse
CREATE OR REPLACE INTERACTIVE WAREHOUSE IW_WH
WAREHOUSE_SIZE = XSMALL;

USE WAREHOUSE STD_WH;
-- Attach interactive tables to interactive warehouse
ALTER WAREHOUSE IW_WH ADD TABLES (IW_TEST.PRODUCTS_IT, IW_TEST.EVENTS_IT);

SHOW WAREHOUSES LIKE 'IW_WH'
->> SELECT "state", "type", "size", "min_cluster_count", "max_cluster_count", "started_clusters", "tables" FROM $1;

--SHOW PARAMETERS IN WAREHOUSE;
