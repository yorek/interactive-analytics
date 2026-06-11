-- =========================================================================
-- Schema + data for the IW E2E benchmark (Interactive Tables variant).
--
-- Interactive Tables are CTAS-only (no separate CREATE + INSERT), and
-- Interactive Warehouses can ONLY query interactive tables. This file
-- creates both PRODUCTS and EVENTS as interactive tables, populates them
-- inline via the GENERATOR table function, and adds Search Optimization
-- on EVENTS(EVENT_ID) for the P1 lookup pattern.
--
-- Run on a STANDARD warehouse (e.g. PM_WH). The CTAS read/write path is
-- not allowed on an Interactive Warehouse.
-- =========================================================================

CREATE SCHEMA IF NOT EXISTS DMAURI_PLAYGROUND.IW_E2E_TEST;

USE DATABASE DMAURI_PLAYGROUND;
USE SCHEMA   IW_E2E_TEST;

-- Drop any prior STANDARD tables so we can re-create as interactive. If
-- they don't exist (or already are interactive), these are no-ops.
DROP TABLE IF EXISTS PRODUCTS;
DROP TABLE IF EXISTS EVENTS;
DROP TABLE IF EXISTS PRODUCTS_IT;
DROP TABLE IF EXISTS EVENTS_IT;

-- -------------------------------------------------------------------------
-- PRODUCTS (interactive): 100K rows across 100 categories.
-- -------------------------------------------------------------------------
CREATE OR REPLACE INTERACTIVE TABLE PRODUCTS
AS
SELECT
    SEQ4()                                   AS PRODUCT_ID,
    'Product-' || SEQ4()::STRING             AS NAME,
    'Category-' || (MOD(SEQ4(), 100) + 1)::STRING AS CATEGORY,
    ROUND(UNIFORM(5, 500, RANDOM()), 2)::NUMBER(10,2) AS PRICE
FROM TABLE(GENERATOR(ROWCOUNT => 100000));

CREATE OR REPLACE INTERACTIVE TABLE PRODUCTS_IT
CLUSTER BY (PRODUCT_ID)
AS
SELECT
    *
FROM 
    PRODUCTS;

-- -------------------------------------------------------------------------
-- EVENTS (interactive): 1B rows clustered on (TENANT_ID, EVENT_DATE).
--   * P0 queries prune via the clustering key.
--   * P1 lookups use Search Optimization on EVENT_ID.
-- ORDER BY in the source CTAS guarantees well-clustered initial partitions.
-- -------------------------------------------------------------------------
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
                'sa-east')                                             AS REGION,
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
                'sa-east') || '-' || (SUB_REGION_IDX + 1)::STRING      AS SUB_REGION
FROM (
    SELECT
        SEQ8()::NUMBER                                                 AS EVENT_ID,
        UNIFORM(1, 10000,   RANDOM(1))::NUMBER                          AS TENANT_ID,
        UNIFORM(1, 1000000, RANDOM(2))::NUMBER                          AS USER_ID,
        DATEADD(day, -UNIFORM(0, 364, RANDOM(3)), CURRENT_DATE())      AS EVENT_DATE,
        DATEADD(second,
                -UNIFORM(0, 86399, RANDOM(4)),
                DATEADD(day, -UNIFORM(0, 364, RANDOM(5)), CURRENT_TIMESTAMP()))::TIMESTAMP_NTZ
                                                                       AS EVENT_TS,
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
                    'login')                                           AS EVENT_TYPE,
        UNIFORM(0, 99999, RANDOM(7))::NUMBER                           AS PRODUCT_ID,
        ROUND(UNIFORM(1, 50000, RANDOM(8)) / 100.0, 2)::NUMBER(12,2)   AS AMOUNT,
        UNIFORM(1, 10, RANDOM(9))::NUMBER                              AS QUANTITY,
        MOD(ABS(RANDOM(10))::NUMBER, 12)                               AS REGION_IDX,
        MOD(ABS(RANDOM(11))::NUMBER, 4)                                AS SUB_REGION_IDX
    FROM TABLE(GENERATOR(ROWCOUNT => 1000000000))
)
ORDER BY TENANT_ID, EVENT_DATE;

CREATE OR REPLACE INTERACTIVE TABLE "EVENTS_IT"
CLUSTER BY (TENANT_ID, EVENT_DATE)
AS
SELECT
    *
FROM 
    "EVENTS";

-- Search Optimization for the P1 lookup pattern: WHERE EVENT_ID = ?
ALTER TABLE "EVENTS" ADD SEARCH OPTIMIZATION ON EQUALITY(EVENT_ID);
ALTER TABLE "EVENTS_IT" ADD SEARCH OPTIMIZATION ON EQUALITY(EVENT_ID);
