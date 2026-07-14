# Interactive Analytics

This repository contains **samples, demos, and benchmarking tools** to help you
get started with Snowflake [**Interactive Analytics**](https://docs.snowflake.com/en/user-guide/interactive) scenarios — including
interactive warehouses, interactive tables, and low-latency, high-concurrency
workloads.

## Contents

### [`tpc-h/`](tpc-h/)

TPC-H benchmark harness for Snowflake **Interactive Warehouses**. Copies TPC-H tables from `SNOWFLAKE_SAMPLE_DATA` into a local benchmark database, then runs the 22 standard queries (original and modern rewrites) against standard or interactive tables at scale factors 1, 10, 100, and 1000. See [`tpc-h/README.md`](tpc-h/README.md) for setup and usage.

---

More samples and scenarios will be added over time.
