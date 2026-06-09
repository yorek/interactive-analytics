# Interactive Analytics

This repository contains **samples, demos, and benchmarking tools** to help you
get started with Snowflake **Interactive Analytics** scenarios — including
interactive warehouses, interactive tables, and low-latency, high-concurrency
workloads.

## Contents

### [`interactive-vs-standard/`](interactive-vs-standard/)

A parallel-load benchmarking tool that compares query latency and throughput
(queries/second) between a **standard warehouse** (`STD_WH`) and an
**interactive warehouse** (`IW_WH`). It runs a configurable workload across N
simulated concurrent users against TPC-DS interactive tables, and reports
latency percentiles (p50/p95/p99), throughput, and side-by-side deltas.

Includes the SQL setup script to provision the required database, schema,
warehouses, and interactive tables. See the folder's
[README](interactive-vs-standard/README.md) for full usage details.

---

More samples and scenarios will be added over time.
