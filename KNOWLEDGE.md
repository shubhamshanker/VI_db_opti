# Knowledge Base (What Was Implemented)

## Environment
- Local PostgreSQL is available on `localhost:5432`.
- Logical DB split is simulated via 4 databases on same server: `db1`, `db2`, `db3`, `db4`.
- Credentials used in code: `user/pass`.

## Data and Schema
- Table used: `analytics_table`.
- Seeded deterministic rows per DB to validate routing and merged responses.

## Code Changes Delivered
- Added detailed observability to `main.py`:
  - request ID propagation (`req=<id>`)
  - pool init/close timing
  - routing logs
  - per-node query logs
  - end-to-end query timing
- Added `benchmark.py`:
  - repeatable latency benchmark
  - p50/p95/p99 output

## Current Validation
- Federated fetch works across routed nodes.
- Logs clearly show routing, scatter-gather execution, and timings.
- Benchmark reports tail latency metrics for SLO discussion.

## Next Recommended Step
- Add SLO gate in benchmark (`--p95-threshold-ms`) and fail non-compliant runs.
