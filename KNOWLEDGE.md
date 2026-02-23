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
- Added richer routing in `main.py`:
  - exact `device+circle+cluster` mapping
  - fallback routing by circle, cluster, and device
  - route reason (`matched_by`) included in response
- Added explicit split/consolidation transparency:
  - `query_plan` with SQL template + per-node bind values
  - `consolidation` with merged rows, merge time, success/fail node counts
- Added `benchmark.py`:
  - repeatable latency benchmark
  - p50/p95/p99 output
  - fanout and consolidation clarity metrics (`avg_fanout_nodes`, `avg_merge_ms`)

## Current Validation
- Federated fetch works across routed nodes.
- Logs clearly show routing, scatter-gather execution, and timings.
- Benchmark reports tail latency metrics for SLO discussion.
- API supports both:
  - `circles` input
  - `query_items` input (`device_id`, `circle_id`, `cluster_id`)

## Next Recommended Step
- Add SLO gate in benchmark (`--p95-threshold-ms`) and fail non-compliant runs.
- Externalize routing tables to DB/config service for production-scale updates.
