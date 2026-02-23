# Log Flow (End-to-End)

## Sequence
1. `POOL_INIT_START`
2. `POOL_INIT_NODE_START` / `POOL_INIT_NODE_DONE` (for each node)
3. `POOL_INIT_DONE`
4. `FED_QUERY_START req=<id>`
5. `ROUTING_DONE req=<id>` + `ROUTING_NODE req=<id>`
6. `FED_QUERY_SCATTER req=<id>`
7. `NODE_QUERY_START req=<id>` (parallel per node)
8. `NODE_QUERY_DONE req=<id>` or `NODE_QUERY_FAIL req=<id>`
9. `FED_QUERY_DONE req=<id>`
10. `APP_RESULT req=<id>`
11. `APP_SAMPLE_ROW req=<id>` (if rows exist)
12. `POOL_CLOSE_DONE`

## What Each Log Proves
- Routing correctness: `ROUTING_NODE`
- Parallel execution: multiple `NODE_QUERY_START` under same `req`
- Node latency: `NODE_QUERY_DONE ... elapsed_ms`
- End-to-end latency: `FED_QUERY_DONE ... elapsed_ms`
- Functional correctness: `APP_RESULT row_count`

## Quick Demo Commands
- Main flow: `source .venv/bin/activate && python main.py`
- Perf summary: `python benchmark.py --runs 50`

## Example KPI Snapshot
- `p50` (typical latency)
- `p95` (tail latency target)
- `p99` (stress/rare tail)
- error events count (`NODE_QUERY_FAIL`)
