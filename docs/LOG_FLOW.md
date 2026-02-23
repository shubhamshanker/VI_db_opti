# Log Flow (End-to-End)

## Sequence
1. `POOL_INIT_START`
2. `POOL_INIT_NODE_START` / `POOL_INIT_NODE_DONE` (for each node)
3. `POOL_INIT_DONE`
4. `FED_QUERY_START req=<id>`
5. `ROUTING_DONE req=<id>` + `ROUTING_NODE req=<id>`
6. `SPLIT_PLAN req=<id>` + `NODE_PLAN req=<id>` (query split and binds)
7. `FED_QUERY_SCATTER req=<id>`
8. `NODE_QUERY_START req=<id>` (parallel per node)
9. `NODE_QUERY_DONE req=<id>` or `NODE_QUERY_FAIL req=<id>`
10. `CONSOLIDATION_DONE req=<id>`
11. `FED_QUERY_DONE req=<id>`
12. `APP_RESULT req=<id>`
13. `APP_SAMPLE_ROW req=<id>` (if rows exist)
14. `POOL_CLOSE_DONE`

## API Sequence (Manager + Technical Dual Output)
1. `POST /query` receives list of circles.
2. API executes federated query and generates structured payload.
3. API builds manager summary from structured payload.
4. API returns:
   - `technical` JSON section (full traceable details)
   - `manager_summary` text section (plain English)

## What Each Log Proves
- Routing correctness: `ROUTING_NODE`
- Query split correctness: `SPLIT_PLAN`, `NODE_PLAN`
- Parallel execution: multiple `NODE_QUERY_START` under same `req`
- Node execution details: `NODE_QUERY_START ... sql=... bind_values=...`
- Node latency: `NODE_QUERY_DONE ... elapsed_ms`
- Consolidation behavior: `CONSOLIDATION_DONE`
- End-to-end latency: `FED_QUERY_DONE ... elapsed_ms`
- Functional correctness: `APP_RESULT row_count`

## Manager Summary Fields
- What was asked
- How request was split (fanout + node plans)
- What executed (SQL template + bind values)
- How consolidated (merged rows + merge time)
- Final status (rows, failures, end-to-end latency)

## Quick Demo Commands
- Main flow: `source .venv/bin/activate && python main.py`
- Main flow no-color: `python main.py --no-color`
- Perf summary: `python benchmark.py --runs 50`
- Perf summary no-color: `python benchmark.py --runs 50 --no-color`
- API server: `python main.py --mode server --port 8080`
- API request:
  - `curl -s -X POST http://127.0.0.1:8080/query -H 'Content-Type: application/json' -d '{"circles":["circle_1","circle_7","circle_12"]}'`
  - `curl -s -X POST http://127.0.0.1:8080/query -H 'Content-Type: application/json' -d '{"query_items":[{"input_id":"exact","device_id":"device_a","circle_id":"circle_1","cluster_id":"cluster_red"},{"input_id":"fallback","device_id":"device_unknown","circle_id":"circle_7","cluster_id":"cluster_unknown"}]}'`

## Example KPI Snapshot
- `p50` (typical latency)
- `p95` (tail latency target)
- `p99` (stress/rare tail)
- error events count (`NODE_QUERY_FAIL`)

## Readability Features
- Rich colorized logging for CLI flow analysis.
- Rich tables/panels for split plan, node outcomes, and benchmark metrics.
- `--no-color` switch for CI/plain terminals.
