# 15-Min Demo Runbook

## Goal
Show, in one short session, how routing, split execution, asyncio parallelism, pool usage, and consolidation work.

## 0-2 min: Setup
- `source .venv/bin/activate`
- `python -m pip install -r requirements.txt`
- Confirm DB is up on `localhost:5432`.

## 2-5 min: Single Run (manager + technical)
- `python main.py`
- Show:
  - rich panels/tables (`Federated Flow`, `Query Split Plan`, `Consolidation`)
  - `SPLIT_PLAN` and `NODE_PLAN`
  - `CONSOLIDATION_DONE`
  - optional plain mode: `python main.py --no-color`

## 5-9 min: API Demonstration
- Start server:
  - `python main.py --mode server --host 127.0.0.1 --port 8080`
- Health:
  - `curl -s http://127.0.0.1:8080/health`
- Rich routing request:
  - `curl -s -X POST http://127.0.0.1:8080/query -H 'Content-Type: application/json' -d '{"query_items":[{"input_id":"exact","device_id":"device_a","circle_id":"circle_1","cluster_id":"cluster_red"},{"input_id":"fallback","device_id":"device_unknown","circle_id":"circle_7","cluster_id":"cluster_unknown"},{"input_id":"cluster","device_id":"device_unknown","circle_id":"circle_12","cluster_id":"cluster_green"}]}'`
- In response, show:
  - `technical.routing.route_details`
  - `technical.query_plan.node_plans`
  - `technical.consolidation`
  - `manager_summary`

## 9-12 min: Performance Snapshot
- `python benchmark.py --runs 20`
- Show:
  - `p50`, `p95`, `p99`
  - `avg_fanout_nodes`, `avg_merge_ms`
  - Manager interpretation rich panel
  - optional plain mode: `python benchmark.py --runs 20 --no-color`

## 12-15 min: Explain Asyncio + Pool (simple)
- `asyncio`: sends node queries in parallel, reducing total latency.
- `asyncpg pool`: reuses live DB connections; avoids reconnect per request.
- Consolidation merges all node responses into one result and reports merge timing.

## Expected Success Signals
- `NODE_QUERY_FAIL` is zero in healthy run.
- `CONSOLIDATION_DONE` shows merged rows > 0.
- `manager_summary` explains outcome without technical jargon.
