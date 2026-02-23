# High-Level Design (HLD)

## Objective
Federate one logical analytics query across multiple PostgreSQL databases, show exactly how requests are split/executed/consolidated, and keep outputs understandable for both engineers and managers.

## Components
- `DataFederator`: orchestrates pool init, routing, scatter-gather, and pool close.
- `NODE_CONFIGS` + `ACTIVE_DB_NODES`: config-driven node activation (supports 5-6 DBs via config only).
- Routing tables:
  - exact: `device + circle + cluster -> node`
  - fallback: `circle -> node`, then `cluster -> node`, then `device -> node`
- Node query workers: one async task per node using `asyncpg`.
- Observability layer: structured logs with `req`, split plan, node plan, and consolidation summary.

## Request Flow
1. Initialize pools for all configured nodes.
2. Accept request as either `circles` or `query_items` (`device_id`, `circle_id`, `cluster_id`).
3. Resolve route per input (exact rule first, fallback chain next).
4. Build query split plan (SQL template + per-node bind values).
5. Dispatch per-node SQL concurrently.
6. Consolidate node results into one merged response.
7. Return technical view + manager-friendly view.

## Non-Functional Design
- Concurrency: asyncio + asyncpg pool.
- Fault isolation: node-level try/except prevents global crash.
- Performance: split, per-node, merge, and end-to-end timers.
- Traceability: request-scoped logging (`req=<id>`) with explicit `SPLIT_PLAN`, `NODE_PLAN`, `CONSOLIDATION_DONE`.

## Why Asyncio And Pool (Simple)
- `asyncio`: runs node queries in parallel, so overall latency approaches slowest node instead of sum of all nodes.
- `asyncpg` pool: reuses open DB connections, reducing reconnect overhead and improving throughput.

## 15-Min Demo Path
1. Run single request (`python main.py`) and show manager summary.
2. Start API (`python main.py --mode server`) and call `POST /query` with `query_items`.
3. Show split plan (`query_plan.node_plans`) and consolidation (`consolidation`).
4. Run benchmark (`python benchmark.py --runs 20`) and show p95 + fanout + merge time.
