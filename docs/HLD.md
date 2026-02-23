# High-Level Design (HLD)

## Objective
Federate one logical analytics query across multiple PostgreSQL databases and return a single merged response with low latency.

## Components
- `DataFederator`: orchestrates pool init, routing, scatter-gather, and pool close.
- `DB_CONFIGS`: maps logical node IDs to PostgreSQL DSNs.
- `CIRCLE_ROUTING_TABLE`: maps `circle_id` to the owning node.
- Node query workers: one async task per node using `asyncpg`.
- Observability layer: structured logs with `req` request ID and elapsed times.

## Request Flow
1. Initialize pools for all configured nodes.
2. Accept target circles from caller.
3. Route circles to node groups.
4. Dispatch per-node SQL queries concurrently.
5. Merge rows from all node responses.
6. Return unified result set to caller.

## Non-Functional Design
- Concurrency: asyncio + asyncpg pool.
- Fault isolation: node-level try/except prevents global crash.
- Performance: per-node and end-to-end timers.
- Traceability: request-scoped logging (`req=<id>`).
