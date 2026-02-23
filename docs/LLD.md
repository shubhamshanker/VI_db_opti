# Low-Level Design (LLD)

## Module: `main.py`

### Config
- `DB_CONFIGS`: 4 logical nodes (`db_node_1..4`) mapped to local DBs (`db1..4`) on `localhost:5432`.
- `CIRCLE_ROUTING_TABLE`: deterministic ownership by circle range.

### Data Structures
- `QueryTask(node_id: str, circles: List[str])`: unit of routed work.

### Core Methods
- `initialize_pools()`
  - Creates asyncpg pools per node.
  - Logs pool init start/end and per-node init time.
- `_route_circles(circles, request_id)`
  - Groups circles by owner node.
  - Logs routed and missing circles.
- `_fetch_from_node(task, sql, request_id)`
  - Executes parameterized query: `SELECT * FROM analytics_table WHERE circle_id = ANY($1)`.
  - Logs node start/end, row count, elapsed ms, and failures.
- `execute_federated_query(target_circles, request_id=None)`
  - Creates request ID if absent.
  - Performs routing -> scatter -> gather -> flatten.
  - Logs federated start/end and total elapsed ms.
- `close_pools()`
  - Closes all pools and logs total close time.

## Module: `benchmark.py`
- Runs N federated queries.
- Emits per-run logs and aggregate stats: `min/avg/p50/p95/p99/max`.
- CLI:
  - `--runs` (default 20)
  - `--circles` (default `circle_1 circle_7 circle_12`)
