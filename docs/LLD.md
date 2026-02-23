# Low-Level Design (LLD)

## Module: `main.py`

### Config
- `NODE_CONFIGS`: node DSNs defined for `db_node_1..db_node_6`.
- `ACTIVE_DB_NODES` env var controls active nodes at runtime (scale by config only).
- Routing model:
  - exact: `DEVICE_CIRCLE_CLUSTER_ROUTING`
  - fallback: `CIRCLE_ROUTING_TABLE` -> `CLUSTER_ROUTING_TABLE` -> `DEVICE_ROUTING_TABLE`

### Data Structures
- `QueryTask(node_id: str, circles: List[str])`: unit of routed work.
- `QueryInput(input_id, device_id, circle_id, cluster_id)`: normalized inbound routing payload.

### Core Methods
- `initialize_pools()`
  - Creates asyncpg pools per node.
  - Logs pool init start/end and per-node init time.
- `_route_circles(circles, request_id)`
  - Replaced by `_route_query_inputs(query_inputs, request_id)`.
  - Resolves each input with route reason (`matched_by`) and groups by node.
  - Returns route details and unresolved circles.
- `_fetch_from_node(task, sql, request_id)`
  - Executes parameterized query: `SELECT * FROM analytics_table WHERE circle_id = ANY($1)`.
  - Logs SQL template and bind values per node.
  - Returns node execution metadata (`executed_sql`, `bind_values`, `row_count`, `elapsed_ms`).
- `execute_federated_query(target_circles, request_id=None)`
  - Creates request ID if absent.
  - Performs routing -> scatter -> gather -> flatten.
  - Logs federated start/end and total elapsed ms.
- `execute_federated_query_detailed(target_circles, request_id=None, include_rows=True)`
  - Returns API-friendly structured response:
    - `request_id`, `input`, `routing`, `query_plan`, `node_results`, `totals`, `timings`, `consolidation`, `rows`.
  - Preserves technical logs for engineering debugging.
  - Emits explicit split-plan and consolidation logs (`SPLIT_PLAN`, `NODE_PLAN`, `CONSOLIDATION_DONE`).
- `close_pools()`
  - Closes all pools and logs total close time.

### Manager-Friendly Output
- `build_manager_summary(response)`
  - Converts technical response into plain-English summary.
  - Explicitly describes:
    - what was asked
    - how it was split
    - what executed on each node
    - how it was consolidated
    - why asyncio/pool are used

### Local HTTP API
- `FederatorHttpHandler` (`main.py`)
  - `GET /health` -> service status.
  - `POST /query` with JSON body:
    - `{"circles":["circle_1","circle_7","circle_12"]}` or
    - `{"query_items":[{"input_id":"q1","device_id":"device_a","circle_id":"circle_1","cluster_id":"cluster_red"}]}`
  - Response contains both:
    - `technical` (full structured payload)
    - `manager_summary` (plain-English text)
    - `manager_view` (short manager-focused object)
- Run server:
  - `python main.py --mode server --host 127.0.0.1 --port 8080`

## Module: `benchmark.py`
- Runs N federated queries.
- Emits per-run logs and aggregate stats: `min/avg/p50/p95/p99/max`.
- Adds plain-language interpretation block for non-technical review.
- Reports split/consolidation clarity metrics:
  - `avg_fanout_nodes`, `avg_merge_ms`, `total_failed_nodes`
- CLI:
  - `--runs` (default 20)
  - `--circles` (default `circle_1 circle_7 circle_12`)
