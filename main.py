import asyncio
import asyncpg
import logging
import time
import uuid
from urllib.parse import urlparse
from typing import List, Dict
from dataclasses import dataclass

# --- Configuration & Metadata ---
DB_CONFIGS = {
    "db_node_1": "postgresql://user:pass@localhost:5432/db1",
    "db_node_2": "postgresql://user:pass@localhost:5432/db2",
    "db_node_3": "postgresql://user:pass@localhost:5432/db3",
    "db_node_4": "postgresql://user:pass@localhost:5432/db4",
}

# Mapping: circle_id -> database_node_id
CIRCLE_ROUTING_TABLE = {
    **{f"circle_{i}": "db_node_1" for i in range(1, 6)},
    **{f"circle_{i}": "db_node_2" for i in range(6, 11)},
    **{f"circle_{i}": "db_node_3" for i in range(11, 16)},
    **{f"circle_{i}": "db_node_4" for i in range(16, 21)},
}

@dataclass
class QueryTask:
    node_id: str
    circles: List[str]


def _dsn_database_name(dsn: str) -> str:
    return urlparse(dsn).path.lstrip("/") or "unknown_db"


def _new_request_id() -> str:
    return uuid.uuid4().hex[:8]


class DataFederator:
    def __init__(self):
        self.pools: Dict[str, asyncpg.Pool] = {}

    async def initialize_pools(self):
        """Initialize high-performance connection pools for all nodes."""
        init_start = time.perf_counter()
        logging.info("POOL_INIT_START total_nodes=%d", len(DB_CONFIGS))
        for node_id, dsn in DB_CONFIGS.items():
            db_name = _dsn_database_name(dsn)
            node_start = time.perf_counter()
            logging.info("POOL_INIT_NODE_START node=%s db=%s", node_id, db_name)
            # max_size should be tuned based on DB CPU/Memory
            self.pools[node_id] = await asyncpg.create_pool(
                dsn, min_size=10, max_size=50, command_timeout=5.0
            )
            node_ms = (time.perf_counter() - node_start) * 1000
            logging.info(
                "POOL_INIT_NODE_DONE node=%s db=%s elapsed_ms=%.2f",
                node_id,
                db_name,
                node_ms,
            )
        total_ms = (time.perf_counter() - init_start) * 1000
        logging.info("POOL_INIT_DONE total_nodes=%d elapsed_ms=%.2f", len(self.pools), total_ms)

    async def close_pools(self):
        if not self.pools:
            return
        close_start = time.perf_counter()
        await asyncio.gather(*(pool.close() for pool in self.pools.values()))
        close_ms = (time.perf_counter() - close_start) * 1000
        logging.info("POOL_CLOSE_DONE total_nodes=%d elapsed_ms=%.2f", len(self.pools), close_ms)

    def _route_circles(self, circles: List[str], request_id: str) -> List[QueryTask]:
        """Groups circles by their target DB node to minimize network round-trips."""
        mapping: Dict[str, List[str]] = {}
        missing: List[str] = []
        for c in circles:
            node = CIRCLE_ROUTING_TABLE.get(c)
            if node:
                mapping.setdefault(node, []).append(c)
            else:
                missing.append(c)
        if missing:
            logging.warning("ROUTING_MISSING req=%s circles=%s", request_id, missing)
        logging.info(
            "ROUTING_DONE req=%s input_circles=%d target_nodes=%d",
            request_id,
            len(circles),
            len(mapping),
        )
        for node_id, routed_circles in mapping.items():
            logging.info(
                "ROUTING_NODE req=%s node=%s circles=%s",
                request_id,
                node_id,
                routed_circles,
            )
        return [QueryTask(node, circs) for node, circs in mapping.items()]

    async def _fetch_from_node(self, task: QueryTask, sql_template: str, request_id: str) -> List[Dict]:
        """Executes the query on a specific node with error isolation."""
        pool = self.pools.get(task.node_id)
        if not pool:
            logging.error("NODE_QUERY_NO_POOL req=%s node=%s", request_id, task.node_id)
            return []

        node_start = time.perf_counter()
        logging.info(
            "NODE_QUERY_START req=%s node=%s circles=%s circle_count=%d",
            request_id,
            task.node_id,
            task.circles,
            len(task.circles),
        )
        try:
            async with pool.acquire() as conn:
                # Use binary protocol for speed; fetch() is faster than fetchrow()
                results = await conn.fetch(sql_template, task.circles)
                node_ms = (time.perf_counter() - node_start) * 1000
                rows = [dict(r) for r in results]
                logging.info(
                    "NODE_QUERY_DONE req=%s node=%s rows=%d elapsed_ms=%.2f",
                    request_id,
                    task.node_id,
                    len(rows),
                    node_ms,
                )
                return rows
        except Exception as e:
            node_ms = (time.perf_counter() - node_start) * 1000
            logging.exception(
                "NODE_QUERY_FAIL req=%s node=%s elapsed_ms=%.2f error=%s",
                request_id,
                task.node_id,
                node_ms,
                e,
            )
            return []  # Or raise, depending on your 'Partial Results' policy

    async def execute_federated_query(self, target_circles: List[str], request_id: str | None = None):
        """The core Scatter-Gather orchestrator."""
        request_id = request_id or _new_request_id()
        total_start = time.perf_counter()
        logging.info("FED_QUERY_START req=%s target_circles=%s", request_id, target_circles)

        # 1. Routing
        tasks = self._route_circles(target_circles, request_id)
        if not tasks:
            logging.warning("FED_QUERY_NO_TASKS req=%s target_circles=%s", request_id, target_circles)
            return []

        # 2. Parallel Dispatch (Scatter)
        sql = "SELECT * FROM analytics_table WHERE circle_id = ANY($1)"
        execution_coros = [self._fetch_from_node(t, sql, request_id) for t in tasks]
        logging.info("FED_QUERY_SCATTER req=%s node_count=%d", request_id, len(tasks))

        # 3. Consolidation (Gather)
        # asyncio.gather fires them all at once on the event loop
        results_nested = await asyncio.gather(*execution_coros)

        # Flatten and return (Consolidation)
        flattened = [item for sublist in results_nested for item in sublist]
        total_ms = (time.perf_counter() - total_start) * 1000
        logging.info(
            "FED_QUERY_DONE req=%s node_count=%d total_rows=%d elapsed_ms=%.2f",
            request_id,
            len(tasks),
            len(flattened),
            total_ms,
        )
        return flattened

# --- Usage / Entry Point ---
async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    federator = DataFederator()
    try:
        await federator.initialize_pools()

        # Example: query circles that route to 3 distinct DB nodes
        circles_to_query = ["circle_1", "circle_7", "circle_12"]
        request_id = _new_request_id()
        app_start = time.perf_counter()
        data = await federator.execute_federated_query(circles_to_query, request_id=request_id)
        app_ms = (time.perf_counter() - app_start) * 1000

        logging.info("APP_RESULT req=%s row_count=%d elapsed_ms=%.2f", request_id, len(data), app_ms)
        if data:
            logging.info("APP_SAMPLE_ROW req=%s sample=%s", request_id, data[0])
        print(f"Retrieved {len(data)} records across federated nodes.")
    finally:
        await federator.close_pools()

if __name__ == "__main__":
    asyncio.run(main())