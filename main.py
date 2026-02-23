import asyncio
import asyncpg
import json
import logging
import time
import uuid
import argparse
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from typing import List, Dict, Any
from dataclasses import dataclass
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# --- Configuration & Metadata ---
NODE_CONFIGS = {
    "db_node_1": "postgresql://user:pass@localhost:5432/db1",
    "db_node_2": "postgresql://user:pass@localhost:5432/db2",
    "db_node_3": "postgresql://user:pass@localhost:5432/db3",
    "db_node_4": "postgresql://user:pass@localhost:5432/db4",
    "db_node_5": "postgresql://user:pass@localhost:5432/db5",
    "db_node_6": "postgresql://user:pass@localhost:5432/db6",
}

def _get_active_nodes() -> List[str]:
    configured = os.getenv("ACTIVE_DB_NODES", "db_node_1,db_node_2,db_node_3,db_node_4")
    requested = [node.strip() for node in configured.split(",") if node.strip()]
    return [node for node in requested if node in NODE_CONFIGS]


ACTIVE_NODE_IDS = _get_active_nodes()
DB_CONFIGS = {node: NODE_CONFIGS[node] for node in ACTIVE_NODE_IDS}

# Primary deterministic circle ownership map (now scale-ready to 6 nodes).
CIRCLE_ROUTING_TABLE = {
    **{f"circle_{i}": "db_node_1" for i in range(1, 6)},
    **{f"circle_{i}": "db_node_2" for i in range(6, 11)},
    **{f"circle_{i}": "db_node_3" for i in range(11, 16)},
    **{f"circle_{i}": "db_node_4" for i in range(16, 21)},
    **{f"circle_{i}": "db_node_5" for i in range(21, 26)},
    **{f"circle_{i}": "db_node_6" for i in range(26, 31)},
}

# Optional route hints and fallback maps.
CLUSTER_ROUTING_TABLE = {
    "cluster_red": "db_node_1",
    "cluster_blue": "db_node_2",
    "cluster_green": "db_node_3",
    "cluster_yellow": "db_node_4",
    "cluster_violet": "db_node_5",
    "cluster_orange": "db_node_6",
}

DEVICE_ROUTING_TABLE = {
    "device_a": "db_node_1",
    "device_b": "db_node_2",
    "device_c": "db_node_3",
    "device_d": "db_node_4",
}

# Exact high-priority routing rules.
DEVICE_CIRCLE_CLUSTER_ROUTING = {
    ("device_a", "circle_1", "cluster_red"): "db_node_1",
    ("device_b", "circle_7", "cluster_blue"): "db_node_2",
    ("device_c", "circle_12", "cluster_green"): "db_node_3",
    ("device_d", "circle_17", "cluster_yellow"): "db_node_4",
}

@dataclass
class QueryTask:
    node_id: str
    circles: List[str]


@dataclass
class QueryInput:
    input_id: str
    device_id: str | None = None
    circle_id: str | None = None
    cluster_id: str | None = None


def _dsn_database_name(dsn: str) -> str:
    return urlparse(dsn).path.lstrip("/") or "unknown_db"


def _new_request_id() -> str:
    return uuid.uuid4().hex[:8]


CONSOLE = Console()


def get_console() -> Console:
    return CONSOLE


def setup_output(no_color: bool = False) -> None:
    global CONSOLE
    CONSOLE = Console(no_color=no_color)
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[
            RichHandler(
                console=CONSOLE,
                show_time=True,
                show_level=True,
                show_path=False,
                markup=True,
            )
        ],
        force=True,
    )


def _normalize_query_inputs(
    circles: List[str] | None = None,
    query_items: List[Dict[str, Any]] | None = None,
) -> List[QueryInput]:
    if query_items:
        normalized: List[QueryInput] = []
        for idx, item in enumerate(query_items, start=1):
            normalized.append(
                QueryInput(
                    input_id=item.get("input_id") or f"q{idx}",
                    device_id=item.get("device_id"),
                    circle_id=item.get("circle_id"),
                    cluster_id=item.get("cluster_id"),
                )
            )
        return normalized

    circles = circles or []
    return [QueryInput(input_id=f"c{idx}", circle_id=c) for idx, c in enumerate(circles, start=1)]


class DataFederator:
    def __init__(self):
        self.pools: Dict[str, asyncpg.Pool] = {}

    async def initialize_pools(self):
        """Initialize high-performance connection pools for all nodes."""
        init_start = time.perf_counter()
        logging.info("POOL_INIT_START total_nodes=%d active_nodes=%s", len(DB_CONFIGS), list(DB_CONFIGS.keys()))
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

    def _resolve_node_for_input(self, query_input: QueryInput) -> tuple[str | None, str]:
        """Resolve route using exact map then progressive fallbacks."""
        device_id = query_input.device_id
        circle_id = query_input.circle_id
        cluster_id = query_input.cluster_id

        if device_id and circle_id and cluster_id:
            exact = DEVICE_CIRCLE_CLUSTER_ROUTING.get((device_id, circle_id, cluster_id))
            if exact and exact in DB_CONFIGS:
                return exact, "device+circle+cluster"

        if circle_id:
            node = CIRCLE_ROUTING_TABLE.get(circle_id)
            if node and node in DB_CONFIGS:
                return node, "circle_fallback"

        if cluster_id:
            node = CLUSTER_ROUTING_TABLE.get(cluster_id)
            if node and node in DB_CONFIGS:
                return node, "cluster_fallback"

        if device_id:
            node = DEVICE_ROUTING_TABLE.get(device_id)
            if node and node in DB_CONFIGS:
                return node, "device_fallback"

        return None, "unresolved"

    def _route_query_inputs(
        self,
        query_inputs: List[QueryInput],
        request_id: str,
    ) -> tuple[List[QueryTask], List[Dict[str, Any]], List[str]]:
        """Build per-node tasks and route diagnostics for split-plan transparency."""
        mapping: Dict[str, List[str]] = {}
        route_details: List[Dict[str, Any]] = []
        missing_circles: List[str] = []

        for query_input in query_inputs:
            node, matched_by = self._resolve_node_for_input(query_input)
            has_circle = bool(query_input.circle_id)
            route_record = {
                "input_id": query_input.input_id,
                "device_id": query_input.device_id,
                "circle_id": query_input.circle_id,
                "cluster_id": query_input.cluster_id,
                "target_node": node,
                "matched_by": matched_by,
            }
            route_details.append(route_record)

            if not has_circle:
                logging.warning(
                    "ROUTING_SKIP_NO_CIRCLE req=%s input_id=%s matched_by=%s",
                    request_id,
                    query_input.input_id,
                    matched_by,
                )
                continue

            if not node:
                missing_circles.append(query_input.circle_id or "unknown_circle")
                continue

            mapping.setdefault(node, []).append(query_input.circle_id or "")

        if missing_circles:
            logging.warning("ROUTING_MISSING req=%s circles=%s", request_id, missing_circles)

        logging.info(
            "ROUTING_DONE req=%s total_inputs=%d target_nodes=%d",
            request_id,
            len(query_inputs),
            len(mapping),
        )
        for node_id, routed_circles in mapping.items():
            logging.info(
                "ROUTING_NODE req=%s node=%s circles=%s",
                request_id,
                node_id,
                routed_circles,
            )
        return [QueryTask(node, circs) for node, circs in mapping.items()], route_details, missing_circles

    async def _fetch_from_node(
        self,
        task: QueryTask,
        sql_template: str,
        request_id: str,
        include_rows: bool = True,
    ) -> Dict[str, Any]:
        """Executes the query on a specific node with error isolation."""
        pool = self.pools.get(task.node_id)
        db_name = _dsn_database_name(DB_CONFIGS.get(task.node_id, ""))
        if not pool:
            logging.error("NODE_QUERY_NO_POOL req=%s node=%s", request_id, task.node_id)
            return {
                "node_id": task.node_id,
                "database": db_name,
                "circles": task.circles,
                "status": "failed",
                "row_count": 0,
                "elapsed_ms": 0.0,
                "error": "Pool not initialized",
                "rows": [],
            }

        node_start = time.perf_counter()
        logging.info(
            "NODE_QUERY_START req=%s node=%s circles=%s circle_count=%d sql=%s bind_values=%s",
            request_id,
            task.node_id,
            task.circles,
            len(task.circles),
            sql_template,
            {"$1": task.circles},
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
                result: Dict[str, Any] = {
                    "node_id": task.node_id,
                    "database": db_name,
                    "circles": task.circles,
                    "status": "ok",
                    "row_count": len(rows),
                    "elapsed_ms": round(node_ms, 2),
                    "error": None,
                    "executed_sql": sql_template,
                    "bind_values": {"$1": task.circles},
                }
                result["rows"] = rows if include_rows else []
                return result
        except Exception as e:
            node_ms = (time.perf_counter() - node_start) * 1000
            logging.exception(
                "NODE_QUERY_FAIL req=%s node=%s elapsed_ms=%.2f error=%s",
                request_id,
                task.node_id,
                node_ms,
                e,
            )
            return {
                "node_id": task.node_id,
                "database": db_name,
                "circles": task.circles,
                "status": "failed",
                "row_count": 0,
                "elapsed_ms": round(node_ms, 2),
                "error": str(e),
                "executed_sql": sql_template,
                "bind_values": {"$1": task.circles},
                "rows": [],
            }

    async def execute_federated_query_detailed(
        self,
        target_circles: List[str] | None = None,
        request_id: str | None = None,
        include_rows: bool = True,
        query_items: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        """The core Scatter-Gather orchestrator with structured API response."""
        request_id = request_id or _new_request_id()
        total_start = time.perf_counter()
        normalized_inputs = _normalize_query_inputs(circles=target_circles, query_items=query_items)
        logging.info(
            "FED_QUERY_START req=%s input_count=%d target_circles=%s",
            request_id,
            len(normalized_inputs),
            target_circles or [],
        )

        # 1. Routing
        tasks, route_details, missing = self._route_query_inputs(normalized_inputs, request_id)
        routing_map = {task.node_id: task.circles for task in tasks}
        if not tasks:
            logging.warning("FED_QUERY_NO_TASKS req=%s target_circles=%s", request_id, target_circles)
            return {
                "request_id": request_id,
                "input": {
                    "target_circles": target_circles or [],
                    "query_items": [vars(item) for item in normalized_inputs],
                    "input_count": len(normalized_inputs),
                },
                "routing": {
                    "target_nodes": 0,
                    "mapping": {},
                    "route_details": route_details,
                    "missing_circles": missing,
                },
                "query_plan": {
                    "sql_template": "SELECT * FROM analytics_table WHERE circle_id = ANY($1)",
                    "expected_fanout": 0,
                    "node_plans": [],
                },
                "node_results": [],
                "totals": {
                    "nodes_queried": 0,
                    "nodes_successful": 0,
                    "nodes_failed": 0,
                    "total_rows": 0,
                },
                "timings": {
                    "end_to_end_ms": round((time.perf_counter() - total_start) * 1000, 2),
                },
                "consolidation": {
                    "merged_rows": 0,
                    "merge_ms": 0.0,
                    "nodes_successful": 0,
                    "nodes_failed": 0,
                },
                "rows": [],
            }

        # 2. Parallel Dispatch (Scatter)
        sql = "SELECT * FROM analytics_table WHERE circle_id = ANY($1)"
        query_plan = {
            "sql_template": sql,
            "expected_fanout": len(tasks),
            "node_plans": [
                {
                    "node_id": task.node_id,
                    "database": _dsn_database_name(DB_CONFIGS.get(task.node_id, "")),
                    "circle_bind_values": task.circles,
                    "bind_values": {"$1": task.circles},
                }
                for task in tasks
            ],
        }
        logging.info(
            "SPLIT_PLAN req=%s expected_fanout=%d sql_template=%s",
            request_id,
            query_plan["expected_fanout"],
            sql,
        )
        for node_plan in query_plan["node_plans"]:
            logging.info(
                "NODE_PLAN req=%s node=%s bind_values=%s",
                request_id,
                node_plan["node_id"],
                node_plan["bind_values"],
            )
        execution_coros = [self._fetch_from_node(t, sql, request_id, include_rows=include_rows) for t in tasks]
        logging.info("FED_QUERY_SCATTER req=%s node_count=%d", request_id, len(tasks))

        # 3. Consolidation (Gather)
        # asyncio.gather fires them all at once on the event loop
        results_nested = await asyncio.gather(*execution_coros)

        # Flatten and return (Consolidation)
        merge_start = time.perf_counter()
        flattened = [row for node in results_nested for row in node.get("rows", [])]
        nodes_successful = sum(1 for node in results_nested if node.get("status") == "ok")
        nodes_failed = len(results_nested) - nodes_successful
        total_rows = sum(node.get("row_count", 0) for node in results_nested)
        merge_ms = (time.perf_counter() - merge_start) * 1000
        total_ms = (time.perf_counter() - total_start) * 1000
        consolidation = {
            "merged_rows": total_rows,
            "merge_ms": round(merge_ms, 2),
            "nodes_successful": nodes_successful,
            "nodes_failed": nodes_failed,
            "node_row_counts": {node["node_id"]: node["row_count"] for node in results_nested},
        }
        logging.info(
            "CONSOLIDATION_DONE req=%s merged_rows=%d success_nodes=%d failed_nodes=%d merge_ms=%.2f",
            request_id,
            consolidation["merged_rows"],
            nodes_successful,
            nodes_failed,
            merge_ms,
        )
        logging.info(
            "FED_QUERY_DONE req=%s node_count=%d total_rows=%d failed_nodes=%d elapsed_ms=%.2f",
            request_id,
            len(tasks),
            total_rows,
            nodes_failed,
            total_ms,
        )
        return {
            "request_id": request_id,
            "input": {
                "target_circles": target_circles or [item.circle_id for item in normalized_inputs if item.circle_id],
                "query_items": [vars(item) for item in normalized_inputs],
                "input_count": len(normalized_inputs),
            },
            "routing": {
                "target_nodes": len(tasks),
                "mapping": routing_map,
                "route_details": route_details,
                "missing_circles": missing,
            },
            "query_plan": query_plan,
            "node_results": results_nested,
            "totals": {
                "nodes_queried": len(tasks),
                "nodes_successful": nodes_successful,
                "nodes_failed": nodes_failed,
                "total_rows": total_rows,
            },
            "timings": {
                "end_to_end_ms": round(total_ms, 2),
            },
            "consolidation": consolidation,
            "rows": flattened if include_rows else [],
        }

    async def execute_federated_query(self, target_circles: List[str], request_id: str | None = None) -> List[Dict]:
        """
        Backward-compatible method returning merged rows only.
        Use execute_federated_query_detailed for API and manager views.
        """
        detailed = await self.execute_federated_query_detailed(
            target_circles=target_circles,
            request_id=request_id,
            include_rows=True,
        )
        return detailed["rows"]


def build_manager_summary(response: Dict[str, Any]) -> str:
    req = response.get("request_id", "unknown")
    input_part = response.get("input", {})
    routing = response.get("routing", {})
    query_plan = response.get("query_plan", {})
    totals = response.get("totals", {})
    consolidation = response.get("consolidation", {})
    timings = response.get("timings", {})
    node_results = response.get("node_results", [])

    lines = [
        "REQUEST OVERVIEW",
        f"- request_id: {req}",
        f"- inputs: {input_part.get('input_count', 0)}",
        f"- circles: {input_part.get('target_circles', [])}",
        "",
        "QUERY SPLIT PLAN",
        f"- fanout_nodes: {query_plan.get('expected_fanout', 0)}",
        f"- sql: {query_plan.get('sql_template', 'n/a')}",
    ]

    for node_plan in query_plan.get("node_plans", []):
        lines.append(
            f"- {node_plan['node_id']} ({node_plan['database']}), bind_values={node_plan['bind_values']}"
        )

    lines.extend(
        [
            "",
            "EXECUTION & CONSOLIDATION",
            (
                f"- total_rows: {totals.get('total_rows', 0)}, "
                f"success_nodes: {totals.get('nodes_successful', 0)}, "
                f"failed_nodes: {totals.get('nodes_failed', 0)}"
            ),
            (
                f"- merged_rows: {consolidation.get('merged_rows', 0)}, "
                f"merge_ms: {consolidation.get('merge_ms', 0)}"
            ),
            f"- end_to_end_ms: {timings.get('end_to_end_ms', 0)}",
        ]
    )

    missing = routing.get("missing_circles", [])
    if missing:
        lines.append(f"- unresolved_circles={missing}")

    lines.append("")
    lines.append("PER-NODE OUTCOMES")
    for node in node_results:
        line = (
            f"- {node.get('node_id')} ({node.get('database')}): "
            f"{node.get('status')}, rows={node.get('row_count')}, ms={node.get('elapsed_ms')}"
        )
        if node.get("error"):
            line += f", error={node.get('error')}"
        lines.append(line)

    lines.extend(
        [
            "",
            "WHY ASYNCIO + POOL",
            "- asyncio runs node queries in parallel so total time is near the slowest node, not sum of all nodes.",
            "- asyncpg pool reuses DB connections to avoid reconnect overhead on every request.",
        ]
    )
    return "\n".join(lines)


def _render_rich_manager_view(response: Dict[str, Any]) -> None:
    console = get_console()
    input_part = response.get("input", {})
    query_plan = response.get("query_plan", {})
    totals = response.get("totals", {})
    consolidation = response.get("consolidation", {})
    timings = response.get("timings", {})
    node_results = response.get("node_results", [])

    header = Text()
    header.append("Request ", style="bold cyan")
    header.append(response.get("request_id", "unknown"), style="bold white")
    header.append(" | ", style="dim")
    header.append(f"inputs={input_part.get('input_count', 0)} ", style="green")
    header.append(f"fanout={query_plan.get('expected_fanout', 0)} ", style="magenta")
    header.append(f"e2e={timings.get('end_to_end_ms', 0)}ms", style="yellow")
    console.print(Panel(header, title="Federated Flow", border_style="cyan"))

    split_table = Table(title="Query Split Plan", show_header=True, header_style="bold blue")
    split_table.add_column("Node")
    split_table.add_column("Database")
    split_table.add_column("Bind Values")
    for node_plan in query_plan.get("node_plans", []):
        split_table.add_row(
            node_plan.get("node_id", "n/a"),
            node_plan.get("database", "n/a"),
            str(node_plan.get("bind_values", {})),
        )
    console.print(split_table)

    node_table = Table(title="Per-Node Execution", show_header=True, header_style="bold green")
    node_table.add_column("Node")
    node_table.add_column("Status")
    node_table.add_column("Rows")
    node_table.add_column("Latency(ms)")
    for node in node_results:
        status = node.get("status", "unknown")
        style = "green" if status == "ok" else "red"
        node_table.add_row(
            node.get("node_id", "n/a"),
            f"[{style}]{status}[/{style}]",
            str(node.get("row_count", 0)),
            str(node.get("elapsed_ms", 0)),
        )
    console.print(node_table)

    consolidation_text = Text()
    consolidation_text.append(
        f"rows={totals.get('total_rows', 0)} | "
        f"success_nodes={totals.get('nodes_successful', 0)} | "
        f"failed_nodes={totals.get('nodes_failed', 0)}\n",
        style="bold",
    )
    consolidation_text.append(
        f"merged_rows={consolidation.get('merged_rows', 0)} | "
        f"merge_ms={consolidation.get('merge_ms', 0)} | "
        f"end_to_end_ms={timings.get('end_to_end_ms', 0)}",
        style="yellow",
    )
    console.print(Panel(consolidation_text, title="Consolidation", border_style="green"))


async def _run_single_query_for_api(
    target_circles: List[str] | None = None,
    query_items: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    federator = DataFederator()
    try:
        await federator.initialize_pools()
        request_id = _new_request_id()
        response = await federator.execute_federated_query_detailed(
            target_circles=target_circles,
            request_id=request_id,
            include_rows=True,
            query_items=query_items,
        )
        return response
    finally:
        await federator.close_pools()


class FederatorHttpHandler(BaseHTTPRequestHandler):
    server_version = "FederatorHTTP/1.0"

    def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "service": "federator"})
            return
        self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        if self.path != "/query":
            self._send_json(404, {"error": "Not found"})
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            payload = json.loads(body.decode("utf-8"))
            circles = payload.get("circles")
            query_items = payload.get("query_items")

            if circles is None and query_items is None:
                self._send_json(400, {"error": "Provide either 'circles' or 'query_items'"})
                return

            if circles is not None and (not isinstance(circles, list) or not all(isinstance(c, str) for c in circles)):
                self._send_json(400, {"error": "'circles' must be a list of strings"})
                return

            if query_items is not None and not isinstance(query_items, list):
                self._send_json(400, {"error": "'query_items' must be a list of objects"})
                return

            response = asyncio.run(
                _run_single_query_for_api(
                    target_circles=circles,
                    query_items=query_items,
                )
            )
            summary = build_manager_summary(response)
            self._send_json(
                200,
                {
                    "status": "ok",
                    "technical": response,
                    "manager_summary": summary,
                    "manager_view": {
                        "what_was_asked": response["input"],
                        "where_queried": response["routing"]["mapping"],
                        "query_split": response["query_plan"]["node_plans"],
                        "consolidation": response["consolidation"],
                        "overall_ms": response["timings"]["end_to_end_ms"],
                    },
                },
            )
        except Exception as e:
            logging.exception("API_QUERY_FAIL error=%s", e)
            self._send_json(500, {"error": "Internal server error", "details": str(e)})

    def log_message(self, fmt: str, *args: Any) -> None:
        logging.info("API_ACCESS " + fmt, *args)


def run_http_server(host: str, port: int) -> None:
    httpd = ThreadingHTTPServer((host, port), FederatorHttpHandler)
    logging.info("API_SERVER_START host=%s port=%d", host, port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("API_SERVER_STOP requested_by_user=true")
    finally:
        httpd.server_close()

# --- Usage / Entry Point ---
async def main():
    federator = DataFederator()
    try:
        await federator.initialize_pools()

        # Demo request: exact + fallback routes.
        demo_query_items = [
            {"input_id": "exact_match", "device_id": "device_a", "circle_id": "circle_1", "cluster_id": "cluster_red"},
            {"input_id": "circle_fallback", "device_id": "device_unknown", "circle_id": "circle_7", "cluster_id": "cluster_unknown"},
            {"input_id": "cluster_fallback", "device_id": "device_unknown", "circle_id": "circle_12", "cluster_id": "cluster_green"},
        ]
        request_id = _new_request_id()
        app_start = time.perf_counter()
        result = await federator.execute_federated_query_detailed(
            request_id=request_id,
            query_items=demo_query_items,
        )
        app_ms = (time.perf_counter() - app_start) * 1000

        logging.info(
            "APP_RESULT req=%s row_count=%d elapsed_ms=%.2f",
            request_id,
            result["totals"]["total_rows"],
            app_ms,
        )
        if result["rows"]:
            logging.info("APP_SAMPLE_ROW req=%s sample=%s", request_id, result["rows"][0])

        _render_rich_manager_view(result)
        get_console().print(
            Panel(
                build_manager_summary(result),
                title="Manager Summary (API-safe text)",
                border_style="blue",
            )
        )
    finally:
        await federator.close_pools()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Federated query runner and local API server.")
    parser.add_argument(
        "--mode",
        choices=["run", "server"],
        default="run",
        help="run: execute one sample query, server: start HTTP API",
    )
    parser.add_argument("--host", default="127.0.0.1", help="HTTP server host (server mode).")
    parser.add_argument("--port", type=int, default=8080, help="HTTP server port (server mode).")
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colorized rich output for plain terminals/CI.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_output(no_color=args.no_color)
    if args.mode == "server":
        run_http_server(args.host, args.port)
    else:
        asyncio.run(main())