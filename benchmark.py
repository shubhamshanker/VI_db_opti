import argparse
import asyncio
import logging
import statistics
import time
from rich.panel import Panel
from rich.table import Table

from main import DataFederator, _new_request_id, get_console, setup_output


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    idx = max(0, min(len(values) - 1, int(round((pct / 100.0) * (len(values) - 1)))))
    return sorted(values)[idx]


async def run_benchmark(runs: int, circles: list[str]) -> None:
    federator = DataFederator()
    samples_ms: list[float] = []
    total_rows_seen = 0
    fanouts: list[int] = []
    merge_times: list[float] = []
    failed_nodes_total = 0

    try:
        await federator.initialize_pools()

        for i in range(runs):
            req = _new_request_id()
            t0 = time.perf_counter()
            response = await federator.execute_federated_query_detailed(
                circles,
                request_id=req,
                include_rows=False,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            samples_ms.append(elapsed_ms)
            row_count = response["totals"]["total_rows"]
            total_rows_seen += row_count
            fanouts.append(response["query_plan"]["expected_fanout"])
            merge_times.append(response["consolidation"]["merge_ms"])
            failed_nodes_total += response["totals"]["nodes_failed"]
            logging.info(
                "BENCH_RUN run=%d req=%s rows=%d fanout=%d failed_nodes=%d elapsed_ms=%.2f",
                i + 1,
                req,
                row_count,
                response["query_plan"]["expected_fanout"],
                response["totals"]["nodes_failed"],
                elapsed_ms,
            )
    finally:
        await federator.close_pools()

    p50 = percentile(samples_ms, 50)
    p95 = percentile(samples_ms, 95)
    p99 = percentile(samples_ms, 99)
    avg = statistics.mean(samples_ms) if samples_ms else 0.0

    console = get_console()
    summary_table = Table(title="Benchmark Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Metric")
    summary_table.add_column("Value", justify="right")
    summary_table.add_row("runs", str(runs))
    summary_table.add_row("circles", str(circles))
    summary_table.add_row("min_ms", f"{min(samples_ms):.2f}")
    summary_table.add_row("avg_ms", f"{avg:.2f}")
    summary_table.add_row("p50_ms", f"{p50:.2f}")
    summary_table.add_row("p95_ms", f"{p95:.2f}")
    summary_table.add_row("p99_ms", f"{p99:.2f}")
    summary_table.add_row("max_ms", f"{max(samples_ms):.2f}")
    summary_table.add_row("total_rows_seen", str(total_rows_seen))
    summary_table.add_row("avg_fanout_nodes", f"{statistics.mean(fanouts):.2f}")
    summary_table.add_row("avg_merge_ms", f"{statistics.mean(merge_times):.2f}")
    summary_table.add_row("total_failed_nodes", str(failed_nodes_total))
    console.print(summary_table)

    lines = [
        f"- Completed {runs} end-to-end federated requests.",
        f"- Average split fanout: {statistics.mean(fanouts):.2f} database nodes per request.",
        f"- Typical response time (p50): {p50:.2f} ms.",
        f"- Tail response time (p95): {p95:.2f} ms.",
        f"- Worst observed time (p99): {p99:.2f} ms.",
        f"- Average consolidation time: {statistics.mean(merge_times):.2f} ms.",
        f"- Failed nodes observed: {failed_nodes_total}.",
    ]
    if p95 <= 50:
        lines.append("- Status: Excellent for local smoke baseline.")
    elif p95 <= 150:
        lines.append("- Status: Good, suitable for initial demo.")
    elif p95 <= 300:
        lines.append("- Status: Acceptable, monitor before production.")
    else:
        lines.append("- Status: Needs optimization before sign-off.")

    console.print(Panel("\n".join(lines), title="Manager Interpretation", border_style="green"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local federated-query benchmark.")
    parser.add_argument("--runs", type=int, default=20, help="Number of benchmark iterations.")
    parser.add_argument(
        "--circles",
        nargs="+",
        default=["circle_1", "circle_7", "circle_12"],
        help="Circle IDs to query on each run.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colorized rich output for plain terminals/CI.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_output(no_color=args.no_color)
    asyncio.run(run_benchmark(args.runs, args.circles))
