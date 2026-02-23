import argparse
import asyncio
import logging
import statistics
import time

from main import DataFederator, _new_request_id


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    idx = max(0, min(len(values) - 1, int(round((pct / 100.0) * (len(values) - 1)))))
    return sorted(values)[idx]


async def run_benchmark(runs: int, circles: list[str]) -> None:
    federator = DataFederator()
    samples_ms: list[float] = []

    try:
        await federator.initialize_pools()

        for i in range(runs):
            req = _new_request_id()
            t0 = time.perf_counter()
            rows = await federator.execute_federated_query(circles, request_id=req)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            samples_ms.append(elapsed_ms)
            logging.info(
                "BENCH_RUN run=%d req=%s rows=%d elapsed_ms=%.2f",
                i + 1,
                req,
                len(rows),
                elapsed_ms,
            )
    finally:
        await federator.close_pools()

    p50 = percentile(samples_ms, 50)
    p95 = percentile(samples_ms, 95)
    p99 = percentile(samples_ms, 99)
    avg = statistics.mean(samples_ms) if samples_ms else 0.0

    print("\n=== Benchmark Summary ===")
    print(f"runs={runs}")
    print(f"circles={circles}")
    print(f"min_ms={min(samples_ms):.2f}")
    print(f"avg_ms={avg:.2f}")
    print(f"p50_ms={p50:.2f}")
    print(f"p95_ms={p95:.2f}")
    print(f"p99_ms={p99:.2f}")
    print(f"max_ms={max(samples_ms):.2f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local federated-query benchmark.")
    parser.add_argument("--runs", type=int, default=20, help="Number of benchmark iterations.")
    parser.add_argument(
        "--circles",
        nargs="+",
        default=["circle_1", "circle_7", "circle_12"],
        help="Circle IDs to query on each run.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    args = parse_args()
    asyncio.run(run_benchmark(args.runs, args.circles))
