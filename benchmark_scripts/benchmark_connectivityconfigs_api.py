"""
Benchmark Schools Connectivity Configs API

Example:
    python3 benchmark_scripts/benchmark_connectivityconfigs_api.py \
        https://uni-ooi-giga-maps-backend-stg.azurewebsites.net
"""

import argparse
import json
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from benchmark_common import (
    ApiCall,
    ApiClient,
    BenchmarkResult,
    build_geo_layer_api_calls,
    discover_geo_layer_scenarios,
    execute_api_calls,
    print_failed_results,
    print_geo_layer_combination_count,
    summarize,
    write_xlsx,
)
from benchmark_config import API_PATHS, BENCHMARKS, DEFAULT_TIMEOUT_SECONDS


BENCHMARK_KEY = "schools_connectivityconfigs"
BENCHMARK_CONFIG = BENCHMARKS[BENCHMARK_KEY]
API_PATH = API_PATHS[BENCHMARK_CONFIG["api_key"]]


@dataclass(frozen=True)
class FocusedCall:
    scope: str
    country_code: str
    country_id: int
    layer_id: int
    admin1_id: Optional[int]
    api_call: ApiCall


def build_connectivityconfigs_calls(client: ApiClient, scenarios, cache_mode: str) -> Tuple[FocusedCall, ...]:
    calls = []
    api_calls = build_geo_layer_api_calls(client, API_PATH, scenarios, cache_mode)
    api_call_by_params = {
        tuple(sorted(call.params.items())): call
        for call in api_calls
    }

    for scenario in scenarios:
        for admin1_id in [None] + list(scenario.admin1_ids):
            params = {
                "layer_id": scenario.layer_id,
                "country_id": scenario.country_id,
            }
            if admin1_id:
                params["admin1_id"] = admin1_id
            if cache_mode == "bypass":
                params["cache"] = "off"
            api_call = api_call_by_params[tuple(sorted(params.items()))]
            calls.append(
                FocusedCall(
                    scope="admin1" if admin1_id else "country",
                    country_code=scenario.country_code,
                    country_id=scenario.country_id,
                    layer_id=scenario.layer_id,
                    admin1_id=admin1_id,
                    api_call=api_call,
                )
            )

    return tuple(calls)


def benchmark_calls(calls: Sequence[FocusedCall], timeout: int, headers, concurrency: int, max_response_chars: int):
    metadata_by_url = {call.api_call.url: call for call in calls}
    results = execute_api_calls(
        [call.api_call for call in calls],
        timeout=timeout,
        headers=headers,
        max_response_chars=max_response_chars,
        concurrency=concurrency,
    )
    return [(metadata_by_url[result.url], result) for result in results]


def scoped_summary(results: Sequence[Tuple[FocusedCall, BenchmarkResult]]) -> dict:
    grouped = defaultdict(list)
    for call, result in results:
        grouped[call.scope].append(result)

    summary = {}
    for scope, scope_results in grouped.items():
        elapsed = [result.elapsed_ms for result in scope_results]
        ok = [result for result in scope_results if result.status and 200 <= result.status < 300]
        summary[scope] = {
            "calls": len(scope_results),
            "success": len(ok),
            "success_rate": len(ok) / len(scope_results) if scope_results else 0,
            "mean_ms": statistics.mean(elapsed) if elapsed else 0,
            "min_ms": min(elapsed) if elapsed else 0,
            "max_ms": max(elapsed) if elapsed else 0,
        }
    return summary


def write_excel(path: str, results: Sequence[Tuple[FocusedCall, BenchmarkResult]], summary: dict, scenarios) -> None:
    result_rows = [(
        "scope",
        "country_code",
        "country_id",
        "admin1_id",
        "layer_id",
        "label",
        "method",
        "status",
        "elapsed_ms",
        "bytes_read",
        "error",
        "query_params",
        "url",
        "response_json",
    )]
    for call, result in results:
        result_rows.append((
            call.scope,
            call.country_code,
            call.country_id,
            call.admin1_id or "",
            call.layer_id,
            result.label,
            result.method,
            result.status,
            round(result.elapsed_ms, 2),
            result.bytes_read,
            result.error,
            result.query_params,
            result.url,
            result.response_json,
        ))

    summary_rows = [("metric", "value")]
    for key, value in summary["overall"].items():
        summary_rows.append((key, json.dumps(value) if isinstance(value, dict) else value))

    scope_rows = [("scope", "calls", "success", "success_rate", "mean_ms", "min_ms", "max_ms")]
    for scope, values in sorted(summary["by_scope"].items()):
        scope_rows.append((
            scope,
            values["calls"],
            values["success"],
            values["success_rate"],
            round(values["mean_ms"], 2),
            round(values["min_ms"], 2),
            round(values["max_ms"], 2),
        ))

    combination_rows = [("country_code", "country_id", "layer_id", "admin1_count", "generated_calls")]
    for scenario in scenarios:
        combination_rows.append((
            scenario.country_code,
            scenario.country_id,
            scenario.layer_id,
            len(scenario.admin1_ids),
            1 + len(scenario.admin1_ids),
        ))
    combination_rows.append(("TOTAL", "", "", "", sum(1 + len(s.admin1_ids) for s in scenarios)))

    write_xlsx(path, (
        ("results", result_rows),
        ("summary", summary_rows),
        ("scope_summary", scope_rows),
        ("combinations", combination_rows),
    ))


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="Target backend origin, e.g. https://staging.example.org")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    total_started = time.perf_counter()
    args = parse_args(argv)
    headers = {}
    client = ApiClient(args.base_url, DEFAULT_TIMEOUT_SECONDS, headers=headers)

    discovery_started = time.perf_counter()
    scenarios = discover_geo_layer_scenarios(
        client=client,
        country_codes=(),
        max_admin1=BENCHMARK_CONFIG["max_admin1"],
        all_countries=BENCHMARK_CONFIG["all_countries"],
        all_layers=BENCHMARK_CONFIG["all_layers"],
    )
    discovery_seconds = time.perf_counter() - discovery_started

    if not scenarios:
        print(
            "No connectivityconfigs benchmark scenarios could be discovered from {}.".format(args.base_url),
            file=sys.stderr,
        )
        return 2

    calls = build_connectivityconfigs_calls(client, scenarios, BENCHMARK_CONFIG["cache_mode"])
    print_geo_layer_combination_count(scenarios)
    print("\nTotal connectivityconfigs API calls to execute: {}".format(len(calls)))
    print("Discovery time: {:.2f}s".format(discovery_seconds))

    print("\nPrepared {} calls.".format(len(calls)))
    print("endpoint={}, concurrency={}, cache_mode={}, output={}".format(
        API_PATH,
        BENCHMARK_CONFIG["concurrency"],
        BENCHMARK_CONFIG["cache_mode"],
        BENCHMARK_CONFIG["output_path"],
    ))

    benchmark_started = time.perf_counter()
    benchmark_results = benchmark_calls(
        calls=calls,
        timeout=DEFAULT_TIMEOUT_SECONDS,
        headers=headers,
        concurrency=BENCHMARK_CONFIG["concurrency"],
        max_response_chars=BENCHMARK_CONFIG["max_response_chars"],
    )
    benchmark_seconds = time.perf_counter() - benchmark_started
    total_seconds = time.perf_counter() - total_started

    plain_results = [result for _, result in benchmark_results]
    summary = {
        "overall": summarize(plain_results, api_name=BENCHMARK_KEY),
        "by_scope": scoped_summary(benchmark_results),
    }

    print("\nOverall summary")
    print(json.dumps(summary["overall"], indent=2, sort_keys=True))
    print("\nScope summary")
    print(json.dumps(summary["by_scope"], indent=2, sort_keys=True))
    print_failed_results(plain_results)
    print("\nBenchmark execution time: {:.2f}s".format(benchmark_seconds))
    print("Total runtime including discovery: {:.2f}s".format(total_seconds))

    write_excel(BENCHMARK_CONFIG["output_path"], benchmark_results, summary, scenarios)
    print("Wrote Excel report: {}".format(BENCHMARK_CONFIG["output_path"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
