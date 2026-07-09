"""
Benchmark a configured map API and export validation data to Excel.

This legacy runner keeps the configurable CLI from the original script, while
the reusable HTTP, discovery, timing, and Excel helpers live in benchmark_common.py.
"""

import argparse
import random
import sys
import time
from typing import Optional, Sequence

from benchmark_common import (
    ApiClient,
    build_api_calls_from_xlsx_query_params,
    build_geo_layer_api_calls,
    discover_geo_layer_scenarios,
    execute_api_calls,
    parse_header,
    print_failed_results,
    print_geo_layer_combination_count,
    print_summary,
    repeat_calls,
    summarize,
    write_excel,
)
from benchmark_config import API_PATHS, DEFAULT_COUNTRY_CODES, DEFAULT_TIMEOUT_SECONDS


DEFAULT_API_KEY = "schools_connectivityconfigs"


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="Target backend origin, e.g. https://staging.example.org")
    parser.add_argument(
        "--api-key",
        default=DEFAULT_API_KEY,
        choices=sorted(API_PATHS.keys()),
        help="API path key from benchmark_config.API_PATHS",
    )
    parser.add_argument("--calls", type=int, default=200, help="Total API calls to execute")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent requests")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="Per-request timeout in seconds")
    parser.add_argument(
        "--country-codes",
        default=",".join(DEFAULT_COUNTRY_CODES),
        help="Comma-separated country codes to discover. Ignored when --all-countries is used.",
    )
    parser.add_argument("--all-countries", action="store_true", help="Discover all countries with schools")
    parser.add_argument("--all-layers", action="store_true", help="Use all published live layers for each country")
    parser.add_argument(
        "--max-admin1",
        type=int,
        default=12,
        help="Max admin1 values per country. Use -1 for all discovered admin1 values.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Shuffle seed for reproducible call order")
    parser.add_argument(
        "--cache-mode",
        choices=("bypass", "default"),
        default="bypass",
        help="Use bypass to append cache=off, or default to exercise current cache behavior",
    )
    parser.add_argument(
        "--query-params-xlsx",
        help=(
            "Read query_params from an existing benchmark Excel results sheet "
            "and skip discovery."
        ),
    )
    parser.add_argument("--header", action="append", type=parse_header, default=[], help="Extra request header")
    parser.add_argument(
        "--output",
        default="connectivityconfigs_benchmark.xlsx",
        help="Excel .xlsx output path for timings, URLs, and responses",
    )
    parser.add_argument(
        "--max-response-chars",
        type=int,
        default=3000,
        help="Maximum response characters stored per row. Use 0 for full response.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print generated URLs without executing benchmark calls")
    parser.add_argument("--count-only", action="store_true", help="Only discover and print possible generated combinations")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    total_started = time.perf_counter()
    args = parse_args(argv)
    headers = dict(args.header)
    rng = random.Random(args.seed)
    client = ApiClient(args.base_url, args.timeout, headers=headers)
    api_path = API_PATHS[args.api_key]
    country_codes = tuple(code.strip().lower() for code in args.country_codes.split(",") if code.strip())

    scenarios = []
    discovery_started = time.perf_counter()
    if args.query_params_xlsx:
        examples = build_api_calls_from_xlsx_query_params(client, api_path, args.query_params_xlsx)
        discovery_seconds = time.perf_counter() - discovery_started
        print("\nLoaded {} unique query-param combinations from {}".format(
            len(examples),
            args.query_params_xlsx,
        ))
        print("Workbook load time: {:.2f}s".format(discovery_seconds))
    else:
        scenarios = discover_geo_layer_scenarios(
            client,
            country_codes,
            args.max_admin1,
            args.all_countries,
            args.all_layers,
        )
        discovery_seconds = time.perf_counter() - discovery_started
        if not scenarios:
            print(
                "No benchmark scenarios could be discovered from {}. Check the base URL scheme, "
                "server reachability, and whether the target has published country/layer data.".format(args.base_url),
                file=sys.stderr,
            )
            return 2

        examples = build_geo_layer_api_calls(client, api_path, scenarios, args.cache_mode)
        print_geo_layer_combination_count(scenarios)
        print("Discovery time: {:.2f}s".format(discovery_seconds))

    if args.count_only:
        print("Total runtime: {:.2f}s".format(time.perf_counter() - total_started))
        return 0

    calls = repeat_calls(examples, args.calls, rng)
    rng.shuffle(calls)

    print("\nPrepared {} API calls from {} unique combinations.".format(len(calls), len(examples)))
    print("api_key={}, concurrency={}, cache_mode={}, output={}".format(
        args.api_key,
        args.concurrency,
        args.cache_mode,
        args.output,
    ))

    if args.dry_run:
        for call in calls[: min(30, len(calls))]:
            print("{} {} {}".format(call.label, call.method, call.url))
        print("Total runtime: {:.2f}s".format(time.perf_counter() - total_started))
        return 0

    benchmark_started = time.perf_counter()
    results = execute_api_calls(
        calls,
        timeout=args.timeout,
        headers=headers,
        max_response_chars=args.max_response_chars,
        concurrency=args.concurrency,
    )

    benchmark_seconds = time.perf_counter() - benchmark_started
    total_seconds = time.perf_counter() - total_started
    summary = summarize(results, api_name=args.api_key)
    print_summary(summary)
    print_failed_results(results)
    print("\nBenchmark execution time: {:.2f}s".format(benchmark_seconds))
    print("Total runtime including discovery: {:.2f}s".format(total_seconds))

    write_excel(args.output, results, summary, scenarios)
    print("Wrote Excel report: {}".format(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
