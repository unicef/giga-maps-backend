"""
Benchmark old school APIs against the new entity APIs.

The script discovers countries/admin1/default live school layers, discovers
entity layers for configured entity types, builds comparable request cases for:
  - tiles/connectivity/
  - tiles/connectivity_status/
  - layers/map/
  - layers/info/
  - connectivityconfigs/

It writes an Excel checkpoint every ENTITY_COMPARISON["flush_every"] completed
requests so a long or interrupted run still leaves usable results.

Example:
    python3 benchmark_scripts/benchmark_old_vs_entity_apis.py \
        https://uni-ooi-giga-maps-backend-stg.azurewebsites.net
"""

import argparse
import json
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

from benchmark_common import (
    ApiCall,
    ApiClient,
    BenchmarkResult,
    cache_value,
    discover_geo_layer_scenarios,
    execute_api_calls,
    failed_results,
    normalize_list_response,
    pick_layer,
    print_failed_results,
    print_geo_layer_combination_count,
    summarize,
    write_xlsx,
)
from benchmark_config import API_PATHS, DEFAULT_TIMEOUT_SECONDS, ENTITY_COMPARISON


@dataclass(frozen=True)
class ComparisonCall:
    case_id: str
    api_name: str
    side: str
    entity_type: str
    scope: str
    country_code: str
    country_id: int
    admin1_id: Optional[int]
    layer_id: Optional[int]
    api_call: ApiCall


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="Target backend origin, e.g. https://staging.example.org")
    return parser.parse_args(argv)


def with_cache(params: Dict[str, object], cache_mode: str) -> Dict[str, object]:
    params = dict(params)
    cache = cache_value(cache_mode)
    if cache:
        params["cache"] = cache
    return params


def layer_path(api_key: str, layer_id: int) -> str:
    return API_PATHS[api_key].format(layer_id=layer_id)


def discover_entity_layer_ids(client: ApiClient, scenarios, entity_type_codes: Sequence[str]) -> Dict[Tuple[int, str], int]:
    layer_ids = {}
    for scenario in scenarios:
        for entity_type in entity_type_codes:
            if entity_type == "school":
                layer_ids[(scenario.country_id, entity_type)] = scenario.layer_id
                continue

            params = {
                "country_id": scenario.country_id,
                "entity_type__code": entity_type,
                "page_size": 500,
            }
            try:
                payload = client.get_json(API_PATHS["entities_layers_published"], params)
                layer_id = pick_layer(normalize_list_response(payload), scenario.country_id)
                if layer_id:
                    layer_ids[(scenario.country_id, entity_type)] = layer_id
            except Exception as exc:
                url = client.url(API_PATHS["entities_layers_published"], params)
                print("Warning: entity layer discovery failed for country_id={}, entity_type={}: {} | url={}".format(
                    scenario.country_id,
                    entity_type,
                    exc,
                    url,
                ), file=sys.stderr)
    return layer_ids


def filter_cases(scenario) -> Tuple[Tuple[str, Optional[int], Dict[str, object]], ...]:
    cases = [("country", None, {"country_id": scenario.country_id})]
    for admin1_id in scenario.admin1_ids:
        cases.append(("admin1", admin1_id, {"country_id": scenario.country_id, "admin1_id": admin1_id}))
    return tuple(cases)


def add_call(
    calls,
    client: ApiClient,
    case_id: str,
    api_name: str,
    side: str,
    entity_type: str,
    scope: str,
    country_code: str,
    country_id: int,
    admin1_id: Optional[int],
    layer_id: Optional[int],
    path: str,
    params: Dict[str, object],
) -> None:
    label = "{}:{}:{}:{}:{}:{}".format(case_id, api_name, side, entity_type, scope, layer_id or "none")
    calls.append(
        ComparisonCall(
            case_id=case_id,
            api_name=api_name,
            side=side,
            entity_type=entity_type,
            scope=scope,
            country_code=country_code,
            country_id=country_id,
            admin1_id=admin1_id,
            layer_id=layer_id,
            api_call=ApiCall(
                label=label,
                method="GET",
                url=client.url(path, params),
                params=params,
            ),
        )
    )


def build_comparison_calls(client: ApiClient, scenarios, entity_layer_ids) -> Tuple[ComparisonCall, ...]:
    calls = []
    cache_mode = ENTITY_COMPARISON["cache_mode"]
    entity_types = ENTITY_COMPARISON["entity_type_codes"]
    tile_coordinates = ENTITY_COMPARISON["tile_coordinates"]

    for scenario in scenarios:
        for scope, admin1_id, base_filter in filter_cases(scenario):
            case_prefix = "{}-{}-{}".format(scenario.country_code, scope, admin1_id or "country")

            for tile in tile_coordinates:
                tile_filter = with_cache({**tile, **base_filter, "limit": 50000}, cache_mode)
                tile_case_id = "{}-z{}x{}y{}".format(case_prefix, tile["z"], tile["x"], tile["y"])
                for api_name, old_key, new_key in (
                    ("tiles_connectivity", "schools_tiles_connectivity", "entities_tiles_connectivity"),
                    ("tiles_connectivity_status", "schools_tiles_connectivity_status", "entities_tiles_connectivity_status"),
                ):
                    add_call(
                        calls, client, tile_case_id, api_name, "old", "school", scope,
                        scenario.country_code, scenario.country_id, admin1_id, None,
                        API_PATHS[old_key], tile_filter,
                    )
                    add_call(
                        calls, client, tile_case_id, api_name, "new", "school_health", scope,
                        scenario.country_code, scenario.country_id, admin1_id, None,
                        API_PATHS[new_key], tile_filter,
                    )
                    for entity_type in entity_types:
                        params = dict(tile_filter)
                        params["entity_type__code"] = entity_type
                        add_call(
                            calls, client, tile_case_id, api_name, "new", entity_type, scope,
                            scenario.country_code, scenario.country_id, admin1_id,
                            entity_layer_ids.get((scenario.country_id, entity_type)),
                            API_PATHS[new_key], params,
                        )

            old_layer_id = scenario.layer_id
            for api_name, old_key, new_key in (
                ("layers_map", "schools_layers_map", "entities_layers_map"),
                ("layers_info", "schools_layers_info", "entities_layers_info"),
            ):
                old_path = layer_path(old_key, old_layer_id)
                old_params = with_cache(base_filter, cache_mode)
                if api_name == "layers_map":
                    old_params = {**old_params, **ENTITY_COMPARISON["tile_coordinates"][0], "limit": 50000}
                add_call(
                    calls, client, case_prefix, api_name, "old", "school", scope,
                    scenario.country_code, scenario.country_id, admin1_id, old_layer_id,
                    old_path, old_params,
                )

                for entity_type in entity_types:
                    layer_id = entity_layer_ids.get((scenario.country_id, entity_type))
                    if not layer_id:
                        continue
                    prefixed = "{}_layer_id".format(entity_type)
                    params = with_cache({**base_filter, prefixed: layer_id, "entity_type__code": entity_type}, cache_mode)
                    if api_name == "layers_map":
                        params = {**params, **ENTITY_COMPARISON["tile_coordinates"][0], "limit": 50000}
                    add_call(
                        calls, client, case_prefix, api_name, "new", entity_type, scope,
                        scenario.country_code, scenario.country_id, admin1_id, layer_id,
                        API_PATHS[new_key], params,
                    )

                health_layer_id = entity_layer_ids.get((scenario.country_id, "health"))
                if health_layer_id:
                    combined_params = with_cache({
                        **base_filter,
                        "school_layer_id": old_layer_id,
                        "health_layer_id": health_layer_id,
                    }, cache_mode)
                    if api_name == "layers_map":
                        combined_params = {
                            **combined_params,
                            **ENTITY_COMPARISON["tile_coordinates"][0],
                            "limit": 50000,
                        }
                    add_call(
                        calls, client, case_prefix, api_name, "new", "school_health", scope,
                        scenario.country_code, scenario.country_id, admin1_id, None,
                        API_PATHS[new_key], combined_params,
                    )

            configs_params = with_cache({**base_filter, "layer_id": old_layer_id}, cache_mode)
            add_call(
                calls, client, case_prefix, "connectivityconfigs", "old", "school", scope,
                scenario.country_code, scenario.country_id, admin1_id, old_layer_id,
                API_PATHS["schools_connectivityconfigs"], configs_params,
            )
            for entity_type in entity_types:
                layer_id = entity_layer_ids.get((scenario.country_id, entity_type))
                if not layer_id:
                    continue
                params = with_cache({**base_filter, "layer_id": layer_id, "entity_type__code": entity_type}, cache_mode)
                add_call(
                    calls, client, case_prefix, "connectivityconfigs", "new", entity_type, scope,
                    scenario.country_code, scenario.country_id, admin1_id, layer_id,
                    API_PATHS["entities_connectivityconfigs"], params,
                )

    return tuple(calls)


def summarize_by_group(results: Sequence[Tuple[ComparisonCall, BenchmarkResult]]) -> dict:
    grouped = defaultdict(list)
    for call, result in results:
        grouped[(call.api_name, call.side, call.entity_type)].append(result)

    rows = []
    for (api_name, side, entity_type), group_results in sorted(grouped.items()):
        elapsed = [result.elapsed_ms for result in group_results]
        ok = [result for result in group_results if result.status and 200 <= result.status < 300]
        rows.append({
            "api_name": api_name,
            "side": side,
            "entity_type": entity_type,
            "calls": len(group_results),
            "success": len(ok),
            "success_rate": len(ok) / len(group_results) if group_results else 0,
            "mean_ms": statistics.mean(elapsed) if elapsed else 0,
            "min_ms": min(elapsed) if elapsed else 0,
            "max_ms": max(elapsed) if elapsed else 0,
        })
    return {"rows": rows}


def write_comparison_excel(path: str, results: Sequence[Tuple[ComparisonCall, BenchmarkResult]], scenarios) -> None:
    result_rows = [(
        "case_id", "api_name", "side", "entity_type", "scope", "country_code", "country_id",
        "admin1_id", "layer_id", "status", "elapsed_ms", "bytes_read", "error", "query_params", "url",
    )]
    for call, result in results:
        result_rows.append((
            call.case_id,
            call.api_name,
            call.side,
            call.entity_type,
            call.scope,
            call.country_code,
            call.country_id,
            call.admin1_id or "",
            call.layer_id or "",
            result.status,
            round(result.elapsed_ms, 2),
            result.bytes_read,
            result.error,
            result.query_params,
            result.url,
        ))

    summary_rows = [("api_name", "side", "entity_type", "calls", "success", "success_rate", "mean_ms", "min_ms", "max_ms")]
    for row in summarize_by_group(results)["rows"]:
        summary_rows.append((
            row["api_name"],
            row["side"],
            row["entity_type"],
            row["calls"],
            row["success"],
            row["success_rate"],
            round(row["mean_ms"], 2),
            round(row["min_ms"], 2),
            round(row["max_ms"], 2),
        ))

    failed_rows = [("case_id", "api_name", "side", "entity_type", "status", "error", "url")]
    for call, result in results:
        if failed_results([result]):
            failed_rows.append((
                call.case_id,
                call.api_name,
                call.side,
                call.entity_type,
                result.status or "error",
                result.error,
                result.url,
            ))

    combination_rows = [("country_code", "country_id", "layer_id", "admin1_count", "generated_filter_scopes")]
    for scenario in scenarios:
        combination_rows.append((
            scenario.country_code,
            scenario.country_id,
            scenario.layer_id,
            len(scenario.admin1_ids),
            1 + len(scenario.admin1_ids),
        ))

    write_xlsx(path, (
        ("results", result_rows),
        ("summary", summary_rows),
        ("failed", failed_rows),
        ("combinations", combination_rows),
    ))


def benchmark_calls(calls: Sequence[ComparisonCall], scenarios) -> Sequence[Tuple[ComparisonCall, BenchmarkResult]]:
    metadata_by_label = {call.api_call.label: call for call in calls}
    benchmark_results = []
    output_path = ENTITY_COMPARISON["output_path"]
    flush_every = ENTITY_COMPARISON["flush_every"]

    def flush_excel(completed: int, total: int) -> None:
        write_comparison_excel(output_path, benchmark_results, scenarios)
        print("Saved partial Excel report after {}/{} requests: {}".format(completed, total, output_path))

    def on_result(result: BenchmarkResult, completed: int, total: int) -> None:
        benchmark_results.append((metadata_by_label[result.label], result))
        if completed % flush_every == 0 or completed == total:
            flush_excel(completed, total)

    execute_api_calls(
        [call.api_call for call in calls],
        timeout=DEFAULT_TIMEOUT_SECONDS,
        headers={},
        max_response_chars=ENTITY_COMPARISON["max_response_chars"],
        concurrency=ENTITY_COMPARISON["concurrency"],
        on_result=on_result,
        store_results=False,
    )
    return benchmark_results


def main(argv: Optional[Sequence[str]] = None) -> int:
    total_started = time.perf_counter()
    args = parse_args(argv)
    client = ApiClient(args.base_url, DEFAULT_TIMEOUT_SECONDS, headers={})

    scenarios = discover_geo_layer_scenarios(
        client=client,
        country_codes=(),
        max_admin1=ENTITY_COMPARISON["max_admin1"],
        all_countries=ENTITY_COMPARISON["all_countries"],
        all_layers=ENTITY_COMPARISON["all_layers"],
    )
    if not scenarios:
        print("No benchmark scenarios discovered from {}".format(args.base_url), file=sys.stderr)
        return 2

    print_geo_layer_combination_count(scenarios)
    entity_layer_ids = discover_entity_layer_ids(client, scenarios, ENTITY_COMPARISON["entity_type_codes"])
    calls = build_comparison_calls(client, scenarios, entity_layer_ids)
    print("\nPrepared {} old/new API requests.".format(len(calls)))
    print("output={}, concurrency={}, response_chars={}".format(
        ENTITY_COMPARISON["output_path"],
        ENTITY_COMPARISON["concurrency"],
        ENTITY_COMPARISON["max_response_chars"],
    ))

    benchmark_results = benchmark_calls(calls, scenarios)
    plain_results = [result for _, result in benchmark_results]
    summary = summarize(plain_results, api_name="old_vs_entity")

    print("\nOverall summary")
    print(json.dumps(summary, indent=2, sort_keys=True))
    print_failed_results(plain_results)

    write_comparison_excel(ENTITY_COMPARISON["output_path"], benchmark_results, scenarios)
    print("Wrote final Excel report: {}".format(ENTITY_COMPARISON["output_path"]))
    print("Total runtime: {:.2f}s".format(time.perf_counter() - total_started))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
