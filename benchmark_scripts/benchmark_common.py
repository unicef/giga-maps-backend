"""Shared helpers for benchmark scripts."""

import argparse
import json
import math
import random
import ssl
import statistics
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlsplit
from urllib.request import Request, urlopen
from xml.sax.saxutils import escape


@dataclass(frozen=True)
class GeoLayerScenario:
    country_id: int
    country_code: str
    layer_id: int
    admin1_ids: Tuple[int, ...]


@dataclass(frozen=True)
class ApiCall:
    label: str
    method: str
    url: str
    params: Dict[str, object]


@dataclass
class BenchmarkResult:
    label: str
    method: str
    url: str
    query_params: str
    status: Optional[int]
    elapsed_ms: float
    bytes_read: int
    response_json: str = ""
    error: str = ""


class ApiClient:
    def __init__(self, base_url: str, timeout: int, headers: Optional[Dict[str, str]] = None):
        self.base_url = ensure_base_url(base_url)
        self.timeout = timeout
        self.headers = headers or {}

    def url(self, path: str, params: Optional[Dict[str, object]] = None) -> str:
        final_url = urljoin(self.base_url, path.lstrip("/"))
        if params:
            query = urlencode({key: value for key, value in params.items() if value is not None})
            final_url = "{}?{}".format(final_url, query)
        return final_url

    def get_json(self, path: str, params: Optional[Dict[str, object]] = None):
        request = Request(
            self.url(path, params),
            headers={"Accept": "application/json", **self.headers},
            method="GET",
        )
        with urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))


def ensure_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("--base-url must include scheme and host, for example https://example.org")
    return base_url.rstrip("/") + "/"


def format_discovery_error(exc: Exception, base_url: str) -> str:
    message = str(exc)
    parsed = urlparse(base_url)
    if parsed.scheme == "https" and "WRONG_VERSION_NUMBER" in message:
        return (
            "{}. This usually means the target is serving plain HTTP, not HTTPS. "
            "For local Django, use http://{} instead."
        ).format(exc, parsed.netloc)
    if isinstance(exc, ssl.SSLError):
        return "{}. Check whether the target URL scheme and certificate are correct.".format(exc)
    return str(exc)


def print_warning_with_url(message: str, url: str, exc: Exception) -> None:
    print("{}: {} | url={}".format(message, format_discovery_error(exc, url), url), file=sys.stderr)


def parse_header(raw_header: str) -> Tuple[str, str]:
    if ":" not in raw_header:
        raise argparse.ArgumentTypeError("Headers must be in 'Name: value' format")
    name, value = raw_header.split(":", 1)
    return name.strip(), value.strip()


def normalize_list_response(data) -> List[dict]:
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        return data["results"]
    if isinstance(data, list):
        return data
    return []


def get_discovered_countries(client: ApiClient) -> List[dict]:
    return normalize_list_response(client.get_json("/api/locations/countries/", {
        "has_schools": "true",
        "page_size": 500,
    }))


def pick_layer(layers: Sequence[dict], country_id: int) -> Optional[int]:
    candidates = []
    for layer in layers:
        if layer.get("type") != "LIVE":
            continue
        active_countries = layer.get("active_countries_list") or []
        for relationship in active_countries:
            if str(relationship.get("country")) == str(country_id):
                candidates.append((relationship.get("is_default") is True, layer))
                break
    if not candidates:
        candidates = [(False, layer) for layer in layers if layer.get("type") == "LIVE"]
    candidates.sort(key=lambda item: item[0], reverse=True)
    for _, layer in candidates:
        if layer.get("id"):
            return int(layer["id"])
    return None


def get_live_layer_ids(layers: Sequence[dict], country_id: int, all_layers: bool) -> Tuple[int, ...]:
    if not all_layers:
        layer_id = pick_layer(layers, country_id)
        return (layer_id,) if layer_id else ()

    layer_ids = []
    for layer in layers:
        if layer.get("type") != "LIVE" or not layer.get("id"):
            continue
        active_countries = layer.get("active_countries_list") or []
        if not active_countries:
            layer_ids.append(int(layer["id"]))
            continue
        for relationship in active_countries:
            if str(relationship.get("country")) == str(country_id):
                layer_ids.append(int(layer["id"]))
                break
    return tuple(dict.fromkeys(layer_ids))


def cache_value(cache_mode: str) -> Optional[str]:
    if cache_mode == "bypass":
        return "off"
    return None


def get_admin1_ids(country: dict, max_admin1: int) -> Tuple[int, ...]:
    admin1_ids = tuple(
        int(admin["id"])
        for admin in country.get("admin1_metadata", [])
        if isinstance(admin, dict) and admin.get("id")
    )
    if max_admin1 >= 0:
        return admin1_ids[:max_admin1]
    return admin1_ids


def discover_geo_layer_scenarios(
    client: ApiClient,
    country_codes: Sequence[str],
    max_admin1: int,
    all_countries: bool,
    all_layers: bool,
) -> List[GeoLayerScenario]:
    countries_by_code = {}
    countries_url = client.url("/api/locations/countries/", {
        "has_schools": "true",
        "page_size": 500,
    })
    try:
        countries = get_discovered_countries(client)
        countries_by_code = {str(country.get("code", "")).lower(): country for country in countries}
    except Exception as exc:
        print_warning_with_url("Warning: country list discovery failed", countries_url, exc)

    scenarios = []
    selected_country_codes = tuple(countries_by_code.keys()) if all_countries else country_codes
    for raw_code in selected_country_codes:
        code = raw_code.lower()
        country = countries_by_code.get(code, {})
        country_detail_url = client.url("/api/locations/countries/{}/".format(code))
        try:
            detail = client.get_json("/api/locations/countries/{}/".format(code))
            country = {**country, **detail}
        except Exception as exc:
            print_warning_with_url(
                "Warning: country detail discovery failed for {}".format(code),
                country_detail_url,
                exc,
            )

        country_id = country.get("id")
        if not country_id:
            continue

        layer_url = client.url(
            "/api/accounts/layers/PUBLISHED/",
            {"country_id": country_id, "page_size": 500},
        )
        try:
            layer_payload = client.get_json(
                "/api/accounts/layers/PUBLISHED/",
                {"country_id": country_id, "page_size": 500},
            )
            layer_ids = get_live_layer_ids(normalize_list_response(layer_payload), int(country_id), all_layers)
        except Exception as exc:
            print_warning_with_url("Warning: layer discovery failed for {}".format(code), layer_url, exc)
            layer_ids = ()
        if not layer_ids:
            continue

        admin1_ids = get_admin1_ids(country, max_admin1)
        for layer_id in layer_ids:
            scenarios.append(
                GeoLayerScenario(
                    country_id=int(country_id),
                    country_code=code,
                    layer_id=int(layer_id),
                    admin1_ids=admin1_ids,
                )
            )
    return scenarios


def geo_layer_params(scenario: GeoLayerScenario, admin1_id: Optional[int], cache_mode: str) -> Dict[str, object]:
    params = {
        "layer_id": scenario.layer_id,
        "country_id": scenario.country_id,
    }
    if admin1_id:
        params["admin1_id"] = admin1_id
    cache = cache_value(cache_mode)
    if cache:
        params["cache"] = cache
    return params


def build_geo_layer_api_calls(
    client: ApiClient,
    api_path: str,
    scenarios: Sequence[GeoLayerScenario],
    cache_mode: str,
) -> List[ApiCall]:
    calls = []
    for scenario in scenarios:
        admin_candidates = [None] + list(scenario.admin1_ids)
        for admin1_id in admin_candidates:
            label = "{}{}".format(
                scenario.country_code,
                "-admin1-{}".format(admin1_id) if admin1_id else "-country",
            )
            params = geo_layer_params(scenario, admin1_id, cache_mode)
            calls.append(
                ApiCall(
                    label=label,
                    method="GET",
                    url=client.url(api_path, params),
                    params=params,
                )
            )
    return calls


def excel_cell_text(cell) -> str:
    value = cell.find("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
    if value is not None:
        return value.text or ""
    inline_text = cell.find("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}is/{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
    if inline_text is not None:
        return inline_text.text or ""
    return ""


def excel_column_index(reference: str) -> int:
    letters = "".join(ch for ch in reference if ch.isalpha())
    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - ord("A") + 1)
    return index - 1


def read_xlsx_rows(path: str, sheet_index: int = 1) -> List[List[str]]:
    import xml.etree.ElementTree as ET

    sheet_name = "xl/worksheets/sheet{}.xml".format(sheet_index)
    rows = []
    with zipfile.ZipFile(path) as workbook:
        root = ET.fromstring(workbook.read(sheet_name))

    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    for row in root.findall(".//{}row".format(namespace)):
        values = []
        for cell in row.findall("{}c".format(namespace)):
            reference = cell.attrib.get("r", "")
            column_index = excel_column_index(reference) if reference else len(values)
            while len(values) < column_index:
                values.append("")
            values.append(excel_cell_text(cell))
        rows.append(values)
    return rows


def coerce_query_param_value(value: str):
    if value.isdigit():
        return int(value)
    return value


def build_api_calls_from_xlsx_query_params(client: ApiClient, api_path: str, workbook_path: str) -> List[ApiCall]:
    rows = read_xlsx_rows(workbook_path, sheet_index=1)
    if not rows:
        raise ValueError("Workbook has no rows: {}".format(workbook_path))

    header = rows[0]
    try:
        label_index = header.index("label")
        query_params_index = header.index("query_params")
    except ValueError as exc:
        raise ValueError(
            'Workbook "{}" must contain "label" and "query_params" columns in the results sheet'.format(
                workbook_path
            )
        ) from exc

    calls = []
    seen = set()
    for row in rows[1:]:
        if len(row) <= query_params_index:
            continue
        query_params = row[query_params_index].strip()
        if not query_params or query_params in seen:
            continue
        seen.add(query_params)

        params = {
            key: coerce_query_param_value(value)
            for key, value in parse_qsl(query_params, keep_blank_values=True)
        }
        label = row[label_index].strip() if len(row) > label_index and row[label_index].strip() else "xlsx-{}".format(len(calls) + 1)
        calls.append(
            ApiCall(
                label=label,
                method="GET",
                url=client.url(api_path, params),
                params=params,
            )
        )

    if not calls:
        raise ValueError("Workbook has no usable query_params rows: {}".format(workbook_path))
    return calls


def repeat_calls(examples: Sequence[ApiCall], count: int, rng: random.Random) -> List[ApiCall]:
    if count <= 0:
        return list(examples)
    calls = []
    while len(calls) < count:
        batch = list(examples)
        rng.shuffle(batch)
        calls.extend(batch)
    return calls[:count]


def compact_json(raw_body: bytes, max_chars: int) -> str:
    text = raw_body.decode("utf-8", errors="replace")
    try:
        text = json.dumps(json.loads(text), sort_keys=True)
    except json.JSONDecodeError:
        pass
    if max_chars and len(text) > max_chars:
        return text[:max_chars] + "...<truncated>"
    return text


def execute_call(call: ApiCall, timeout: int, headers: Dict[str, str], max_response_chars: int) -> BenchmarkResult:
    request = Request(call.url, headers={"Accept": "application/json", **headers}, method=call.method)
    started = time.perf_counter()
    status = None
    bytes_read = 0
    response_json = ""
    error = ""
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.getcode()
            body = response.read()
            bytes_read = len(body)
            response_json = compact_json(body, max_response_chars)
    except HTTPError as exc:
        status = exc.code
        error = "{} {}".format(exc.__class__.__name__, exc.reason)
        try:
            body = exc.read()
            bytes_read = len(body)
            response_json = compact_json(body, max_response_chars)
        except Exception:
            bytes_read = 0
    except (URLError, TimeoutError, OSError) as exc:
        error = "{}: {}".format(exc.__class__.__name__, exc)
    elapsed_ms = (time.perf_counter() - started) * 1000
    return BenchmarkResult(
        label=call.label,
        method=call.method,
        url=call.url,
        query_params=urlsplit(call.url).query,
        status=status,
        elapsed_ms=elapsed_ms,
        bytes_read=bytes_read,
        response_json=response_json,
        error=error,
    )


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[int(index)]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def summarize(results: Sequence[BenchmarkResult], api_name: str = "api") -> dict:
    elapsed = [result.elapsed_ms for result in results]
    ok = [result for result in results if result.status and 200 <= result.status < 300]
    statuses = {}
    for result in results:
        key = str(result.status) if result.status is not None else "error"
        statuses[key] = statuses.get(key, 0) + 1
    return {
        "api": api_name,
        "calls": len(results),
        "success": len(ok),
        "success_rate": len(ok) / len(results) if results else 0,
        "min_ms": min(elapsed) if elapsed else 0,
        "mean_ms": statistics.mean(elapsed) if elapsed else 0,
        "max_ms": max(elapsed) if elapsed else 0,
        "statuses": statuses,
    }


def execute_api_calls(
    calls: Sequence[ApiCall],
    timeout: int,
    headers: Dict[str, str],
    max_response_chars: int,
    concurrency: int,
) -> List[BenchmarkResult]:
    results = []
    completed = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(execute_call, call, timeout, headers, max_response_chars)
            for call in calls
        ]
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1
            if completed % 50 == 0 or completed == len(calls):
                print("Completed {}/{} calls".format(completed, len(calls)))
    return results


def excel_column_name(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def xml_text(value) -> str:
    return escape(str(value), {'"': "&quot;", "'": "&apos;"})


def sheet_xml(rows: Sequence[Sequence[object]]) -> str:
    max_column_count = max((len(row) for row in rows), default=0)
    max_row_count = len(rows)
    dimension = "A1"
    if max_column_count and max_row_count:
        dimension = "A1:{}{}".format(excel_column_name(max_column_count), max_row_count)
    column_widths = [12] * max_column_count
    for row in rows:
        for index, value in enumerate(row):
            column_widths[index] = min(max(column_widths[index], len(str(value)) + 2), 90)

    columns = "".join(
        '<col min="{0}" max="{0}" width="{1}" customWidth="1"/>'.format(index + 1, width)
        for index, width in enumerate(column_widths)
    )
    row_xml = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for column_index, value in enumerate(row, start=1):
            if value is None:
                continue
            reference = "{}{}".format(excel_column_name(column_index), row_index)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                cells.append('<c r="{0}"><v>{1}</v></c>'.format(reference, value))
            else:
                cells.append(
                    '<c r="{0}" t="inlineStr"><is><t>{1}</t></is></c>'.format(
                        reference,
                        xml_text(value),
                    )
                )
        row_xml.append('<row r="{0}">{1}</row>'.format(row_index, "".join(cells)))

    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <dimension ref="{dimension}"/>
  <sheetViews>
    <sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView>
  </sheetViews>
  <cols>{columns}</cols>
  <sheetData>{rows}</sheetData>
</worksheet>""".format(dimension=dimension, columns=columns, rows="".join(row_xml))


def write_xlsx(path: str, sheets: Sequence[Tuple[str, Sequence[Sequence[object]]]]) -> None:
    workbook_sheets = "".join(
        '<sheet name="{0}" sheetId="{1}" r:id="rId{1}"/>'.format(xml_text(name), index)
        for index, (name, _) in enumerate(sheets, start=1)
    )
    workbook_rels = "".join(
        '<Relationship Id="rId{0}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{0}.xml"/>'.format(index)
        for index, _ in enumerate(sheets, start=1)
    )
    sheet_overrides = "".join(
        '<Override PartName="/xl/worksheets/sheet{0}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'.format(index)
        for index, _ in enumerate(sheets, start=1)
    )

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
  {sheet_overrides}
</Types>""".format(sheet_overrides=sheet_overrides))
        workbook.writestr("_rels/.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>""")
        workbook.writestr("xl/workbook.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>{workbook_sheets}</sheets>
</workbook>""".format(workbook_sheets=workbook_sheets))
        workbook.writestr("xl/_rels/workbook.xml.rels", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  {workbook_rels}
  <Relationship Id="rId{styles_id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>""".format(workbook_rels=workbook_rels, styles_id=len(sheets) + 1))
        workbook.writestr("xl/styles.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="1"><fill><patternFill patternType="none"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
  <dxfs count="0"/>
  <tableStyles count="0" defaultTableStyle="TableStyleMedium9" defaultPivotStyle="PivotStyleLight16"/>
</styleSheet>""")
        workbook.writestr("docProps/core.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <dc:creator>benchmark_live_map_apis.py</dc:creator>
</cp:coreProperties>""")
        workbook.writestr("docProps/app.xml", """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">
  <Application>Python</Application>
</Properties>""")
        for index, (_, rows) in enumerate(sheets, start=1):
            workbook.writestr("xl/worksheets/sheet{}.xml".format(index), sheet_xml(rows))


def write_excel(
    path: str,
    results: Sequence[BenchmarkResult],
    summary: dict,
    scenarios: Sequence[GeoLayerScenario],
) -> None:
    result_rows = [(
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
    for result in results:
        result_rows.append((
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
    for key, value in summary.items():
        summary_rows.append((key, json.dumps(value) if isinstance(value, dict) else value))

    combination_rows = [("country_code", "country_id", "layer_id", "admin1_count", "generated_combinations")]
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
        ("combinations", combination_rows),
    ))


def print_summary(summary: dict) -> None:
    print("\nSummary")
    print("api,calls,success,success_rate,min_ms,mean_ms,max_ms,statuses")
    print(
        "{api},{calls},{success},{success_rate:.1%},{min_ms:.2f},{mean_ms:.2f},"
        "{max_ms:.2f},{statuses}".format(**summary)
    )


def failed_results(results: Sequence[BenchmarkResult]) -> List[BenchmarkResult]:
    return [
        result
        for result in results
        if result.error or result.status is None or not (200 <= result.status < 300)
    ]


def print_failed_results(results: Sequence[BenchmarkResult], limit: Optional[int] = None) -> None:
    failures = failed_results(results)
    if not failures:
        return

    print("\nFailed API calls")
    displayed = failures if limit is None else failures[:limit]
    for result in displayed:
        status = result.status if result.status is not None else "error"
        error = result.error or "HTTP {}".format(status)
        print("[{}] {} | {} | {}".format(status, result.label, error, result.url))
    if limit is not None and len(failures) > limit:
        print("... {} more failed calls omitted".format(len(failures) - limit))


def print_geo_layer_combination_count(scenarios: Sequence[GeoLayerScenario]) -> None:
    print("\nGenerated country/admin1/layer combinations")
    total = 0
    country_ids = set()
    layer_ids = set()
    for scenario in scenarios:
        count = 1 + len(scenario.admin1_ids)
        total += count
        country_ids.add(scenario.country_id)
        layer_ids.add(scenario.layer_id)
        print(
            "{}: country_id={}, layer_id={}, admin1={}, combinations={}".format(
                scenario.country_code,
                scenario.country_id,
                scenario.layer_id,
                len(scenario.admin1_ids),
                count,
            )
        )
    print("Countries: {}".format(len(country_ids)))
    print("Country/layer pairs: {}".format(len(scenarios)))
    print("Unique layer IDs: {}".format(len(layer_ids)))
    print("Total generated combinations: {}".format(total))
    print("Formula: for each country/layer pair, 1 country-level call + N admin1 calls.")
