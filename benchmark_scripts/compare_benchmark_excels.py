"""
Compare two connectivityconfigs benchmark Excel reports.

The benchmark report contains timing metadata plus one API response per row.
This script compares the generated `results` sheets by request key and checks
whether the API responses stayed the same after a code change.

Example:
    python3 postgres_data/benchmark_scripts/compare_benchmark_excels.py \
        before.xlsx \
        after.xlsx \
        --diff-output connectivityconfigs-response-diff.csv
"""

import argparse
import csv
import json
import sys
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit
from xml.etree import ElementTree


XLSX_MAIN_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
XLSX_RELS_NS = "{http://schemas.openxmlformats.org/package/2006/relationships}"
OFFICE_RELS_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


@dataclass(frozen=True)
class ResultRow:
    source_row: int
    label: str
    method: str
    status: str
    error: str
    query_params: str
    url: str
    response_json: str


@dataclass(frozen=True)
class NormalizedResponse:
    comparable: str
    display: str
    status: str
    error: str
    truncated: bool


def column_index(cell_reference: str) -> int:
    letters = "".join(char for char in cell_reference if char.isalpha())
    index = 0
    for char in letters:
        index = index * 26 + ord(char.upper()) - ord("A") + 1
    return index - 1


def xml_text(element: Optional[ElementTree.Element]) -> str:
    if element is None:
        return ""
    return "".join(element.itertext())


def read_shared_strings(workbook: zipfile.ZipFile) -> List[str]:
    try:
        root = ElementTree.fromstring(workbook.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    strings = []
    for item in root.findall("{}si".format(XLSX_MAIN_NS)):
        strings.append(xml_text(item))
    return strings


def sheet_paths(workbook: zipfile.ZipFile) -> Dict[str, str]:
    workbook_root = ElementTree.fromstring(workbook.read("xl/workbook.xml"))
    rels_root = ElementTree.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
    rel_targets = {}
    for rel in rels_root.findall("{}Relationship".format(XLSX_RELS_NS)):
        rel_targets[rel.attrib["Id"]] = rel.attrib["Target"].lstrip("/")

    paths = {}
    for sheet in workbook_root.findall(".//{}sheet".format(XLSX_MAIN_NS)):
        rel_id = sheet.attrib.get("{}id".format(OFFICE_RELS_NS))
        target = rel_targets.get(rel_id)
        if not target:
            continue
        if not target.startswith("xl/"):
            target = "xl/{}".format(target)
        paths[sheet.attrib["name"]] = target
    return paths


def read_sheet_rows(path: str, sheet_name: str) -> List[List[str]]:
    with zipfile.ZipFile(path) as workbook:
        paths = sheet_paths(workbook)
        sheet_path = paths.get(sheet_name)
        if not sheet_path:
            available = ", ".join(sorted(paths)) or "none"
            raise ValueError("{} does not contain sheet {!r}. Available sheets: {}".format(
                path,
                sheet_name,
                available,
            ))

        shared_strings = read_shared_strings(workbook)
        root = ElementTree.fromstring(workbook.read(sheet_path))
        rows = []
        for row in root.findall(".//{}row".format(XLSX_MAIN_NS)):
            values = []
            for cell in row.findall("{}c".format(XLSX_MAIN_NS)):
                index = column_index(cell.attrib.get("r", "A"))
                while len(values) <= index:
                    values.append("")
                cell_type = cell.attrib.get("t")
                if cell_type == "inlineStr":
                    values[index] = xml_text(cell.find("{}is".format(XLSX_MAIN_NS)))
                elif cell_type == "s":
                    shared_index = int(xml_text(cell.find("{}v".format(XLSX_MAIN_NS))) or 0)
                    values[index] = shared_strings[shared_index] if shared_index < len(shared_strings) else ""
                else:
                    values[index] = xml_text(cell.find("{}v".format(XLSX_MAIN_NS)))
            rows.append(values)
        return rows


def row_value(row: Sequence[str], header_indexes: Dict[str, int], column: str) -> str:
    index = header_indexes[column]
    return row[index] if index < len(row) else ""


def read_results(path: str, sheet_name: str) -> List[ResultRow]:
    rows = read_sheet_rows(path, sheet_name)
    if not rows:
        return []
    headers = {name: index for index, name in enumerate(rows[0])}
    required = ("label", "method", "status", "error", "query_params", "url", "response_json")
    missing = [name for name in required if name not in headers]
    if missing:
        raise ValueError("{} is missing required columns: {}".format(path, ", ".join(missing)))

    results = []
    for index, row in enumerate(rows[1:], start=2):
        results.append(ResultRow(
            source_row=index,
            label=row_value(row, headers, "label"),
            method=row_value(row, headers, "method"),
            status=row_value(row, headers, "status"),
            error=row_value(row, headers, "error"),
            query_params=row_value(row, headers, "query_params"),
            url=row_value(row, headers, "url"),
            response_json=row_value(row, headers, "response_json"),
        ))
    return results


def canonical_query_params(raw_query: str) -> str:
    if not raw_query:
        return ""
    pairs = parse_qsl(raw_query, keep_blank_values=True)
    return urlencode(sorted(pairs))


def request_key(row: ResultRow, key_columns: Sequence[str]) -> str:
    values = []
    for column in key_columns:
        value = getattr(row, column)
        if column == "url":
            split_url = urlsplit(value)
            value = "{}?{}".format(split_url.path, canonical_query_params(split_url.query))
        elif column == "query_params":
            value = canonical_query_params(value)
        values.append("{}={}".format(column, value))
    return " | ".join(values)


def normalize_response(row: ResultRow, ignore_json_keys: Sequence[str]) -> NormalizedResponse:
    raw_response = row.response_json
    truncated = raw_response.endswith("...<truncated>")
    comparable = raw_response[:-14] if truncated else raw_response
    try:
        parsed = json.loads(comparable)
        if ignore_json_keys:
            parsed = remove_json_keys(parsed, set(ignore_json_keys))
        comparable = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        display = json.dumps(parsed, sort_keys=True)
    except json.JSONDecodeError:
        display = comparable
    display = "status={} error={} response={}".format(row.status, row.error, display)
    comparable_with_status = "status={};error={};response={}".format(row.status, row.error, comparable)
    return NormalizedResponse(
        comparable=comparable_with_status,
        display=display,
        status=row.status,
        error=row.error,
        truncated=truncated,
    )


def remove_json_keys(value, ignored_keys: set):
    if isinstance(value, dict):
        return {
            key: remove_json_keys(child, ignored_keys)
            for key, child in value.items()
            if key not in ignored_keys
        }
    if isinstance(value, list):
        return [remove_json_keys(item, ignored_keys) for item in value]
    return value


def summarize_by_key(
    rows: Sequence[ResultRow],
    key_columns: Sequence[str],
    ignore_json_keys: Sequence[str],
) -> Tuple[Dict[str, NormalizedResponse], List[str]]:
    grouped: Dict[str, List[NormalizedResponse]] = {}
    for row in rows:
        key = request_key(row, key_columns)
        grouped.setdefault(key, []).append(normalize_response(row, ignore_json_keys))

    unstable_keys = []
    summary = {}
    for key, responses in grouped.items():
        unique = {response.comparable for response in responses}
        if len(unique) > 1:
            unstable_keys.append(key)
        summary[key] = responses[0]
    return summary, unstable_keys


def shortened(value: str, max_chars: int) -> str:
    if max_chars <= 0 or len(value) <= max_chars:
        return value
    return value[:max_chars] + "...<truncated>"


def compare_results(
    before_rows: Sequence[ResultRow],
    after_rows: Sequence[ResultRow],
    key_columns: Sequence[str],
    ignore_json_keys: Sequence[str],
) -> Tuple[List[Tuple[str, str, str, str]], List[str], List[str]]:
    before, before_unstable = summarize_by_key(before_rows, key_columns, ignore_json_keys)
    after, after_unstable = summarize_by_key(after_rows, key_columns, ignore_json_keys)

    differences = []
    all_keys = sorted(set(before) | set(after))
    for key in all_keys:
        before_response = before.get(key)
        after_response = after.get(key)
        if before_response is None:
            differences.append(("added", key, "", after_response.display))
        elif after_response is None:
            differences.append(("removed", key, before_response.display, ""))
        elif before_response.comparable != after_response.comparable:
            differences.append(("changed", key, before_response.display, after_response.display))
        elif before_response.truncated or after_response.truncated:
            differences.append(("truncated", key, before_response.display, after_response.display))
    return differences, before_unstable, after_unstable


def write_diff_csv(path: str, differences: Sequence[Tuple[str, str, str, str]], max_chars: int) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("change_type", "request_key", "before_response", "after_response"))
        for change_type, key, before_response, after_response in differences:
            writer.writerow((
                change_type,
                key,
                shortened(before_response, max_chars),
                shortened(after_response, max_chars),
            ))


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", help="Baseline benchmark Excel file")
    parser.add_argument("after", help="Benchmark Excel file generated after the change")
    parser.add_argument("--sheet", default="results", help="Worksheet name to compare")
    parser.add_argument(
        "--key-columns",
        default="method,query_params",
        help="Comma-separated ResultRow fields used to match calls. Common values: label, method, query_params, url.",
    )
    parser.add_argument(
        "--ignore-json-key",
        action="append",
        default=[],
        help="JSON object key to ignore anywhere in the response. Can be passed multiple times.",
    )
    parser.add_argument(
        "--diff-output",
        help="Optional CSV file to write row-level response differences",
    )
    parser.add_argument(
        "--max-diff-chars",
        type=int,
        default=3000,
        help="Maximum response characters per CSV diff cell. Use 0 for full response.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    key_columns = tuple(column.strip() for column in args.key_columns.split(",") if column.strip())
    valid_key_columns = set(ResultRow.__dataclass_fields__)
    invalid_key_columns = [column for column in key_columns if column not in valid_key_columns]
    if invalid_key_columns:
        print("Invalid --key-columns: {}".format(", ".join(invalid_key_columns)), file=sys.stderr)
        return 2

    before_rows = read_results(args.before, args.sheet)
    after_rows = read_results(args.after, args.sheet)
    differences, before_unstable, after_unstable = compare_results(
        before_rows,
        after_rows,
        key_columns,
        args.ignore_json_key,
    )

    changed = sum(1 for change_type, _, _, _ in differences if change_type == "changed")
    added = sum(1 for change_type, _, _, _ in differences if change_type == "added")
    removed = sum(1 for change_type, _, _, _ in differences if change_type == "removed")
    truncated = sum(1 for change_type, _, _, _ in differences if change_type == "truncated")

    print("Compared {} baseline rows with {} candidate rows.".format(len(before_rows), len(after_rows)))
    print("Matched by: {}".format(", ".join(key_columns)))
    print("Differences: changed={}, added={}, removed={}, truncated={}".format(
        changed,
        added,
        removed,
        truncated,
    ))
    if before_unstable:
        print("Warning: baseline has {} request keys with inconsistent repeated responses.".format(
            len(before_unstable),
        ))
    if after_unstable:
        print("Warning: candidate has {} request keys with inconsistent repeated responses.".format(
            len(after_unstable),
        ))

    if args.diff_output:
        write_diff_csv(args.diff_output, differences, args.max_diff_chars)
        print("Wrote diff CSV: {}".format(args.diff_output))

    if differences:
        print("\nFirst differences:")
        for change_type, key, before_response, after_response in differences[:10]:
            print("- {}: {}".format(change_type, key))
            if change_type == "changed":
                print("  before: {}".format(shortened(before_response, 300)))
                print("  after:  {}".format(shortened(after_response, 300)))
        return 1

    print("API responses match for all comparable request keys.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
