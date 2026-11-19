import csv
import io
import logging
import os

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models.functions import Lower
from django.urls import reverse
from django.utils import timezone

import requests

from proco.core.utils import is_blank_string
from proco.locations.models import Country
from proco.utils.cache import cache_manager
from proco.utils.tasks import update_cached_value

logger = logging.getLogger('gigamaps.' + __name__)

CODE_COLUMN = 'code'
DESCRIPTION_FIELDS = ('data_source_description', 'health_data_source_description')
COUNTRY_PLACEHOLDER = '{country}'

# Soft cache prefixes of the responses that serialize the descriptions (BaseCountrySerializer):
# v1 and v2 country list and detail.
DESCRIPTION_CACHE_KEYS = (
    'COUNTRIES_LIST*',
    'COUNTRY_INFO_pk_*',
    'V2_ENTITY_COUNTRIES_LIST*',
    'V2_ENTITY_COUNTRY_DETAIL_pk_*',
)


class DescriptionLoadPlan(object):
    """
    DescriptionLoadPlan
        Result of comparing the CSV rows with the current Country values.
        changes: list of (country, {field: new value}) for the fields that change
        kept: list of messages for fields left as they are because of --only-empty
        issues: list of messages for rows or fields that were skipped
    """

    def __init__(self):
        self.changes = []
        self.kept = []
        self.issues = []


def read_csv_text(file_path=None, url=None):
    """
    read_csv_text
        Read the CSV content from a local file or from an https URL

    :param file_path: absolute path of the CSV file
    :param url: https URL that returns the CSV with a plain GET
    :return: CSV content as text
    """
    if url:
        if not url.lower().startswith('https://'):
            raise CommandError("Only https URLs are supported, found '{0}'.".format(url))
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
        except requests.RequestException as ex:
            raise CommandError('Could not download the CSV: {0}'.format(ex))
        content = response.content
    else:
        if not file_path or not os.path.isfile(file_path):
            raise CommandError("Provide a valid and an absolute file path, found '{0}'.".format(file_path))
        with open(file_path, 'rb') as csv_file:
            content = csv_file.read()

    # utf-8-sig drops the BOM that Excel and some spreadsheet exports put before the first header
    return content.decode('utf-8-sig')


def parse_rows(csv_text):
    """
    parse_rows
        Parse the CSV text into rows with the code and the description columns, stripped

    :param csv_text: CSV content
    :return: list of (line number, {column: value})
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    columns = [str(column).strip() for column in (reader.fieldnames or [])]
    missing_columns = [column for column in (CODE_COLUMN,) + DESCRIPTION_FIELDS if column not in columns]
    if missing_columns:
        raise CommandError('Missing CSV column(s): {0}. Found: {1}'.format(
            ', '.join(missing_columns), ', '.join(columns)[:200]))
    reader.fieldnames = columns

    rows = []
    for row in reader:
        rows.append((reader.line_num, {
            column: (row.get(column) or '').strip()
            for column in (CODE_COLUMN,) + DESCRIPTION_FIELDS
        }))
    return rows


def plan_changes(rows, only_empty=False):
    """
    plan_changes
        Compare the CSV rows with the current Country values.
        A blank cell leaves the field untouched, and {country} is replaced by the country name.

    :param rows: output of parse_rows
    :param only_empty: if True, fields that already have a value are kept
    :return: DescriptionLoadPlan
    """
    plan = DescriptionLoadPlan()

    countries_by_code = {}
    for country in Country.objects.annotate(code_lower=Lower('code')).defer('geometry').order_by('id'):
        countries_by_code.setdefault(country.code_lower, []).append(country)

    seen_codes = set()
    for line_no, row in rows:
        code = row[CODE_COLUMN]
        if is_blank_string(code):
            plan.issues.append('line {0}: empty code, row skipped.'.format(line_no))
            continue

        code_key = code.lower()
        if code_key in seen_codes:
            plan.issues.append("line {0}: code '{1}' is repeated, row skipped.".format(line_no, code))
            continue
        seen_codes.add(code_key)

        countries = countries_by_code.get(code_key)
        if not countries:
            plan.issues.append("line {0}: unknown country code '{1}', row skipped.".format(line_no, code))
            continue

        for country in countries:
            values = {}
            for field_name in DESCRIPTION_FIELDS:
                text = row[field_name]
                if is_blank_string(text):
                    continue

                text = text.replace(COUNTRY_PLACEHOLDER, country.name)
                max_length = Country._meta.get_field(field_name).max_length
                if len(text) > max_length:
                    plan.issues.append(
                        'line {0}: {1} {2} has {3} characters after substitution, the limit is {4}; '
                        'field skipped.'.format(line_no, country.code, field_name, len(text), max_length))
                    continue

                current_text = getattr(country, field_name) or ''
                if text == current_text:
                    continue
                if only_empty and not is_blank_string(current_text):
                    plan.kept.append('{0} {1}: already has a value, kept (--only-empty).'.format(
                        country.code, field_name))
                    continue

                values[field_name] = text

            if values:
                plan.changes.append((country, values))

    return plan


def refresh_country_description_cache(country_codes):
    """
    refresh_country_description_cache
        Mark the cached country list/detail responses as stale and queue their rebuild in Celery.
        Country.save() is not used: its invalidate_country_related_cache runs 13 Redis KEYS scans per
        country, mostly for responses (schools, layers, filters) that do not contain the descriptions.

    :param country_codes: codes of the countries that changed
    """
    cache_manager.invalidate(DESCRIPTION_CACHE_KEYS)

    for country_code in sorted({code.lower() for code in country_codes}):
        update_cached_value.delay(url=reverse('locations:countries-detail', kwargs={'pk': country_code}))
        update_cached_value.delay(url=reverse('entities:retrieve-entity-country', kwargs={'pk': country_code}))
    update_cached_value.delay(url=reverse('locations:countries-list'))
    update_cached_value.delay(url=reverse('entities:list-entity-countries'))


def apply_changes(plan):
    """
    apply_changes
        Write the planned description changes and refresh the related cache

    :param plan: DescriptionLoadPlan
    """
    now = timezone.now()
    with transaction.atomic():
        for country, values in plan.changes:
            Country.objects.filter(id=country.id).update(modified=now, **values)

    refresh_country_description_cache([country.code for country, _ in plan.changes])


class Command(BaseCommand):
    help = (
        'Load Country.data_source_description and Country.health_data_source_description from a CSV with the '
        'columns code, data_source_description, health_data_source_description (other columns are ignored). '
        'A blank cell leaves that field as it is; {country} is replaced by the country name. '
        'Run with --dry-run first.'
    )

    def add_arguments(self, parser):
        source = parser.add_mutually_exclusive_group(required=True)
        source.add_argument(
            '-cf',
            '--country-file',
            type=str,
            dest='country_file',
            help='Absolute path of the CSV file.',
        )
        source.add_argument(
            '--url',
            type=str,
            dest='url',
            help='https URL that returns the CSV with a plain GET (e.g. a spreadsheet published as CSV).',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            dest='dry_run',
            default=False,
            help='Print the per-country changes without writing anything.',
        )
        parser.add_argument(
            '--only-empty',
            action='store_true',
            dest='only_empty',
            default=False,
            help='Only fill fields that are empty today; by default a non-blank CSV cell overwrites the field.',
        )

    def handle(self, **options):
        dry_run = options.get('dry_run')
        csv_text = read_csv_text(file_path=options.get('country_file'), url=options.get('url'))
        plan = plan_changes(parse_rows(csv_text), only_empty=options.get('only_empty'))

        prefix = '[DRY RUN] ' if dry_run else ''
        for country, values in plan.changes:
            self.stdout.write('{0}{1} {2}'.format(prefix, country.code, country.name))
            for field_name, text in values.items():
                self.stdout.write('  {0}\n    old: {1!r}\n    new: {2!r}'.format(
                    field_name, getattr(country, field_name) or '', text))
        for message in plan.kept:
            self.stdout.write('{0}{1}'.format(prefix, message))
        for message in plan.issues:
            self.stderr.write('{0}{1}'.format(prefix, message))

        fields_count = sum(len(values) for _, values in plan.changes)
        summary = '{0}countries to update: {1}, fields: {2}, kept: {3}, skipped: {4}'.format(
            prefix, len(plan.changes), fields_count, len(plan.kept), len(plan.issues))

        if dry_run or not plan.changes:
            self.stdout.write(summary + '\nNothing written.')
            return

        apply_changes(plan)
        logger.info(
            'Country data source descriptions loaded for: %s',
            ', '.join(country.code for country, _ in plan.changes),
        )
        self.stdout.write(summary + '\nWritten. Cache rebuild queued in Celery.')
