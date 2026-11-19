import csv
import logging
import os

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from proco.locations.models import Country
from proco.utils.cache import cache_manager

logger = logging.getLogger('gigamaps.' + __name__)

ISO3_COLUMN = 'iso3_format'
SCHOOL_DATA_TYPE_COLUMN = 'OSM_Govt_final'

CSV_VALUE_TO_SCHOOL_DATA_TYPE = {
    'government': Country.SCHOOL_DATA_TYPE_GOVERNMENT,
    'osm': Country.SCHOOL_DATA_TYPE_OSM,
    'not mapped': Country.SCHOOL_DATA_TYPE_NOT_MAPPED,
}

# Every serialized form of a Country that exposes school_data_type. Invalidated once per run instead of
# calling Country.invalidate_country_related_cache() per row, which would scan Redis keys ~14 times per country.
SCHOOL_DATA_TYPE_CACHE_KEYS = (
    '*GLOBAL_COUNTRY_SEARCH_MAPPING_*',
    'COUNTRIES_LIST*',
    'COUNTRY_INFO_pk_*',
    '*V2_ENTITY_COUNTRIES_LIST_*',
    '*V2_ENTITY_COUNTRY_DETAIL_pk_*',
)


def read_school_data_types(input_file):
    """
    read_school_data_types
        Read the curated country CSV and map each ISO3 code to a Country.school_data_type value.
        The file is exported with a legacy encoding; only the ISO3 and type columns are used and they are
        ASCII, so latin-1 decoding is enough and never fails.

    :param input_file: path to the CSV file
    :return: (dict iso3 -> school_data_type, list of error strings)
    """
    if not os.path.isfile(input_file):
        raise CommandError('File not found: {0}'.format(input_file))

    with open(input_file, encoding='latin-1', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        missing_columns = {ISO3_COLUMN, SCHOOL_DATA_TYPE_COLUMN} - set(reader.fieldnames or [])
        if missing_columns:
            raise CommandError('Missing CSV columns: {0}'.format(', '.join(sorted(missing_columns))))

        types_by_iso3 = {}
        errors = []
        for line_number, row in enumerate(reader, start=2):
            iso3 = (row.get(ISO3_COLUMN) or '').strip().upper()
            raw_value = (row.get(SCHOOL_DATA_TYPE_COLUMN) or '').strip()
            if not iso3:
                errors.append('line {0}: empty {1}'.format(line_number, ISO3_COLUMN))
                continue

            school_data_type = CSV_VALUE_TO_SCHOOL_DATA_TYPE.get(raw_value.lower())
            if school_data_type is None:
                errors.append('line {0}: {1} has unknown {2} value "{3}"'.format(
                    line_number, iso3, SCHOOL_DATA_TYPE_COLUMN, raw_value))
                continue

            if iso3 in types_by_iso3 and types_by_iso3[iso3] != school_data_type:
                errors.append('line {0}: {1} appears more than once with different values'.format(line_number, iso3))
                continue
            types_by_iso3[iso3] = school_data_type

    return types_by_iso3, errors


class Command(BaseCommand):
    help = 'Load Country.school_data_type from the curated country CSV (columns iso3_format and OSM_Govt_final).'

    def add_arguments(self, parser):
        parser.add_argument('--file', dest='input_file', required=True, help='Path to the country CSV file.')
        parser.add_argument(
            '--dry-run', action='store_true', dest='dry_run', default=False,
            help='Report what would change without writing to the database or the cache.',
        )

    def handle(self, **options):
        dry_run = options['dry_run']
        types_by_iso3, errors = read_school_data_types(options['input_file'])
        if errors:
            raise CommandError('Invalid CSV, nothing written:\n  {0}'.format('\n  '.join(errors)))

        countries_by_iso3 = {}
        duplicated_iso3 = set()
        db_without_iso3 = []
        for country in Country.objects.all().only('id', 'name', 'code', 'iso3_format', 'school_data_type'):
            iso3 = (country.iso3_format or '').strip().upper()
            if not iso3:
                db_without_iso3.append(country)
            elif iso3 in countries_by_iso3:
                duplicated_iso3.add(iso3)
            else:
                countries_by_iso3[iso3] = country

        if duplicated_iso3:
            raise CommandError('Several active countries share the ISO3 code(s) {0}, nothing written.'.format(
                ', '.join(sorted(duplicated_iso3))))

        csv_without_country = sorted(set(types_by_iso3) - set(countries_by_iso3))
        db_without_csv_row = sorted(set(countries_by_iso3) - set(types_by_iso3))

        changes = []
        for iso3, school_data_type in sorted(types_by_iso3.items()):
            country = countries_by_iso3.get(iso3)
            if country and country.school_data_type != school_data_type:
                changes.append((iso3, country, school_data_type))

        if not dry_run and changes:
            with transaction.atomic():
                for _iso3, country, school_data_type in changes:
                    Country.objects.filter(id=country.id).update(school_data_type=school_data_type)
            cache_manager.invalidate(SCHOOL_DATA_TYPE_CACHE_KEYS)

        self._report(dry_run, types_by_iso3, countries_by_iso3, changes,
                     csv_without_country, db_without_csv_row, db_without_iso3)

    def _report(self, dry_run, types_by_iso3, countries_by_iso3, changes,
                csv_without_country, db_without_csv_row, db_without_iso3):
        write = self.stdout.write
        write('{0}: {1} CSV rows, {2} active countries with an ISO3 code'.format(
            'DRY RUN' if dry_run else 'APPLIED', len(types_by_iso3), len(countries_by_iso3)))

        write('\nChanges {0} ({1}):'.format('that would be applied' if dry_run else 'applied', len(changes)))
        for iso3, country, school_data_type in changes:
            write('  {0} {1} (id {2}): {3} -> {4}'.format(
                iso3, country.name, country.id, country.school_data_type, school_data_type))

        write('\nCSV rows without an active country ({0}): {1}'.format(
            len(csv_without_country), ', '.join(csv_without_country) or '-'))
        write('Active countries without a CSV row, left unchanged ({0}): {1}'.format(
            len(db_without_csv_row),
            ', '.join('{0} {1}'.format(iso3, countries_by_iso3[iso3].name) for iso3 in db_without_csv_row) or '-'))
        write('Active countries without an ISO3 code, left unchanged ({0}): {1}'.format(
            len(db_without_iso3), ', '.join('{0} (id {1})'.format(c.name, c.id) for c in db_without_iso3) or '-'))
