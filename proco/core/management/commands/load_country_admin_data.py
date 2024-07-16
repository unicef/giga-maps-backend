import json
import os
import sys
import logging

import numpy as np
import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models.functions import Lower

from proco.core import utils as core_utilities
from proco.locations.models import Country, CountryAdminMetadata

logger = logging.getLogger('gigamaps.' + __name__)


def is_file(fp):
    if not os.path.isfile(fp):
        raise CommandError("Provide a valid and an absolute file path, found '{0}'.".format(fp))
    return True


def get_country(country_code, countries):
    # If its blank string then put default value
    if pd.isna(country_code) or core_utilities.is_blank_string(country_code):
        return None

    # Remove space from start and end if present
    country_code = str(country_code).strip().lower()

    return countries.get(country_code)


def get_file_name_and_extension(file_name):
    return '.'.join(file_name.split('.')[:-1]), file_name.split('.')[-1]


def load_admin0_file_data(file_path):
    is_file(file_path)
    input_df = pd.read_csv(file_path, delimiter=',', quotechar='"', keep_default_na=False)
    input_df.dropna(how='all', inplace=True)

    input_df = input_df.applymap(core_utilities.sanitize_str)

    csv_required_cols = ['iso31661', 'iso31661alpha3', 'name', 'nameen', 'description', 'centroid', 'bbox', 'mapboxid']

    core_utilities.column_normalize(input_df, valid_columns=csv_required_cols)
    logger.debug('Csv normalized columns: {0}.'.format(input_df.columns.tolist()))

    input_df.drop_duplicates(subset=['iso31661', 'iso31661alpha3'], keep='last', inplace=True)

    country_codes = dict(Country.objects.all().annotate(code_lower=Lower('code')).values_list('code_lower', 'id'))
    country_iso3_codes = dict(
        Country.objects.all().annotate(iso3_format_lower=Lower('iso3_format')).values_list('iso3_format_lower', 'id'))
    logger.debug('Country code mapping: {0}'.format(country_codes))
    logger.debug('Country ISO3 code mapping: {0}'.format(country_iso3_codes))

    input_df['errors'] = None

    has_data_errors = False

    for index, row in input_df.iterrows():
        errors = []

        country_id = get_country(row['iso31661'], country_codes)
        if country_id is None:
            country_id = get_country(row['iso31661alpha3'], country_iso3_codes)

        input_df.at[index, 'country_id'] = country_id

        if core_utilities.is_blank_string(row['name']):
            errors.append('Name field is required')

        if len(errors) > 0:
            logger.debug('Errors: ', errors, ', Code: ', row['iso31661'], ', Country Id: ', country_id)

            has_data_errors = True
            input_df.at[index, 'errors'] = ','.join(errors)

    logger.debug('Has data errors: {0}'.format(has_data_errors))
    if has_data_errors:
        error_file = '_errors.'.join(get_file_name_and_extension(file_path))
        logger.error('Csv has data errors. Please check the error file, correct it and then start again.'
                     ' Error file: {0}'.format(error_file))
        input_df.to_csv(error_file, quotechar='"', index=False)
        return

    logger.info('Success: Validation has passed by the input file.')
    input_df = input_df.replace(np.nan, None)
    rows = input_df.to_dict(orient='records')

    for row_data in rows:
        CountryAdminMetadata.objects.update_or_create(
            country_id=row_data['country_id'],
            layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN0,
            giga_id_admin=row_data['iso31661alpha3'],  # Store ISO Alpha3 code to Giga ID
            defaults={
                'name': row_data['name'],
                'name_en': row_data['nameen'],
                'description': row_data['description'],
                'centroid': json.loads(row_data['centroid']),
                'bbox': json.loads(row_data['bbox']),
                'mapbox_id': row_data['mapboxid'],
            },
        )


def load_admin1_file_data(file_path):
    is_file(file_path)
    input_df = pd.read_csv(file_path, delimiter=',', quotechar='"', keep_default_na=False)
    input_df.dropna(how='all', inplace=True)

    input_df = input_df.applymap(core_utilities.sanitize_str)

    csv_required_cols = ['iso31661', 'iso31661alpha3',
                         'name', 'nameen', 'description',
                         'centroid', 'bbox',
                         'admin1idgiga', 'mapboxid']

    core_utilities.column_normalize(input_df, valid_columns=csv_required_cols)

    logger.debug('Csv normalized columns: {0}'.format(input_df.columns.tolist()))

    input_df.drop_duplicates(subset=['iso31661', 'iso31661alpha3', 'admin1idgiga'], keep='last', inplace=True)

    country_codes = dict(Country.objects.all().annotate(code_lower=Lower('code')).values_list('code_lower', 'id'))
    logger.debug('Country code mapping: {0}'.format(country_codes))

    country_iso3_codes = dict(
        Country.objects.all().annotate(iso3_format_lower=Lower('iso3_format')).values_list('iso3_format_lower', 'id'))
    logger.debug('Country ISO3 code mapping: {0}'.format(country_iso3_codes))

    parent_code_vs_id = dict(
        CountryAdminMetadata.objects.all().filter(
            layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN0).annotate(
            giga_id_admin_lower=Lower('giga_id_admin')).values_list(
            'giga_id_admin_lower', 'id'),
    )
    logger.debug('Country giga id code - ID mapping: {0}'.format(parent_code_vs_id))

    input_df['errors'] = None

    has_data_errors = False

    for index, row in input_df.iterrows():
        errors = []

        country_id = get_country(row['iso31661'], country_codes)
        if country_id is None:
            country_id = get_country(row['iso31661alpha3'], country_iso3_codes)

        input_df.at[index, 'country_id'] = country_id

        parent_id = get_country(row['iso31661alpha3'], parent_code_vs_id)
        if parent_id is None:
            parent_id = get_country(row['iso31661'], parent_code_vs_id)
        input_df.at[index, 'parent_id'] = parent_id

        if core_utilities.is_blank_string(row['name']):
            errors.append('Name field is required')

        if len(errors) > 0:
            logger.debug('Errors: ', errors, ', Code: ', row['admin1idgiga'], ', Country Id: ', country_id)

            has_data_errors = True
            input_df.at[index, 'errors'] = ','.join(errors)

    logger.debug('Has data errors: {0}'.format(has_data_errors))
    if has_data_errors:
        error_file = '_errors.'.join(get_file_name_and_extension(file_path))
        logger.error('Csv has data errors. Please check the error file, correct it and then start again.'
                     ' Error file: {0}'.format(error_file))
        input_df.to_csv(error_file, quotechar='"', index=False)
        return

    logger.info('Success: Validation has passed by the input file.')
    input_df = input_df.replace(np.nan, None)
    rows = input_df.to_dict(orient='records')

    for row_data in rows:
        CountryAdminMetadata.objects.update_or_create(
            country_id=row_data['country_id'],
            layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN1,
            giga_id_admin=row_data['admin1idgiga'],
            defaults={
                'name': row_data['name'],
                'name_en': row_data['nameen'],
                'description': row_data['description'],
                'centroid': json.loads(row_data['centroid']),
                'bbox': json.loads(row_data['bbox']),
                'parent_id': row_data['parent_id'],
                'mapbox_id': row_data['mapboxid'],
            },
        )


def load_admin2_file_data(file_path):
    is_file(file_path)
    input_df = pd.read_csv(file_path, delimiter=',', quotechar='"', keep_default_na=False)
    input_df.dropna(how='all', inplace=True)

    input_df = input_df.applymap(core_utilities.sanitize_str)

    csv_required_cols = ['iso31661', 'iso31661alpha3',
                         'name', 'nameen', 'description',
                         'centroid', 'bbox',
                         'admin1idgiga', 'admin2idgiga', 'mapboxid', ]

    core_utilities.column_normalize(input_df, valid_columns=csv_required_cols)

    logger.debug('Csv normalized columns: {0}'.format(input_df.columns.tolist()))

    input_df.drop_duplicates(subset=['iso31661', 'iso31661alpha3', 'admin2idgiga'], keep='last', inplace=True)

    country_codes = dict(Country.objects.all().annotate(code_lower=Lower('code')).values_list('code_lower', 'id'))
    country_iso3_codes = dict(
        Country.objects.all().annotate(iso3_format_lower=Lower('iso3_format')).values_list('iso3_format_lower', 'id'))
    logger.debug('Country code mapping: {0}'.format(country_codes))
    logger.debug('Country ISO3 code mapping: {0}'.format(country_iso3_codes))

    parent_code_vs_id = dict(
        CountryAdminMetadata.objects.all().filter(
            layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN1).annotate(
            giga_id_admin_lower=Lower('giga_id_admin')).values_list(
            'giga_id_admin_lower', 'id'),
    )
    logger.debug('Admin1 giga id code - ID mapping: {0}'.format(parent_code_vs_id))

    input_df['errors'] = None

    has_data_errors = False

    for index, row in input_df.iterrows():
        errors = []

        country_id = get_country(row['iso31661'], country_codes)
        if country_id is None:
            country_id = get_country(row['iso31661alpha3'], country_iso3_codes)

        input_df.at[index, 'country_id'] = country_id

        parent_id = get_country(row['admin1idgiga'], parent_code_vs_id)
        input_df.at[index, 'parent_id'] = parent_id

        if core_utilities.is_blank_string(row['name']):
            errors.append('Name field is required')

        if len(errors) > 0:
            logger.error('Errors: ', errors, ', Code: ', row['admin2idgiga'], ', Country Id: ', country_id)

            has_data_errors = True
            input_df.at[index, 'errors'] = ','.join(errors)

    logger.debug('Has data errors: {0}'.format(has_data_errors))
    if has_data_errors:
        error_file = '_errors.'.join(get_file_name_and_extension(file_path))
        logger.error('Csv has data errors. Please check the error file, correct it and then start again.'
                     ' Error file: {0}'.format(error_file))
        input_df.to_csv(error_file, quotechar='"', index=False)
        return

    logger.info('Success: Validation has passed by the input file.')
    input_df = input_df.replace(np.nan, None)
    rows = input_df.to_dict(orient='records')

    for row_data in rows:
        CountryAdminMetadata.objects.update_or_create(
            country_id=row_data['country_id'],
            layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN2,
            giga_id_admin=row_data['admin2idgiga'],
            defaults={
                'name': row_data['name'],
                'name_en': row_data['nameen'],
                'description': row_data['description'],
                'centroid': json.loads(row_data['centroid']),
                'bbox': json.loads(row_data['bbox']),
                'parent_id': row_data['parent_id'],
                'mapbox_id': row_data['mapboxid'],
            },
        )


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-af',
            '--admin-file',
            type=str,
            dest='admin_file',
            help='Path to Admin CSV file.',
        )

        parser.add_argument(
            '-at',
            '--admin-type',
            type=str,
            dest='admin_type',
            help='admin0 or admin1 or admin2.',
        )

    def handle(self, **options):
        input_file = options.get('admin_file')
        if not input_file:
            sys.exit("Mandatory argument '--admin-file/-af' is missing.")

        admin_type = options.get('admin_type')
        if admin_type not in ['admin0', 'admin1', 'admin2']:
            sys.exit("Mandatory argument '--admin-type/-at' is missing. Available options: {0}".format(
                ['admin0', 'admin1', 'admin2']))

        with transaction.atomic():
            logger.info('Loading admins data for {0} ....'.format(admin_type))
            if admin_type == 'admin0':
                load_admin0_file_data(input_file)
            elif admin_type == 'admin1':
                load_admin1_file_data(input_file)
            elif admin_type == 'admin2':
                load_admin2_file_data(input_file)
            logger.info('Data loaded successfully!\n')
