import json
import logging
import os
from datetime import timedelta

import delta_sharing
import numpy as np
import pandas as pd
import pytz
import requests
from delta_sharing.protocol import Schema, Share
from delta_sharing.reader import DeltaSharingReader
from django.conf import settings
from django.db.models import Q
from django.db.models.functions import Lower
from rest_framework import status

from proco.accounts.models import APIKey
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import RealTimeConnectivity
from proco.core import utils as core_utilities
from proco.custom_auth.models import ApplicationUser
from proco.data_sources import models as sources_models
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils.dates import format_date
from proco.utils.urls import add_url_params

logger = logging.getLogger('gigamaps.' + __name__)

response_timezone = pytz.timezone(settings.TIME_ZONE)

ds_settings = settings.DATA_SOURCE_CONFIG


class ProcoSharingClient(delta_sharing.SharingClient):

    def get_share(self, share_name: str) -> Share:
        """
        Get share that can be accessed by you in a Delta Sharing Server.

        :return: the share that can be accessed.
        """
        shares = self.list_shares()
        for share in shares:
            if share.name == share_name:
                return share

    def get_schema(self, share: Share, schema_name: str) -> Schema:
        """
        Get schema in a share that can be accessed by you in a Delta Sharing Server.

        :param share: the share to list.
        :param schema_name: the schema name to get.
        :return: the schema in a share.
        """
        schemas = self.list_schemas(share)
        for schema in schemas:
            if schema.name == schema_name:
                return schema


def normalize_school_name(school_name):
    # If its blank string then put default value
    if pd.isna(school_name) or core_utilities.is_blank_string(school_name):
        return 'Name unknown'

    # Remove space from start and end if present
    # Replace the 2 times double quotes ("") with 1 time double quotes (")
    school_name = str(school_name).strip().replace('""', '"')

    # If school name start & ends with ", then remove these from start and end
    if (len(school_name) >= 2 and school_name[0] == school_name[-1]) and school_name.startswith(("'", '"')):
        school_name = school_name[1:-1]

    return school_name


def normalize_school_master_data_frame(df):
    df['school_name'] = df['school_name'].apply(normalize_school_name)
    if 'school_id_govt' in list(df.columns.tolist()):
        df['school_id_govt'] = df['school_id_govt'].fillna('thisnanwillreplaceback').apply(
            lambda val: str(val).lower()).replace('thisnanwillreplaceback', np.nan)
    else:
        df['school_id_govt'] = None
    return df


def normalize_qos_data_frame(df):
    if 'school_id_govt' in list(df.columns.tolist()):
        df['school_id_govt'] = df['school_id_govt'].fillna('thisnanwillreplaceback').apply(
            lambda val: str(val).lower()).replace('thisnanwillreplaceback', np.nan)
    else:
        df['school_id_govt'] = None
    return df


def has_changes_for_review(row, school):
    if school:
        if row['school_name'].lower() != school.name.lower():
            return True

        old_external_id = None \
            if core_utilities.is_blank_string(school.external_id) else str(school.external_id).lower()
        new_external_id = None \
            if core_utilities.is_blank_string(row['school_id_govt']) else str(row['school_id_govt']).lower()

        if old_external_id != new_external_id:
            return True

        old_admin1_id = None
        if school.admin1:
            old_admin1_id = str(school.admin1.giga_id_admin).lower()
        new_admin1_id = None \
            if core_utilities.is_blank_string(row['admin1_id_giga']) else str(row['admin1_id_giga']).lower()

        if old_admin1_id != new_admin1_id:
            return True

        old_admin2_id = None
        if school.admin2:
            old_admin2_id = str(school.admin2.giga_id_admin).lower()
        new_admin2_id = None \
            if core_utilities.is_blank_string(row['admin2_id_giga']) else str(row['admin2_id_giga']).lower()

        if old_admin2_id != new_admin2_id:
            return True

        old_lat = school.geopoint.y
        new_lat = row['latitude']
        if (
            old_lat != new_lat and
            (
                str(old_lat).split('.')[0] != str(new_lat).split('.')[0] or
                str(old_lat).split('.')[1][:5] != str(new_lat).split('.')[1][:5]
            )
        ):
            return True

        old_long = school.geopoint.x
        new_long = row['longitude']
        if (
            old_long != new_long and
            (
                str(old_long).split('.')[0] != str(new_long).split('.')[0] or
                str(old_long).split('.')[1][:5] != str(new_long).split('.')[1][:5]
            )
        ):
            return True

        old_education_level = None \
            if core_utilities.is_blank_string(school.education_level) else str(school.education_level).lower()
        new_education_level = None \
            if core_utilities.is_blank_string(row['education_level']) else str(row['education_level']).lower()

        if old_education_level != new_education_level:
            return True
        return False
    return True


def parse_row(row):
    row.replace(np.nan, None, inplace=True)
    row.replace(pd.NaT, None, inplace=True)

    for timestamp_col_name in [
        'timestamp',
        'school_location_ingestion_timestamp',
        'connectivity_RT_ingestion_timestamp',
        'connectivity_govt_ingestion_timestamp',
    ]:
        value = row.get(timestamp_col_name, None)
        if not core_utilities.is_blank_string(value):
            row[timestamp_col_name] = value.tz_localize(response_timezone)

    return row.to_dict()


def sync_school_master_data(profile_file, share_name, schema_name, table_name, changes_for_countries, deleted_schools,
                            school_master_fields):
    country = Country.objects.filter(iso3_format=table_name, ).first()
    logger.debug('Country object: {0}'.format(country))

    if not country:
        logger.error('Country with ISO3 Format ({0}) not found in DB. '
                     'Hence skipping the load for current table.'.format(table_name))
        raise ValueError(f"Invalid 'iso3_format': {table_name}")

    table_last_data_version = sources_models.SchoolMasterData.get_last_version(table_name)
    logger.debug('Table last data version present in DB: {0}'.format(table_last_data_version))

    # Create an url to access a shared table.
    # A table path is the profile file path following with `#` and the fully qualified name of a table
    # (`<share-name>.<schema-name>.<table-name>`).
    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
        share_name=share_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    logger.debug('Table URL: %s', table_url)

    table_current_version = delta_sharing.get_table_version(table_url)
    logger.debug('Table current version from API: {0}'.format(table_current_version))

    if table_last_data_version == table_current_version:
        logger.info('Both School Master data version in DB and Table version from API, are same. '
                    'Hence skipping the data update for current country ({0}).'.format(country))
        return

    table_protocol = delta_sharing.get_table_protocol(table_url)
    logger.debug('Table Protocol: {0}'.format(table_protocol))

    table_meta_data = delta_sharing.get_table_metadata(table_url)
    logger.debug('Table Metadata: {0}'.format(table_meta_data.__dict__))

    loaded_data_df = delta_sharing.load_table_changes_as_pandas(
        table_url,
        table_last_data_version,
        table_current_version,
        None,
        None,
    )
    logger.debug('Total count of rows in the data: {0}'.format(len(loaded_data_df)))

    if len(loaded_data_df) > 0:
        # Sort the values based on _commit_timestamp ASC
        loaded_data_df = loaded_data_df.sort_values(
            by=[DeltaSharingReader._commit_version_col_name(), DeltaSharingReader._commit_timestamp_col_name()],
            na_position='first')
        loaded_data_df.drop_duplicates(
            subset=['school_id_giga'],
            keep='last',
            inplace=True,
        )
        loaded_data_df = loaded_data_df[loaded_data_df[DeltaSharingReader._change_type_col_name()].isin(
            ['insert', 'update_postimage', 'remove', 'delete'])]

        logger.debug('Total count of rows in the data after duplicate cleanup: {0}'.format(len(loaded_data_df)))

        df_columns = list(loaded_data_df.columns.tolist())
        cols_to_delete = list(set(df_columns) - set(school_master_fields)) + ['id', 'created', 'modified', 'school_id',
                                                                              'country_id', 'status',
                                                                              'modified_by', 'published_by',
                                                                              'published_at', 'is_read', ]
        logger.debug('All School Master API response columns: {}'.format(df_columns))
        logger.debug('All School Master API response columns to delete: {}'.format(
            list(set(df_columns) - set(school_master_fields))))

        insert_entries = []
        remove_entries = []

        changes_for_countries[table_name] = True

        loaded_data_df = normalize_school_master_data_frame(loaded_data_df)

        loaded_data_df['version'] = table_current_version
        loaded_data_df['country'] = country

        for _, row in loaded_data_df.iterrows():
            change_type = row[DeltaSharingReader._change_type_col_name()]

            row.drop(
                labels=cols_to_delete,
                inplace=True,
                errors='ignore',
            )

            if change_type in ['insert', 'update_postimage']:
                school = School.objects.filter(
                    country=country,
                    giga_id_school=row['school_id_giga'],
                ).first()

                # 1. If it is a new school, then it has to go through review process
                # 2. If it is an existing school, then check for required field if it has changed.
                # if changes, then only send for review otherwise publish it directly
                if school:
                    row['school_id'] = school.id
                    review_required = has_changes_for_review(row, school)
                    if not review_required:
                        row['status'] = sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED
                        row['published_at'] = core_utilities.get_current_datetime_object()

                row_as_dict = parse_row(row)
                insert_entries.append(sources_models.SchoolMasterData(**row_as_dict))

                if len(insert_entries) == 5000:
                    logger.debug('Loading the data to "SchoolMasterData" table as it has reached 5000 benchmark.')
                    sources_models.SchoolMasterData.objects.bulk_create(insert_entries)
                    insert_entries = []
                    logger.debug('#' * 10)
                    logger.debug('\n\n')

            elif change_type in ['remove', 'delete']:
                school = School.objects.filter(
                    country=country,
                    giga_id_school=row['school_id_giga'],
                ).first()

                # School can be deleted only if its already present in Giga DB
                if school:
                    row['school_id'] = school.id
                    row['status'] = sources_models.SchoolMasterData.ROW_STATUS_DELETED
                    row['modified'] = core_utilities.get_current_datetime_object()

                    row_as_dict = parse_row(row)
                    remove_entries.append(sources_models.SchoolMasterData(**row_as_dict))

                if len(remove_entries) == 5000:
                    logger.info('Loading the data to "SchoolMasterData" table as it has reached 5000 benchmark.')
                    sources_models.SchoolMasterData.objects.bulk_create(remove_entries)
                    remove_entries = []
                    logger.debug('#' * 10)
                    logger.debug('\n\n')

        logger.info('Loading the remaining ({0}) data to "SchoolMasterData" table.'.format(len(insert_entries)))
        if len(insert_entries) > 0:
            sources_models.SchoolMasterData.objects.bulk_create(insert_entries)

        logger.info('Removing ({0}) records from "SchoolMasterData" table.'.format(len(remove_entries)))
        if len(remove_entries) > 0:
            sources_models.SchoolMasterData.objects.bulk_create(remove_entries)

            deleted_schools.extend(
                [country.name + ' : ' + school_master_row.school_name for school_master_row in remove_entries])
    else:
        logger.info('No data to update in current table: {0}.'.format(table_name))


def get_request_headers(request_configs):
    source_request_headers = request_configs.get('headers', {})
    auth_required = request_configs.get('auth_token_required', False)

    if auth_required:
        internal_users = list(ApplicationUser.objects.filter(
            Q(is_active=True) & (Q(is_superuser=True) | Q(is_staff=True))
        ).values_list('id', flat=True).order_by('id').distinct('id'))

        # API Key for is_staff = True user which never expires
        daily_check_app_api_keys = list(APIKey.objects.annotate(api_code_lower=Lower('api__code')).filter(
            api_code_lower=ds_settings.get('DAILY_CHECK_APP').get('API_CODE').lower(),
            status=APIKey.APPROVED,
            valid_to__gte=core_utilities.get_current_datetime_object().date(),
            user__in=internal_users,
            has_write_access=True,
        ).values_list('api_key', flat=True))

        if len(daily_check_app_api_keys) > 0:
            token = daily_check_app_api_keys[-1]
        else:
            token = 'dummy_value_to_raise_401_response_error_as_valid_key_not_available'

        source_request_headers['Authorization'] = 'Bearer {0}'.format(token)

    return source_request_headers


def load_daily_check_app_data_source_response_to_model(model, request_configs):
    """
    "request_config": {
        "url": "https://uni-connect-services-dev.azurewebsites.net/api/v1/measurements",
        "method": "GET",
        "data_limit": 1000,
        "auth_token_required": true,
        "headers": {
            "Content-Type": "application/json"
        }
    },
    """
    source_request_headers = get_request_headers(request_configs)

    page_no = 0
    page_size = request_configs.get('data_limit', 1000)

    has_more_data = True

    insert_entries = []
    new_params = {}

    while has_more_data:
        logger.debug('#' * 10)
        source_url = request_configs.get('url')

        if request_configs.get('query_params'):
            for param, value in request_configs.get('query_params').items():
                new_params[param] = value.format(page_no=page_no, page_size=page_size)

            page_no += 1
            source_url = add_url_params(request_configs.get('url'), new_params)

        logger.debug('Executing the request URL: {0}'.format(source_url))
        logger.debug('Request header: {0}'.format(source_request_headers))

        response = requests.get(source_url, headers=source_request_headers)

        if response.status_code != status.HTTP_200_OK:
            logger.error('Invalid response received {0}'.format(response))
            return

        response_data = response.json()

        if len(response_data) == 0:
            logger.debug('No records to read further.')
            has_more_data = False
        else:
            for data in response_data:
                if not data.get('created_at', None):
                    data['created_at'] = data.get('timestamp')
                insert_entries.append(model(**data))

        if len(insert_entries) >= 5000:
            logger.info('Loading the data to "{0}" table as it has reached 5000 benchmark.'.format(model.__name__))
            model.objects.bulk_create(insert_entries)
            insert_entries = []
            logger.debug('#' * 10)
            logger.debug('\n\n')

    logger.info('Loading the remaining ({0}) data to "{1}" table.'.format(len(insert_entries), model.__name__))
    if len(insert_entries) > 0:
        model.objects.bulk_create(insert_entries)


def sync_dailycheckapp_realtime_data():
    current_datetime = core_utilities.get_current_datetime_object()

    last_measurement_date = sources_models.DailyCheckAppMeasurementData.get_last_dailycheckapp_measurement_date()
    logger.info('Daily Check APP last measurement date: {0}'.format(last_measurement_date))

    request_configs = {
        'url': '{0}/measurements/v2'.format(ds_settings.get('DAILY_CHECK_APP').get('BASE_URL')),
        'method': 'GET',
        'data_limit': 1000,
        'query_params': {
            'page': '{page_no}',
            'size': '{page_size}',
            'orderBy': 'created_at',
            'filterBy': 'created_at',
            'filterCondition': 'gt',
            'filterValue': '{0}'.format(last_measurement_date),
        },
        'auth_token_required': True,
        'headers': {
            'Content-Type': 'application/json'
        }
    }
    load_daily_check_app_data_source_response_to_model(sources_models.DailyCheckAppMeasurementData, request_configs)

    dailycheckapp_measurements = sources_models.DailyCheckAppMeasurementData.objects.filter(
        created_at__gt=last_measurement_date,
        created_at__lte=current_datetime,
    ).filter(
        (Q(download__isnull=True) | Q(download__gte=0)) &
        (Q(upload__isnull=True) | Q(upload__gte=0)) &
        (Q(latency__isnull=True) | Q(latency__gte=0)),
    )
    logger.debug('Migrating the records from "DailyCheckAppMeasurementData" to "RealTimeConnectivity" '
                 'with date range: {0} - {1}'.format(last_measurement_date, current_datetime))

    realtime = []

    countries = set(dailycheckapp_measurements.values_list(
        'country_code', flat=True,
    ).order_by('country_code'))

    for country_code in countries:
        logger.debug('Current Country Code: {}'.format(country_code))
        if country_code:
            country = Country.objects.filter(code=country_code).first()
        else:
            country = None

        dcm_giga_ids = set(dailycheckapp_measurements.filter(
            country_code=country_code,
            source__iexact='DailyCheckApp',
        ).values_list('giga_id_school', flat=True).order_by('giga_id_school'))

        dcm_schools = {
            school.giga_id_school: school
            for school in School.objects.filter(giga_id_school__in=dcm_giga_ids)
        }
        logger.debug('Total schools in DailyCheckApp: {0}, Successfully mapped schools: {1}'.format(
            len(dcm_giga_ids), len(dcm_schools)))

        mlab_school_ids = set(dailycheckapp_measurements.filter(
            country_code=country_code,
            source__iexact='MLab',
        ).values_list('school_id', flat=True).order_by('school_id'))

        schools_qs = School.objects
        if country:
            schools_qs = schools_qs.filter(country=country)

        mlab_schools = {
            school.external_id: school
            for school in schools_qs.filter(external_id__in=mlab_school_ids)
        }
        logger.debug('Total schools in MLab: {0}, Successfully mapped schools: {1}'.format(
            len(mlab_school_ids), len(mlab_schools)))

        for dailycheckapp_measurement in dailycheckapp_measurements.filter(country_code=country_code):
            if str(dailycheckapp_measurement.source).lower() == 'dailycheckapp':
                giga_id_school = dailycheckapp_measurement.giga_id_school
                if giga_id_school not in dcm_schools:
                    logger.debug(f'skipping DCM unknown school Country Code: {country_code}, '
                                 f'Giga ID: {giga_id_school}')
                    continue
                school = dcm_schools[giga_id_school]
            else:
                school_id = dailycheckapp_measurement.school_id
                if school_id not in mlab_schools:
                    logger.debug(f'skipping MLab unknown school Country Code: {country_code}, '
                                 f'Govt School ID: {school_id}')
                    continue
                school = mlab_schools[school_id]

            connectivity_speed = dailycheckapp_measurement.download
            if connectivity_speed:
                # kb/s -> b/s
                connectivity_speed = connectivity_speed * 1000

            connectivity_upload_speed = dailycheckapp_measurement.upload
            if connectivity_upload_speed:
                # kb/s -> b/s
                connectivity_upload_speed = connectivity_upload_speed * 1000

            realtime.append(RealTimeConnectivity(
                created=dailycheckapp_measurement.timestamp,
                connectivity_speed=connectivity_speed,
                connectivity_upload_speed=connectivity_upload_speed,
                connectivity_latency=dailycheckapp_measurement.latency,
                school=school,
                live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
            ))

            if len(realtime) == 5000:
                logger.info('Loading the data to "RealTimeConnectivity" table as it has reached 5000 benchmark.')
                RealTimeConnectivity.objects.bulk_create(realtime)
                realtime = []

    logger.info('Loading the remaining ({0}) data to "RealTimeConnectivity" table.'.format(len(realtime)))
    if len(realtime) > 0:
        RealTimeConnectivity.objects.bulk_create(realtime)

    # not using aggregate because there can be new entries between two operations
    if dailycheckapp_measurements:
        last_update = max((m.created_at for m in dailycheckapp_measurements))
    else:
        last_update = current_datetime
    sources_models.DailyCheckAppMeasurementData.set_last_dailycheckapp_measurement_date(last_update)


def load_qos_data_source_response_to_model():
    qos_ds_settings = ds_settings.get('QOS')

    share_name = qos_ds_settings['SHARE_NAME']
    schema_name = qos_ds_settings['SCHEMA_NAME']
    country_codes_for_exclusion = qos_ds_settings['COUNTRY_EXCLUSION_LIST']

    profile_json = {
        'shareCredentialsVersion': qos_ds_settings.get('SHARE_CREDENTIALS_VERSION', 1),
        'endpoint': qos_ds_settings.get('ENDPOINT'),
        'bearerToken': qos_ds_settings.get('BEARER_TOKEN'),
        'expirationTime': qos_ds_settings.get('EXPIRATION_TIME')
    }
    profile_file = os.path.join(
        settings.BASE_DIR,
        'qos_profile_{dt}.share'.format(
            dt=format_date(core_utilities.get_current_datetime_object())
        )
    )
    open(profile_file, 'w').write(json.dumps(profile_json))

    # Create a SharingClient.
    client = ProcoSharingClient(profile_file)
    qos_share = client.get_share(share_name)

    changes_for_countries = {}

    if qos_share:
        qos_schema = client.get_schema(qos_share, schema_name)

        if qos_schema:
            schema_tables = client.list_tables(qos_schema)
            logger.debug('All tables ready to access: {0}'.format(schema_tables))

            qos_model_fields = [f.name for f in sources_models.QoSData._meta.get_fields()]

            for schema_table in schema_tables:
                logger.debug('#' * 10)
                logger.debug('Table: %s', schema_table)

                table_name = schema_table.name

                try:
                    country = Country.objects.filter(iso3_format=table_name).first()
                    logger.debug('Country object: {0}'.format(country))

                    if not country:
                        logger.error('Country with ISO3 Format ({0}) not found in DB. '
                                     'Hence skipping the load for current table.'.format(table_name))
                        continue

                    if len(country_codes_for_exclusion) > 0 and table_name in country_codes_for_exclusion:
                        logger.warning('Country with ISO3 Format ({0}) asked to exclude in PROCO DB. '
                                       'Hence skipping the load for current table.'.format(table_name))
                        continue

                    table_last_data_version = sources_models.QoSData.get_last_version(table_name)
                    logger.debug('Table last data version present in DB: {0}'.format(table_last_data_version))

                    # Create an url to access a shared table.
                    # A table path is the profile file path following with `#` and the fully qualified name of a table
                    # (`<share-name>.<schema-name>.<table-name>`).
                    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
                        share_name=share_name,
                        schema_name=schema_name,
                        table_name=table_name,
                    )
                    logger.debug('Table URL: %s', table_url)

                    table_current_version = delta_sharing.get_table_version(table_url)
                    logger.debug('Table current version from API: {0}'.format(table_current_version))

                    if table_last_data_version == table_current_version:
                        logger.info('Both QoS data version in DB and Table version from API, are same. '
                                    'Hence skipping the data update for current country ({0}).'.format(country))
                        continue

                    table_protocol = delta_sharing.get_table_protocol(table_url)
                    logger.debug('Table Protocol: {0}'.format(table_protocol))

                    table_meta_data = delta_sharing.get_table_metadata(table_url)
                    logger.debug('Table Metadata: {0}'.format(table_meta_data.__dict__))

                    if not table_last_data_version:
                        # In case if its 1st pull, then pull only last 10 version's data at max
                        # This is the case when we have restored the DB dump and running the task first time
                        table_last_data_version = int(max(-1, table_current_version - 10))

                    version_list = list(range(table_last_data_version + 1, table_current_version + 1))
                    for version in version_list:
                        loaded_data_df = delta_sharing.load_table_changes_as_pandas(
                            table_url,
                            version,
                            version,
                            None,
                            None,
                        )
                        logger.debug(
                            'Total count of rows in the {0} version data: {1}'.format(version, len(loaded_data_df)))
                        loaded_data_df = loaded_data_df[loaded_data_df[DeltaSharingReader._change_type_col_name()].isin(
                            ['insert', 'update_postimage'])]

                        logger.debug(
                            'Total count of rows after filtering only ["insert", "update_postimage"] in the "{0}" '
                            'version data: {1}'.format(version, len(loaded_data_df)))

                        if len(loaded_data_df) > 0:
                            insert_entries = []

                            changes_for_countries[table_name] = True

                            df_columns = list(loaded_data_df.columns.tolist())
                            cols_to_delete = list(set(df_columns) - set(qos_model_fields)) + ['id', 'created',
                                                                                              'modified', 'school_id',
                                                                                              'country_id',
                                                                                              'modified_by', ]
                            logger.debug('All QoS API response columns: {}'.format(df_columns))
                            logger.debug('All QoS API response columns to delete: {}'.format(
                                list(set(df_columns) - set(qos_model_fields))))

                            loaded_data_df.drop(columns=cols_to_delete, inplace=True, errors='ignore', )

                            loaded_data_df = normalize_qos_data_frame(loaded_data_df)

                            loaded_data_df['version'] = version
                            loaded_data_df['country'] = country

                            for _, row in loaded_data_df.iterrows():
                                school = School.objects.filter(
                                    country=country, giga_id_school=row['school_id_giga'],
                                ).first()

                                if not school:
                                    logger.warning(
                                        'School with Giga ID ({0}) not found in PROCO DB. '
                                        'Hence skipping the load for current school.'.format(row['school_id_giga']))
                                    continue

                                row['school'] = school

                                row_as_dict = parse_row(row)
                                insert_entries.append(row_as_dict)

                                if len(insert_entries) == 5000:
                                    logger.info('Loading the data to "QoSData" table as it has reached 5000 benchmark.')
                                    core_utilities.bulk_create_or_update(insert_entries, sources_models.QoSData,
                                                                         ['school', 'timestamp'])
                                    insert_entries = []
                                    logger.debug('#' * 10)
                                    logger.debug('\n\n')

                            logger.info(
                                'Loading the remaining ({0}) data to "QoSData" table.'.format(len(insert_entries)))
                            if len(insert_entries) > 0:
                                core_utilities.bulk_create_or_update(insert_entries, sources_models.QoSData,
                                                                     ['school', 'timestamp'])
                    else:
                        logger.info('No data to update in current table: {0}.'.format(table_name))
                except Exception as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
        else:
            logger.error('QoS schema ({0}) does not exist to use for share ({1}).'.format(schema_name, share_name))
    else:
        logger.error('QoS share ({0}) does not exist to use.'.format(share_name))

    try:
        os.remove(profile_file)
    except OSError:
        pass


def sync_qos_realtime_data():
    current_datetime = core_utilities.get_current_datetime_object()

    last_entry_date = RealTimeConnectivity.objects.filter(
        live_data_source=statistics_configs.QOS_SOURCE,
    ).order_by('-created').values_list('created', flat=True).first()

    if not last_entry_date:
        last_entry_date = current_datetime - timedelta(days=1)

    qos_measurements = sources_models.QoSData.objects.filter(
        timestamp__gt=last_entry_date,
        timestamp__lte=current_datetime,
    ).values(
        'timestamp', 'speed_download', 'speed_upload', 'latency', 'school',
        'roundtrip_time', 'jitter_download', 'jitter_upload', 'rtt_packet_loss_pct',
        'speed_download_probe', 'speed_upload_probe', 'latency_probe',
    ).order_by('timestamp').distinct(*['timestamp', 'school'])

    logger.debug('Migrating the records from "QoSData" to "RealTimeConnectivity" with date range: {0} - {1}'.format(
        last_entry_date, current_datetime))

    realtime = []

    for qos_measurement in qos_measurements:
        connectivity_speed = qos_measurement.get('speed_download')
        if connectivity_speed:
            # convert Mbps to bps
            connectivity_speed = connectivity_speed * 1000 * 1000

        connectivity_speed_probe = qos_measurement.get('speed_download_probe')
        if connectivity_speed_probe:
            # convert Mbps to bps
            connectivity_speed_probe = connectivity_speed_probe * 1000 * 1000

        connectivity_upload_speed = qos_measurement.get('speed_upload')
        if connectivity_upload_speed:
            # convert Mbps to bps
            connectivity_upload_speed = connectivity_upload_speed * 1000 * 1000

        connectivity_upload_speed_probe = qos_measurement.get('speed_upload_probe')
        if connectivity_upload_speed_probe:
            # convert Mbps to bps
            connectivity_upload_speed_probe = connectivity_upload_speed_probe * 1000 * 1000

        realtime.append(RealTimeConnectivity(
            created=qos_measurement.get('timestamp'),
            connectivity_speed=connectivity_speed,
            connectivity_upload_speed=connectivity_upload_speed,
            connectivity_latency=qos_measurement.get('latency'),
            connectivity_speed_probe=connectivity_speed_probe,
            connectivity_upload_speed_probe=connectivity_upload_speed_probe,
            connectivity_latency_probe=qos_measurement.get('latency_probe'),
            roundtrip_time=qos_measurement.get('roundtrip_time'),
            jitter_download=qos_measurement.get('jitter_download'),
            jitter_upload=qos_measurement.get('jitter_upload'),
            rtt_packet_loss_pct=qos_measurement.get('rtt_packet_loss_pct'),
            school_id=qos_measurement.get('school'),
            live_data_source=statistics_configs.QOS_SOURCE,
        ))

        if len(realtime) == 5000:
            logger.info('Loading the data to "RealTimeConnectivity" table as it has reached 5000 benchmark.')
            RealTimeConnectivity.objects.bulk_create(realtime)
            realtime = []

    logger.info('Loading the remaining ({0}) data to "RealTimeConnectivity" table.'.format(len(realtime)))
    if len(realtime) > 0:
        RealTimeConnectivity.objects.bulk_create(realtime)
