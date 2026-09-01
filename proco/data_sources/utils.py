import json
import logging
import os

from datetime import datetime as dt_class
from django.utils import timezone
from datetime import timedelta
from typing import Optional, Any

import delta_sharing
import numpy as np
import pandas as pd
import pytz
import requests
from requests.exceptions import HTTPError
from delta_sharing.protocol import Schema, Share
from delta_sharing.reader import DeltaSharingReader
from django.conf import settings
from django.core.cache import cache
from django.db.models import Avg, Q
from django.db.models.functions import Lower
from rest_framework import status

from proco.accounts.models import APIKey
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import RealTimeConnectivity, SchoolRealTimeRegistration, EntityRealTimeConnectivity, EntityDailyStatus, EntityRealTimeRegistration
from proco.core import utils as core_utilities
from proco.core.config import app_config as core_configs
from proco.custom_auth.models import ApplicationUser
from proco.data_sources import models as sources_models
from proco.entities.models import Entity, EntityType
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils.dates import format_date
from proco.utils.urls import add_url_params

logger = logging.getLogger('gigamaps.' + __name__)

response_timezone = pytz.timezone(settings.TIME_ZONE)

ds_settings = settings.DATA_SOURCE_CONFIG


class ProcoSharingClient(delta_sharing.SharingClient):

    def get_share(self, share_name: str) -> Optional[Share]:
        """
        Get share that can be accessed by you in a Delta Sharing Server.

        :return: the share that can be accessed.
        """
        shares = self.list_shares()
        for share in shares:
            if share.name == share_name:
                return share
        return None

    def get_schema(self, share: Share, schema_name: str) -> Optional[Schema]:
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
        return None


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

    if 'verification_status' in list(df.columns.tolist()):
        df['is_verified_school'] = df['verification_status'].fillna('').apply(
            lambda val: str(val).strip().lower() != 'unverified')
    else:
        df['is_verified_school'] = True
    return df


def normalize_qos_data_frame(df):
    if 'school_id_govt' in list(df.columns.tolist()):
        df['school_id_govt'] = df['school_id_govt'].fillna('thisnanwillreplaceback').apply(
            lambda val: str(val).lower()).replace('thisnanwillreplaceback', np.nan)
    else:
        df['school_id_govt'] = None
    return df


def _values_equal(a: Any, b: Any) -> bool:
    """
    Null-safe equality check.
    Treats NaN, NaT, and None as equal if both sides are missing.
    """
    if (pd.isna(a) and pd.isna(b)) or (a is None and b is None):
        return True
    return a == b


def has_changes_for_review(row, school) -> bool:
    """
    Compare a DataFrame row with an existing School model instance.
    Returns True if any meaningful change is detected; False otherwise.

    This preserves the original control flow and column accesses, but uses
    null-safe comparisons so NaN/NaT/None do not cause false positives.
    """
    if school:
        if not _values_equal(row['school_name'].lower(), school.name.lower()):
            return True

        old_external_id = None \
            if core_utilities.is_blank_string(school.external_id) else str(school.external_id).lower()
        new_external_id = None \
            if core_utilities.is_blank_string(row['school_id_govt']) else str(row['school_id_govt']).lower()
        if not _values_equal(old_external_id, new_external_id):
            return True

        old_admin1_id = None
        if school.admin1:
            old_admin1_id = str(school.admin1.giga_id_admin).lower()
        new_admin1_id = None \
            if core_utilities.is_blank_string(row['admin1_id_giga']) else str(row['admin1_id_giga']).lower()
        if not _values_equal(old_admin1_id, new_admin1_id):
            return True

        old_admin2_id = None
        if school.admin2:
            old_admin2_id = str(school.admin2.giga_id_admin).lower()
        new_admin2_id = None \
            if core_utilities.is_blank_string(row['admin2_id_giga']) else str(row['admin2_id_giga']).lower()
        if not _values_equal(old_admin2_id, new_admin2_id):
            return True

        old_lat = school.geopoint.y
        new_lat = row['latitude']
        if (
            not _values_equal(old_lat, new_lat) and
            (
                str(old_lat).split('.')[0] != str(new_lat).split('.')[0] or
                str(old_lat).split('.')[1][:5] != str(new_lat).split('.')[1][:5]
            )
        ):
            return True

        old_long = school.geopoint.x
        new_long = row['longitude']
        if (
            not _values_equal(old_long, new_long) and
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
        if not _values_equal(old_education_level, new_education_level):
            return True

        school_rt_instance = SchoolRealTimeRegistration.objects.filter(school=school).order_by('-created').first()
        old_connectivity_rt = school_rt_instance.rt_registered if school_rt_instance else None

        new_connectivity_rt = None
        if (
            not pd.isnull(row['connectivity_RT']) and
            not pd.isnull(row['connectivity_RT_ingestion_timestamp'])
        ):
            new_connectivity_rt = str(row['connectivity_RT']).lower() in core_configs.true_choices

        if not _values_equal(old_connectivity_rt, new_connectivity_rt):
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


def parse_row_safe(row):
    row = row.copy()
    row = row.replace({np.nan: None, pd.NaT: None})

    for col in [
        'timestamp',
        'school_location_ingestion_timestamp',
        'connectivity_RT_ingestion_timestamp',
        'connectivity_govt_ingestion_timestamp',
        '_commit_timestamp',  # ← this is the culprit
    ]:
        value = row.get(col)

        if value is None:
            continue

        # Epoch milliseconds → pandas Timestamp
        if isinstance(value, (int, float)):
            row[col] = pd.to_datetime(value, unit="ms", utc=True)
            continue

        # Naive pandas timestamp → localize
        if hasattr(value, "tz_localize") and value.tzinfo is None:
            row[col] = value.tz_localize(response_timezone)

    return row.to_dict()


def sync_school_master_data(profile_file, share_name, schema_name, table_name, changes_for_countries, deleted_schools,
                            school_master_fields):
    country = Country.objects.filter(iso3_format=table_name, ).first()
    logger.debug('Country object: {0}'.format(country))

    if not country:
        logger.warning('Country with ISO3 Format ({0}) not found in DB. '
                       'Hence skipping the load for current table.'.format(table_name))
        return

    table_last_data_version = sources_models.SchoolMasterData.get_last_version(table_name)
    logger.debug('Table last data version present in DB: {0}'.format(table_last_data_version))

    # Create a url to access a shared table.
    # A table path is the profile file path following with `#` and the fully qualified name of a table
    # (`<share-name>.<schema-name>.<table-name>`).
    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
        share_name=share_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    logger.debug('Table URL: %s', table_url)

    try:
        table_current_version = delta_sharing.get_table_version(table_url)
    except HTTPError as ex:
        if ex.response is not None and ex.response.status_code == 404:
            logger.warning('Table version not found (404) for country ({0}). Skipping.'.format(table_name))
            return
        logger.warning('HTTP error getting table version for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return
    except Exception as ex:
        logger.warning('Failed to get table version for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return

    logger.debug('Table current version from API: {0}'.format(table_current_version))

    if table_last_data_version == table_current_version:
        logger.info('Both School Master data version in DB and Table version from API, are same. '
                    'Hence skipping the data update for current country ({0}).'.format(country))
        return

    if table_last_data_version is not None and table_current_version is not None and table_last_data_version > table_current_version:
        logger.warning(
            'School Master start version ({0}) in DB is greater than remote table version ({1}) for country ({2}). '
            'Pulling full data from version 0.'.format(table_last_data_version, table_current_version, table_name)
        )
        table_last_data_version = 0

    try:
        loaded_data_df = delta_sharing.load_table_changes_as_pandas(
            table_url,
            table_last_data_version,
            table_current_version,
            None,
            None,
        )
    except HTTPError as ex:
        if ex.response is not None and ex.response.status_code in (400, 404):
            logger.warning('Failed to load table changes for country ({0}) [HTTP {1}]: {2}. Skipping.'.format(
                table_name, ex.response.status_code, ex))
            return
        logger.warning('HTTP error loading table changes for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return
    except Exception as ex:
        logger.warning('Failed to load table changes for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return

    logger.debug('Total count of rows in the data: {0}'.format(len(loaded_data_df)))
    pull_datetime = core_utilities.get_current_datetime_object()

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
        loaded_data_df['pulled_at'] = pull_datetime

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


def get_request_headers(request_configs, api_code=None):
    source_request_headers = request_configs.get('headers', {})
    auth_required = request_configs.get('auth_token_required', False)

    if auth_required:
        internal_users = list(ApplicationUser.objects.filter(
            Q(is_active=True) & (Q(is_superuser=True) | Q(is_staff=True))
        ).values_list('id', flat=True).order_by('id').distinct('id'))

        # Resolve the API code: use the passed-in api_code, or fall back to the request config,
        # or default to DAILY_CHECK_APP.
        resolved_api_code = api_code
        if not resolved_api_code:
            resolved_api_code = ds_settings.get('DAILY_CHECK_APP', {}).get('API_CODE', 'DAILY_CHECK_APP')

        # API Key for is_staff = True user which never expires
        api_keys = list(APIKey.objects.annotate(api_code_lower=Lower('api__code')).filter(
            api_code_lower=resolved_api_code.lower(),
            status=APIKey.APPROVED,
            valid_to__gte=core_utilities.get_current_datetime_object().date(),
            user__in=internal_users,
            has_write_access=True,
        ).values_list('api_key', flat=True))

        if len(api_keys) > 0:
            token = api_keys[-1]
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
    model_field_names = {f.name for f in model._meta.fields} | {f.attname for f in model._meta.fields}

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
        pull_datetime = core_utilities.get_current_datetime_object()

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
                data['pulled_at'] = pull_datetime
                filtered_data = {k: v for k, v in data.items() if k in model_field_names}
                insert_entries.append(model(**filtered_data))

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


def load_qos_data_source_response_to_model(changes_for_countries):
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
                        logger.warning('Country with ISO3 Format ({0}) not found in DB. '
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

                    try:
                        table_current_version = delta_sharing.get_table_version(table_url)
                    except HTTPError as ex:
                        if ex.response is not None and ex.response.status_code == 404:
                            logger.warning('QoS table version not found (404) for country ({0}). Skipping.'.format(table_name))
                            continue
                        logger.warning('HTTP error getting QoS table version for country ({0}): {1}. Skipping.'.format(table_name, ex))
                        continue
                    except Exception as ex:
                        logger.warning('Failed to get QoS table version for country ({0}): {1}. Skipping.'.format(table_name, ex))
                        continue

                    logger.debug('Table current version from API: {0}'.format(table_current_version))

                    if table_last_data_version == table_current_version:
                        logger.info('Both QoS data version in DB and Table version from API, are same. '
                                    'Hence skipping the data update for current country ({0}).'.format(country))
                        continue

                    if not table_last_data_version or (table_current_version is not None and table_last_data_version > table_current_version):
                        # In case if its 1st pull, then pull only last 10 version's data at max
                        # This is the case when we have restored the DB dump and running the task first time
                        table_last_data_version = int(max(-1, table_current_version - 10))

                    version_list = list(range(table_last_data_version + 1, table_current_version + 1))
                    for version in version_list:
                        try:
                            loaded_data_df = delta_sharing.load_table_changes_as_pandas(
                                table_url,
                                version,
                                version,
                                None,
                                None,
                            )
                        except HTTPError as ex:
                            if ex.response is not None and ex.response.status_code in (400, 404):
                                logger.warning('Failed to load QoS table changes for country ({0}) version {1} [HTTP {2}]: {3}. Skipping.'.format(
                                    table_name, version, ex.response.status_code, ex))
                                continue
                            logger.warning('HTTP error loading QoS table changes for country ({0}) version {1}: {2}. Skipping.'.format(table_name, version, ex))
                            continue
                        except Exception as ex:
                            logger.warning('Failed to load QoS table changes for country ({0}) version {1}: {2}. Skipping.'.format(table_name, version, ex))
                            continue

                        logger.debug(
                            'Total count of rows in the {0} version data: {1}'.format(version, len(loaded_data_df)))
                        pull_datetime = core_utilities.get_current_datetime_object()
                        loaded_data_df = loaded_data_df[
                            loaded_data_df[DeltaSharingReader._change_type_col_name()].isin(
                                ['insert', 'update_postimage']
                            )
                        ]

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
                            loaded_data_df['pulled_at'] = pull_datetime

                            for _, row in loaded_data_df.iterrows():
                                school = School.objects.filter(
                                    country=country, giga_id_school=row['school_id_giga'],
                                ).first()

                                if not school:
                                    logger.warning(
                                        'School with Giga ID ({0}) not found in GigaMaps DB. '
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
                    logger.warning('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
        else:
            logger.warning('QoS schema ({0}) does not exist to use for share ({1}).'.format(schema_name, share_name))
    else:
        logger.warning('QoS share ({0}) does not exist to use.'.format(share_name))

    try:
        os.remove(profile_file)
    except OSError:
        pass


def sync_qos_realtime_data(country_id):
    current_datetime = core_utilities.get_current_datetime_object()

    last_entry_date = RealTimeConnectivity.objects.filter(
        live_data_source=statistics_configs.QOS_SOURCE,
        school__country_id=country_id,
        school__deleted__isnull=True,
    ).order_by('-created').values_list('created', flat=True).first()

    if not last_entry_date:
        last_entry_date = current_datetime - timedelta(days=1)

    qos_measurements = sources_models.QoSData.objects.filter(
        timestamp__gt=last_entry_date,
        timestamp__lte=current_datetime,
        country_id=country_id,
    ).values(
        'timestamp', 'speed_download', 'speed_upload', 'latency', 'school',
        'roundtrip_time', 'jitter_download', 'jitter_upload', 'rtt_packet_loss_pct',
        'speed_download_probe', 'speed_upload_probe', 'latency_probe',
        'speed_download_mean', 'speed_upload_mean',
    ).order_by('timestamp').distinct(*['timestamp', 'school'])

    logger.debug('Migrating the records from "QoSData" to "RealTimeConnectivity" with date range: {0} - {1}'.format(
        last_entry_date, current_datetime))

    realtime = []
    # convert Mbps to bps
    fields_for_mb_conversion = [
        'speed_download',
        'speed_upload',
        'speed_download_probe',
        'speed_upload_probe',
        'speed_download_mean',
        'speed_upload_mean',
    ]

    for qos_measurement in qos_measurements:

        for field_name in fields_for_mb_conversion:
            if qos_measurement.get(field_name):
                qos_measurement[field_name] = qos_measurement[field_name] * 1000 * 1000

        realtime.append(RealTimeConnectivity(
            created=qos_measurement.get('timestamp'),
            connectivity_speed=qos_measurement.get('speed_download'),
            connectivity_upload_speed=qos_measurement.get('speed_upload'),
            connectivity_latency=qos_measurement.get('latency'),
            connectivity_speed_probe=qos_measurement.get('speed_download_probe'),
            connectivity_upload_speed_probe=qos_measurement.get('speed_upload_probe'),
            connectivity_latency_probe=qos_measurement.get('latency_probe'),
            connectivity_speed_mean=qos_measurement.get('speed_download_mean'),
            connectivity_upload_speed_mean=qos_measurement.get('speed_upload_mean'),
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


# Need to check this
def normalize_health_entity_name(health_name):
    # If its blank string then put default value
    if pd.isna(health_name) or core_utilities.is_blank_string(health_name):
        return 'Name unknown'

    # Remove space from start and end if present
    # Replace the 2 times double quotes ("") with 1 time double quotes (")
    health_name = str(health_name).strip().replace('""', '"')

    # If health entity name start & ends with ", then remove these from start and end
    if (
        len(health_name) >= 2
        and health_name[0] == health_name[-1]
        and health_name.startswith(("'", '"'))
    ):
        health_name = health_name[1:-1]

    return health_name


def normalize_health_entity_master_data_frame(df):
    df['facility_name'] = df['facility_name'].apply(normalize_health_entity_name)
    if 'facility_id_govt' in list(df.columns.tolist()):
        df['facility_id_govt'] = df['facility_id_govt'].fillna('thisnanwillreplaceback').apply(
            lambda val: str(val).lower()).replace('thisnanwillreplaceback', np.nan)
    else:
        df['facility_id_govt'] = None
    return df


def sort_and_modify_dataframe(loaded_data_df, health_master_fields, changes_for_countries, table_current_version,
                              country, table_name, pull_datetime):
    loaded_data_df = loaded_data_df.sort_values(
        by=[DeltaSharingReader._commit_version_col_name(), DeltaSharingReader._commit_timestamp_col_name()],
        na_position='first')
    loaded_data_df.drop_duplicates(
        subset=['health_id_giga'],
        keep='last',
        inplace=True,
    )
    loaded_data_df = loaded_data_df[loaded_data_df[DeltaSharingReader._change_type_col_name()].isin(
        ['insert', 'update_postimage', 'remove', 'delete'])]

    logger.debug('Total count of rows in the data after duplicate cleanup: {0}'.format(len(loaded_data_df)))

    df_columns = list(loaded_data_df.columns.tolist())
    cols_to_delete = list(set(df_columns) - set(health_master_fields)) + ['id', 'created', 'modified',
                                                                          'country_id', 'status',
                                                                          'modified_by', 'published_by',
                                                                          'published_at', 'is_read', ]
    logger.debug('All Health Master API response columns: {}'.format(df_columns))
    logger.debug('All Health Master API response columns to delete: {}'.format(
        list(set(df_columns) - set(health_master_fields))))
    changes_for_countries[table_name] = True
    loaded_data_df = normalize_health_entity_master_data_frame(loaded_data_df)
    loaded_data_df['version'] = table_current_version
    loaded_data_df['country'] = country
    loaded_data_df['pulled_at'] = pull_datetime
    return loaded_data_df, cols_to_delete



def sync_health_data(loaded_data_df, cols_to_delete, country, deleted_entities):
    insert_entries = []
    remove_entries = []

    chunk_size = 5000
    for start in range(0, len(loaded_data_df), chunk_size):
        chunk_df = loaded_data_df.iloc[start:start + chunk_size]
        giga_ids = chunk_df['health_id_giga'].dropna().unique().tolist()

        entities = Entity.objects.filter(country=country, giga_id__in=giga_ids)
        entity_map = {e.giga_id: e for e in entities}

        for _, row in chunk_df.iterrows():
            change_type = row[DeltaSharingReader._change_type_col_name()]
            row.drop(
                labels=cols_to_delete,
                inplace=True,
                errors='ignore',
            )
            row['country_id'] = country.id
            entity = entity_map.get(row.get('health_id_giga'))

            if change_type in ['insert', 'update_postimage']:
                # Publish entities directly
                if entity:
                    row['entity_id'] = entity.id
                    row['status'] = sources_models.HealthEntityMasterIntermediateData.ROW_STATUS_PUBLISHED
                    row['published_at'] = core_utilities.get_current_datetime_object()
                row_as_dict = parse_row(row)
                insert_entries.append(sources_models.HealthEntityMasterIntermediateData(**row_as_dict))
                if len(insert_entries) >= 5000:
                    logger.debug('Loading the data to "HealthMasterData" table as it has reached 5000 benchmark.')
                    sources_models.HealthEntityMasterIntermediateData.objects.bulk_create(insert_entries)
                    insert_entries = []
                    logger.debug('#' * 10)
                    logger.debug('\n\n')
            elif change_type in ['remove', 'delete']:
                # Entity can be deleted only if its already present in Giga DB
                if entity:
                    row['entity_id'] = entity.id
                    row['status'] = sources_models.HealthEntityMasterIntermediateData.ROW_STATUS_DELETED_PUBLISHED
                    row['modified'] = core_utilities.get_current_datetime_object()

                    row_as_dict = parse_row_safe(row)
                    remove_entries.append(sources_models.HealthEntityMasterIntermediateData(**row_as_dict))
                if len(remove_entries) >= 5000:
                    logger.info(
                        'Loading the data to "HealthEntityMasterIntermediateData" table as it has reached 5000 benchmark.')
                    sources_models.HealthEntityMasterIntermediateData.objects.bulk_create(remove_entries)
                    remove_entries = []
                    logger.debug('#' * 10)
                    logger.debug('\n\n')

    logger.info(
        'Loading the remaining ({0}) data to "HealthEntityMasterIntermediateData" table.'.format(len(insert_entries)))
    if len(insert_entries) > 0:
        sources_models.HealthEntityMasterIntermediateData.objects.bulk_create(insert_entries)
    logger.info('Removing ({0}) records from "HealthEntityMasterIntermediateData" table.'.format(len(remove_entries)))
    if len(remove_entries) > 0:
        sources_models.HealthEntityMasterIntermediateData.objects.bulk_create(remove_entries)
        deleted_entities.extend(
            [country.name + ' : ' + health_master_row.facility_name for health_master_row in remove_entries])


def vaildate_master_version_and_sync_health_master_data(profile_file, share_name, schema_name, table_name,
                                                        changes_for_countries, deleted_entities,
                                                        health_master_fields):
    country = Country.objects.filter(iso3_format=table_name, ).first()
    logger.debug('Country object: {0}'.format(country))

    if not country:
        logger.warning('Country with ISO3 Format ({0}) not found in DB. '
                       'Hence skipping the load for current table.'.format(table_name))
        return

    table_last_data_version = sources_models.HealthEntityMasterIntermediateData.get_last_version(table_name)
    logger.debug('Table last data version present in DB: {0}'.format(table_last_data_version))

    # Create a url to access a shared table.
    # A table path is the profile file path following with `#` and the fully qualified name of a table
    # (`<share-name>.<schema-name>.<table-name>`).
    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
        share_name=share_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    logger.debug('Table URL: %s', table_url)

    try:
        table_current_version = delta_sharing.get_table_version(table_url)
    except HTTPError as ex:
        if ex.response is not None and ex.response.status_code == 404:
            logger.warning('Health Master table version not found (404) for country ({0}). Skipping.'.format(table_name))
            return
        logger.warning('HTTP error getting Health Master table version for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return
    except Exception as ex:
        logger.warning('Failed to get Health Master table version for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return

    logger.debug('Table current version from API: {0}'.format(table_current_version))

    if table_last_data_version == table_current_version:
        logger.info('Both Health Master data version in DB and Table version from API, are same. '
                    'Hence skipping the data update for current country ({0}).'.format(country))
        return

    if table_last_data_version is not None and table_current_version is not None and table_last_data_version > table_current_version:
        logger.warning(
            'Health Master start version ({0}) in DB is greater than remote table version ({1}) for country ({2}). '
            'Pulling full data from version 0.'.format(table_last_data_version, table_current_version, table_name)
        )
        table_last_data_version = 0

    try:
        loaded_data_df = delta_sharing.load_table_changes_as_pandas(
            table_url,
            table_last_data_version,
            table_current_version,
            None,
            None,
        )
    except HTTPError as ex:
        if ex.response is not None and ex.response.status_code in (400, 404):
            logger.warning('Failed to load Health Master table changes for country ({0}) [HTTP {1}]: {2}. Skipping.'.format(
                table_name, ex.response.status_code, ex))
            return
        logger.warning('HTTP error loading Health Master table changes for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return
    except Exception as ex:
        logger.warning('Failed to load Health Master table changes for country ({0}): {1}. Skipping.'.format(table_name, ex))
        return

    logger.debug('Total count of rows in the data: {0}'.format(len(loaded_data_df)))
    pull_datetime = core_utilities.get_current_datetime_object()

    if len(loaded_data_df) > 0:
        # Sort the values based on _commit_timestamp ASC
        loaded_data_df, cols_to_delete = sort_and_modify_dataframe(loaded_data_df, health_master_fields, changes_for_countries,
                                                                   table_current_version, country, table_name, pull_datetime)
        sync_health_data(loaded_data_df, cols_to_delete, country, deleted_entities)
    else:
        logger.info('No data to update in current table: {0}.'.format(table_name))


# ############################# Entity QoS Utilities #############################

def load_entity_qos_data_source_response_to_model(entity_type_code='health'):
    """
    Load entity QoS data from Delta Sharing source into the EntityQoSData staging model.

    Mirrors load_qos_data_source_response_to_model() but operates on entities.
    Looks up Entity by giga_id instead of School by giga_id_school.

    Args:
        entity_type_code: The entity type code to filter entities by (e.g. 'health').
    """
    entity_qos_settings = ds_settings.get('QOS')
    if not entity_qos_settings:
        logger.warning('QOS config not found in DATA_SOURCE_CONFIG. Skipping entity QoS data load.')
        return

    share_name = entity_qos_settings['SHARE_NAME']
    schema_name = entity_qos_settings['ENTITY_SCHEMA_NAME']
    country_codes_for_exclusion = entity_qos_settings.get('COUNTRY_EXCLUSION_LIST', [])

    profile_json = {
        'shareCredentialsVersion': entity_qos_settings.get('SHARE_CREDENTIALS_VERSION', 1),
        'endpoint': entity_qos_settings.get('ENDPOINT'),
        'bearerToken': entity_qos_settings.get('BEARER_TOKEN'),
        'expirationTime': entity_qos_settings.get('EXPIRATION_TIME'),
    }

    if not profile_json.get('endpoint') or not profile_json.get('bearerToken'):
        logger.warning('Entity QoS Delta Sharing endpoint or bearer token not configured. Skipping.')
        return

    profile_file = os.path.join(
        settings.BASE_DIR,
        'entity_qos_profile_{dt}.share'.format(
            dt=format_date(core_utilities.get_current_datetime_object())
        )
    )
    open(profile_file, 'w').write(json.dumps(profile_json))

    try:
        entity_type = EntityType.objects.filter(code=entity_type_code, deleted__isnull=True).first()
        if not entity_type:
            logger.warning('EntityType with code "%s" not found. Skipping entity QoS data load.', entity_type_code)
            return

        client = ProcoSharingClient(profile_file)
        qos_share = client.get_share(share_name)

        if not qos_share:
            logger.warning('Entity QoS share (%s) does not exist.', share_name)
            return

        qos_schema = client.get_schema(qos_share, schema_name)
        if not qos_schema:
            logger.warning('Entity QoS schema (%s) does not exist for share (%s).', schema_name, share_name)
            return

        schema_tables = client.list_tables(qos_schema)
        logger.debug('Entity QoS - All tables ready to access: %s', schema_tables)

        for schema_table in schema_tables:
            table_name = schema_table.name

            try:
                country = Country.objects.filter(iso3_format=table_name).first()
                if not country:
                    logger.warning(
                        'Country with ISO3 Format (%s) not found in DB. Skipping entity QoS load.', table_name
                    )
                    continue

                if not Entity.objects.filter(country=country, entity_type=entity_type, deleted__isnull=True).exists():
                    logger.info("Skipping %s - no %s entities exist locally for this country", table_name, entity_type_code)
                    continue

                if country_codes_for_exclusion and table_name in country_codes_for_exclusion:
                    logger.warning(
                        'Country (%s) excluded from entity QoS data pull. Skipping.', table_name
                    )
                    continue

                # Use a cache key specific to entity QoS
                cache_key = 'entity_qos_data_last_version_{}'.format(table_name)
                table_last_data_version = cache.get(cache_key)

                if not table_last_data_version:
                    # Try to find from existing EntityRealTimeConnectivity records
                    table_last_data_version = None

                table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
                    share_name=share_name,
                    schema_name=schema_name,
                    table_name=table_name,
                )

                try:
                    table_current_version = delta_sharing.get_table_version(table_url)
                except HTTPError as ex:
                    if ex.response is not None and ex.response.status_code == 404:
                        logger.warning('Entity QoS table version not found (404) for country (%s). Skipping.', table_name)
                        continue
                    logger.warning('HTTP error getting Entity QoS table version for country (%s): %s. Skipping.', table_name, ex)
                    continue
                except Exception as ex:
                    logger.warning('Failed to get Entity QoS table version for country (%s): %s. Skipping.', table_name, ex)
                    continue

                logger.debug('Entity QoS - Table version from API: %s', table_current_version)

                if table_last_data_version == table_current_version:
                    logger.info(
                        'Entity QoS data version unchanged for country (%s). Skipping.', country
                    )
                    continue

                if not table_last_data_version or (table_current_version is not None and table_last_data_version > table_current_version):
                    table_last_data_version = int(max(-1, table_current_version - 10))

                version_list = list(range(table_last_data_version + 1, table_current_version + 1))

                for version in version_list:
                    try:
                        loaded_data_df = delta_sharing.load_table_changes_as_pandas(
                            table_url, version, version, None, None,
                        )
                    except HTTPError as ex:
                        if ex.response is not None and ex.response.status_code in (400, 404):
                            logger.warning('Failed to load Entity QoS table changes for country (%s) version %s [HTTP %s]: %s. Skipping.',
                                table_name, version, ex.response.status_code, ex)
                            continue
                        logger.warning('HTTP error loading Entity QoS table changes for country (%s) version %s: %s. Skipping.', table_name, version, ex)
                        continue
                    except Exception as ex:
                        logger.warning('Failed to load Entity QoS table changes for country (%s) version %s: %s. Skipping.', table_name, version, ex)
                        continue

                    logger.debug(
                        'Entity QoS - %s version data row count: %d', version, len(loaded_data_df)
                    )

                    pull_datetime = core_utilities.get_current_datetime_object()
                    loaded_data_df = loaded_data_df[
                        loaded_data_df[DeltaSharingReader._change_type_col_name()].isin(
                            ['insert', 'update_postimage']
                        )
                    ]

                    if len(loaded_data_df) == 0:
                        logger.info('Entity QoS - No data to update for table: %s.', table_name)
                        continue

                    # Pre-fetch local entities for this country into memory
                    local_entities = {
                        str(e.giga_id): e for e in Entity.objects.filter(
                            country=country,
                            entity_type__code=entity_type_code,
                            deleted__isnull=True,
                        )
                    }

                    insert_entries = []

                    for _, row in loaded_data_df.iterrows():
                        giga_id = (
                            row.get('giga_id')
                            or row.get('entity_id_giga')
                            or row.get('giga_id_health')
                            or row.get('health_id_giga')
                            or row.get('school_id_giga')
                            or row.get('giga_id_school')
                            or row.get('school_id')
                        )
                        if not giga_id:
                            logger.warning('Entity QoS - Row missing giga_id. Skipping.')
                            continue

                        entity = local_entities.get(str(giga_id))

                        if not entity:
                            continue

                        row_dict = row.replace({np.nan: None, pd.NaT: None}).to_dict()

                        speed_download = row_dict.get('speed_download')
                        speed_upload = row_dict.get('speed_upload')
                        latency = row_dict.get('latency')

                        speed_download_probe = row_dict.get('speed_download_probe')
                        speed_upload_probe = row_dict.get('speed_upload_probe')
                        latency_probe = row_dict.get('latency_probe')

                        speed_download_mean = row_dict.get('speed_download_mean')
                        speed_upload_mean = row_dict.get('speed_upload_mean')

                        timestamp = row_dict.get('timestamp', pull_datetime)

                        # Convert Mbps to bps if present
                        if speed_download is not None:
                            speed_download = speed_download * 1000 * 1000
                        if speed_upload is not None:
                            speed_upload = speed_upload * 1000 * 1000
                        if speed_download_probe is not None:
                            speed_download_probe = speed_download_probe * 1000 * 1000
                        if speed_upload_probe is not None:
                            speed_upload_probe = speed_upload_probe * 1000 * 1000
                        if speed_download_mean is not None:
                            speed_download_mean = speed_download_mean * 1000 * 1000
                        if speed_upload_mean is not None:
                            speed_upload_mean = speed_upload_mean * 1000 * 1000

                        if speed_download is None and speed_upload is None and latency is None and \
                           speed_download_probe is None and speed_upload_probe is None and latency_probe is None and \
                           speed_download_mean is None and speed_upload_mean is None:
                            # Skip rows with no actual QoS data
                            continue

                        insert_entries.append({
                            'entity': entity,
                            'timestamp': timestamp,
                            'speed_download': speed_download,
                            'speed_upload': speed_upload,
                            'latency': latency,
                            'speed_download_probe': speed_download_probe,
                            'speed_upload_probe': speed_upload_probe,
                            'latency_probe': latency_probe,
                            'speed_download_mean': speed_download_mean,
                            'speed_upload_mean': speed_upload_mean,
                            'roundtrip_time': row_dict.get('roundtrip_time'),
                            'jitter_download': row_dict.get('jitter_download'),
                            'jitter_upload': row_dict.get('jitter_upload'),
                            'rtt_packet_loss_pct': row_dict.get('rtt_packet_loss_pct'),
                            'version': version,
                            'country': country,
                            'pulled_at': pull_datetime,
                        })

                        if len(insert_entries) == 5000:
                            logger.info(
                                'Entity QoS - Loading batch of 5000 to EntityRealTimeConnectivity.'
                            )
                            bulk_create_entity_realtime_connectivity(insert_entries)
                            insert_entries = []

                    if insert_entries:
                        logger.info(
                            'Entity QoS - Loading remaining %d records.', len(insert_entries)
                        )
                        bulk_create_entity_realtime_connectivity(insert_entries)

                # Cache the latest version
                cache.set(cache_key, table_current_version)

            except Exception as ex:
                logger.warning('Entity QoS - Exception for "%s": %s', schema_table.name, str(ex))

    finally:
        try:
            os.remove(profile_file)
        except OSError:
            pass


def bulk_create_entity_realtime_connectivity(entries):
    """
    Bulk create EntityRealTimeConnectivity records from a list of dicts.
    """
    records = []
    for entry in entries:
        records.append(EntityRealTimeConnectivity(
            created=entry.get('timestamp'),
            connectivity_speed=entry.get('speed_download'),
            connectivity_upload_speed=entry.get('speed_upload'),
            connectivity_latency=entry.get('latency'),
            connectivity_speed_probe=entry.get('speed_download_probe'),
            connectivity_upload_speed_probe=entry.get('speed_upload_probe'),
            connectivity_latency_probe=entry.get('latency_probe'),
            connectivity_speed_mean=entry.get('speed_download_mean'),
            connectivity_upload_speed_mean=entry.get('speed_upload_mean'),
            roundtrip_time=entry.get('roundtrip_time'),
            jitter_download=entry.get('jitter_download'),
            jitter_upload=entry.get('jitter_upload'),
            rtt_packet_loss_pct=entry.get('rtt_packet_loss_pct'),
            entity=entry['entity'],
            version=entry.get('version'),
            live_data_source=statistics_configs.QOS_SOURCE,
        ))

    if records:
        EntityRealTimeConnectivity.objects.bulk_create(records)


def sync_entity_qos_realtime_data(country_id, entity_type_code='health', start_date=None, end_date=None):
    """
    Sync entity QoS real-time data from EntityRealTimeConnectivity to EntityDailyStatus.

    Mirrors sync_qos_realtime_data() but operates on entities.

    Args:
        country_id: The country ID to sync data for.
        entity_type_code: The entity type code to filter by.
        start_date: Optional start date for aggregation.
        end_date: Optional end date for aggregation.
    """
    current_datetime = core_utilities.get_current_datetime_object()

    if start_date and end_date:
        filter_kwargs = {
            'created__date__gte': start_date,
            'created__date__lte': end_date,
        }
    else:
        last_entry_date = EntityDailyStatus.objects.filter(
            live_data_source=statistics_configs.QOS_SOURCE,
            entity__country_id=country_id,
            entity__entity_type__code=entity_type_code,
            entity__deleted__isnull=True,
        ).order_by('-date').values_list('date', flat=True).first()

        if not last_entry_date:
            last_entry_date = (current_datetime - timedelta(days=1)).date()

        filter_kwargs = {
            'created__date__gte': last_entry_date,
            'created__date__lte': current_datetime.date(),
        }

    # Get realtime records for this country and entity type since last aggregation
    realtime_records = EntityRealTimeConnectivity.objects.filter(
        live_data_source=statistics_configs.QOS_SOURCE,
        entity__country_id=country_id,
        entity__entity_type__code=entity_type_code,
        entity__deleted__isnull=True,
        **filter_kwargs
    ).values(
        'created__date', 'entity_id',
    ).annotate(
        connectivity_speed_avg=Avg('connectivity_speed'),
        connectivity_upload_speed_avg=Avg('connectivity_upload_speed'),
        connectivity_latency_avg=Avg('connectivity_latency'),
        connectivity_speed_probe_avg=Avg('connectivity_speed_probe'),
        connectivity_upload_speed_probe_avg=Avg('connectivity_upload_speed_probe'),
        connectivity_latency_probe_avg=Avg('connectivity_latency_probe'),
        connectivity_speed_mean_avg=Avg('connectivity_speed_mean'),
        connectivity_upload_speed_mean_avg=Avg('connectivity_upload_speed_mean'),
        roundtrip_time_avg=Avg('roundtrip_time'),
        jitter_download_avg=Avg('jitter_download'),
        jitter_upload_avg=Avg('jitter_upload'),
        rtt_packet_loss_pct_avg=Avg('rtt_packet_loss_pct'),
    ).order_by('created__date')

    processed_entity_ids = set()
    processed_dates = set()
    valid_entity_ids = set()

    # Pre-fetch existing records
    existing_qs = EntityDailyStatus.objects.filter(
        live_data_source=statistics_configs.QOS_SOURCE,
        entity__country_id=country_id,
        entity__entity_type__code=entity_type_code,
        **filter_kwargs
    )
    existing_map = {(obj.entity_id, obj.date): obj for obj in existing_qs}

    daily_records_to_create = []
    daily_records_to_update = []

    for record in realtime_records:
        entity_id = record['entity_id']
        date = record['created__date']
        key = (entity_id, date)

        if record.get('connectivity_speed_avg') is not None or \
           record.get('connectivity_latency_avg') is not None or \
           record.get('connectivity_speed_probe_avg') is not None or \
           record.get('connectivity_latency_probe_avg') is not None or \
           record.get('connectivity_speed_mean_avg') is not None:
            valid_entity_ids.add(entity_id)

        if key in existing_map:
            obj = existing_map[key]
            obj.connectivity_speed = record.get('connectivity_speed_avg')
            obj.connectivity_upload_speed = record.get('connectivity_upload_speed_avg')
            obj.connectivity_latency = record.get('connectivity_latency_avg')
            obj.connectivity_speed_probe = record.get('connectivity_speed_probe_avg')
            obj.connectivity_upload_speed_probe = record.get('connectivity_upload_speed_probe_avg')
            obj.connectivity_latency_probe = record.get('connectivity_latency_probe_avg')
            obj.connectivity_speed_mean = record.get('connectivity_speed_mean_avg')
            obj.connectivity_upload_speed_mean = record.get('connectivity_upload_speed_mean_avg')
            obj.roundtrip_time = record.get('roundtrip_time_avg')
            obj.jitter_download = record.get('jitter_download_avg')
            obj.jitter_upload = record.get('jitter_upload_avg')
            obj.rtt_packet_loss_pct = record.get('rtt_packet_loss_pct_avg')
            daily_records_to_update.append(obj)
        else:
            daily_records_to_create.append(EntityDailyStatus(
                entity_id=entity_id,
                date=date,
                connectivity_speed=record.get('connectivity_speed_avg'),
                connectivity_upload_speed=record.get('connectivity_upload_speed_avg'),
                connectivity_latency=record.get('connectivity_latency_avg'),
                connectivity_speed_probe=record.get('connectivity_speed_probe_avg'),
                connectivity_upload_speed_probe=record.get('connectivity_upload_speed_probe_avg'),
                connectivity_latency_probe=record.get('connectivity_latency_probe_avg'),
                connectivity_speed_mean=record.get('connectivity_speed_mean_avg'),
                connectivity_upload_speed_mean=record.get('connectivity_upload_speed_mean_avg'),
                roundtrip_time=record.get('roundtrip_time_avg'),
                jitter_download=record.get('jitter_download_avg'),
                jitter_upload=record.get('jitter_upload_avg'),
                rtt_packet_loss_pct=record.get('rtt_packet_loss_pct_avg'),
                live_data_source=statistics_configs.QOS_SOURCE,
            ))

        processed_entity_ids.add(entity_id)
        if date:
            processed_dates.add(date)

        if len(daily_records_to_create) >= 5000:
            EntityDailyStatus.objects.bulk_create(daily_records_to_create, ignore_conflicts=True)
            logger.info('Entity QoS - Bulk created %d EntityDailyStatus records.', len(daily_records_to_create))
            daily_records_to_create = []

        if len(daily_records_to_update) >= 5000:
            EntityDailyStatus.objects.bulk_update(
                daily_records_to_update,
                ['connectivity_speed', 'connectivity_upload_speed', 'connectivity_latency',
                 'connectivity_speed_probe', 'connectivity_upload_speed_probe', 'connectivity_latency_probe',
                 'connectivity_speed_mean', 'connectivity_upload_speed_mean',
                 'roundtrip_time', 'jitter_download', 'jitter_upload', 'rtt_packet_loss_pct'],
            )
            logger.info('Entity QoS - Bulk updated %d EntityDailyStatus records.', len(daily_records_to_update))
            daily_records_to_update = []

    if daily_records_to_create:
        EntityDailyStatus.objects.bulk_create(daily_records_to_create, ignore_conflicts=True)
        logger.info('Entity QoS - Bulk created %d EntityDailyStatus records.', len(daily_records_to_create))

    if daily_records_to_update:
        EntityDailyStatus.objects.bulk_update(
            daily_records_to_update,
            ['connectivity_speed', 'connectivity_upload_speed', 'connectivity_latency',
             'connectivity_speed_probe', 'connectivity_upload_speed_probe', 'connectivity_latency_probe',
             'connectivity_speed_mean', 'connectivity_upload_speed_mean',
             'roundtrip_time', 'jitter_download', 'jitter_upload', 'rtt_packet_loss_pct'],
        )
        logger.info('Entity QoS - Bulk updated %d EntityDailyStatus records.', len(daily_records_to_update))

    if not processed_entity_ids:
        logger.info('Entity QoS - No entities processed, skipping registration and weekly aggregation.')
        return

    min_date = min(processed_dates) if processed_dates else (
        start_date or current_datetime.date()
    )
    max_date = max(processed_dates) if processed_dates else (
        end_date or current_datetime.date()
    )

    if settings.USE_TZ:
        reg_datetime = timezone.make_aware(dt_class.combine(min_date, dt_class.min.time()))
    else:
        reg_datetime = dt_class.combine(min_date, dt_class.min.time())

    existing_regs = {
        reg.entity_id: reg
        for reg in EntityRealTimeRegistration.objects.all_records().filter(
            entity_id__in=processed_entity_ids
        )
    }

    to_create = []
    to_update = []
    for entity_id in processed_entity_ids.intersection(valid_entity_ids):
        reg = existing_regs.get(entity_id)
        if reg:
            updated = False
            if not reg.rt_registered:
                reg.rt_registered = True
                updated = True
            if reg.rt_source != statistics_configs.QOS_SOURCE:
                reg.rt_source = statistics_configs.QOS_SOURCE
                updated = True
            if not reg.rt_registration_date or reg.rt_registration_date > reg_datetime:
                reg.rt_registration_date = reg_datetime
                updated = True
            if updated:
                to_update.append(reg)
        else:
            to_create.append(EntityRealTimeRegistration(
                entity_id=entity_id,
                rt_registered=True,
                rt_source=statistics_configs.QOS_SOURCE,
                rt_registration_date=reg_datetime,
            ))

    if to_create:
        EntityRealTimeRegistration.objects.bulk_create(to_create, batch_size=5000)
        logger.info('Entity QoS - Created %d EntityRealTimeRegistration records.', len(to_create))
    if to_update:
        EntityRealTimeRegistration.objects.bulk_update(
            to_update,
            ['rt_registered', 'rt_source', 'rt_registration_date'],
            batch_size=5000,
        )
        logger.info('Entity QoS - Updated %d EntityRealTimeRegistration records.', len(to_update))

    # --- Aggregate daily status to weekly status ---
    from proco.data_sources.tasks import finalize_previous_day_entity_data

    country_obj = Country.objects.get(id=country_id)
    current_date = min_date
    while current_date < max_date:
        finalize_previous_day_entity_data.delay(None, country_obj.id, current_date, entity_type_code)
        current_date += timedelta(days=7)
    # Ensure the final week is covered
    finalize_previous_day_entity_data.delay(None, country_obj.id, max_date, entity_type_code)
    logger.info('Entity QoS - Weekly aggregation completed for country %s (%s to %s).',
                country_obj.name, min_date, max_date)
