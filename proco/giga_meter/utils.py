import logging

import delta_sharing
from delta_sharing.reader import DeltaSharingReader
from django.conf import settings
from requests.exceptions import HTTPError

from proco.giga_meter import models as giga_meter_models
from proco.data_sources import utils as sources_utilities

logger = logging.getLogger('gigamaps.' + __name__)

ds_settings = settings.DATA_SOURCE_CONFIG


def sync_school_master_data(
    profile_file,
    share_name,
    schema_name,
    table_name,
    school_master_fields
):
    country = giga_meter_models.GigaMeter_Country.objects.filter(iso3_format=table_name, ).first()
    logger.debug('Country object: {0}'.format(country))

    if not country:
        logger.warning('Country with ISO3 Format ({0}) not found in DB. '
                       'Hence skipping the load for current table.'.format(table_name))
        return

    country_latest_school_master_data_version = giga_meter_models.GigaMeter_SchoolMasterData.get_last_version(
        table_name)
    logger.debug('Country latest school master data version present in DB: {0}'.format(
        country_latest_school_master_data_version))

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

    if country_latest_school_master_data_version == table_current_version:
        logger.info('Both School Master data version in DB and Table version from API, are same. '
                    'Hence skipping the data update for current country ({0}).'.format(country))
        return

    if country_latest_school_master_data_version is not None and table_current_version is not None and country_latest_school_master_data_version > table_current_version:
        logger.warning(
            'School Master start version ({0}) in DB is greater than remote table version ({1}) for country ({2}). '
            'Pulling full data from version 0.'.format(country_latest_school_master_data_version, table_current_version, table_name)
        )
        country_latest_school_master_data_version = 0

    try:
        loaded_data_df = delta_sharing.load_table_changes_as_pandas(
            table_url,
            country_latest_school_master_data_version,
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
                                                                              'country_id', 'status',]
        logger.debug('All School Master API response columns: {}'.format(df_columns))
        logger.debug('All School Master API response columns to delete: {}'.format(
            list(set(df_columns) - set(school_master_fields))))

        insert_entries = []

        loaded_data_df = sources_utilities.normalize_school_master_data_frame(loaded_data_df)
        loaded_data_df.drop(columns=['is_verified_school'], inplace=True, errors='ignore')

        loaded_data_df['version'] = table_current_version
        loaded_data_df['country'] = country

        for _, row in loaded_data_df.iterrows():
            change_type = row[DeltaSharingReader._change_type_col_name()]

            row.drop(
                labels=cols_to_delete,
                inplace=True,
                errors='ignore',
            )

            if change_type in ['insert', 'update_postimage', 'remove', 'delete']:
                if change_type in ['remove', 'delete']:
                    row['status'] = giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_DELETED

                row_as_dict = sources_utilities.parse_row(row)
                insert_entries.append(giga_meter_models.GigaMeter_SchoolMasterData(**row_as_dict))

                if len(insert_entries) == 5000:
                    logger.debug('Loading the data to "SchoolMasterData" table as it has reached 5000 benchmark.')
                    giga_meter_models.GigaMeter_SchoolMasterData.objects.bulk_create(insert_entries)
                    insert_entries = []
                    logger.debug('#' * 10)
                    logger.debug('\n\n')

        logger.info('Loading the remaining ({0}) data to "SchoolMasterData" table.'.format(len(insert_entries)))
        if len(insert_entries) > 0:
            giga_meter_models.GigaMeter_SchoolMasterData.objects.bulk_create(insert_entries)
    else:
        logger.info('No data to update in current table: {0}.'.format(table_name))

    giga_meter_models.GigaMeter_SchoolMasterData.set_last_version(table_current_version, table_name)


def sync_master_data(config, profile_file, share_name, schema_name, table_name, master_fields):
    """
    Generic Delta Sharing CDC pull into a facility type's intermediate table.
    Parameterized by a FacilityTypeConfig (proco/giga_meter/facility_types.py) -
    replaces sync_school_master_data's now-generic sibling for every type other
    than `school` itself, which stays on the function above (see facility_types.py
    docstring for why school hasn't been migrated onto this yet).
    """
    country = giga_meter_models.GigaMeter_Country.objects.filter(iso3_format=table_name).first()
    logger.debug('Country object: {0}'.format(country))

    if not country:
        logger.error(
            'Country with ISO3 Format ({0}) not found in DB. '
            'Hence skipping the {1} load for current table.'.format(table_name, config.label),
        )
        raise ValueError(f"Invalid 'iso3_format': {table_name}")

    country_latest_version = config.intermediate_model.get_last_version(table_name)
    logger.debug(
        'Country latest {0} master data version present in DB: {1}'.format(config.label, country_latest_version),
    )

    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
        share_name=share_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    logger.debug('%s master table URL: %s', config.label, table_url)

    table_current_version = delta_sharing.get_table_version(table_url)
    logger.debug('{0} master table current version from API: {1}'.format(config.label, table_current_version))

    if country_latest_version == table_current_version:
        logger.info(
            'Both {0} Master data version in DB and Table version from API are same. '
            'Hence skipping the data update for current country ({1}).'.format(config.label, country),
        )
        return

    loaded_data_df = delta_sharing.load_table_changes_as_pandas(
        table_url,
        country_latest_version,
        table_current_version,
        None,
        None,
    )
    logger.debug('Total count of {0} master rows in the data: {1}'.format(config.label, len(loaded_data_df)))

    if len(loaded_data_df) > 0:
        loaded_data_df = loaded_data_df.sort_values(
            by=[
                DeltaSharingReader._commit_version_col_name(),
                DeltaSharingReader._commit_timestamp_col_name(),
            ],
            na_position='first',
        )
        loaded_data_df.drop_duplicates(
            subset=[config.id_field],
            keep='last',
            inplace=True,
        )
        loaded_data_df = loaded_data_df[loaded_data_df[DeltaSharingReader._change_type_col_name()].isin(
            ['insert', 'update_postimage', 'remove', 'delete'],
        )]

        logger.debug(
            'Total count of {0} rows after duplicate cleanup: {1}'.format(config.label, len(loaded_data_df)),
        )

        df_columns = list(loaded_data_df.columns.tolist())
        cols_to_delete = list(set(df_columns) - set(master_fields)) + [
            'id', 'created', 'modified', 'country_id', 'status',
        ]
        logger.debug('All {0} Master API response columns: {1}'.format(config.label, df_columns))
        logger.debug('All {0} Master API response columns to delete: {1}'.format(
            config.label, list(set(df_columns) - set(master_fields)),
        ))

        insert_entries = []

        loaded_data_df = config.normalize_frame(loaded_data_df)

        loaded_data_df['version'] = table_current_version
        loaded_data_df['country'] = country

        for _, row in loaded_data_df.iterrows():
            change_type = row[DeltaSharingReader._change_type_col_name()]

            row.drop(
                labels=cols_to_delete,
                inplace=True,
                errors='ignore',
            )

            if change_type in ['insert', 'update_postimage', 'remove', 'delete']:
                if change_type in ['remove', 'delete']:
                    row['status'] = config.intermediate_model.ROW_STATUS_DELETED

                row_as_dict = sources_utilities.parse_row(row)
                insert_entries.append(config.intermediate_model(**row_as_dict))

                if len(insert_entries) == 5000:
                    logger.debug(
                        'Loading the data to "{0}" table as it has reached 5000 benchmark.'.format(
                            config.intermediate_model.__name__,
                        ),
                    )
                    config.intermediate_model.objects.bulk_create(insert_entries)
                    insert_entries = []
                    logger.debug('#' * 10)

        logger.info(
            'Loading the remaining ({0}) data to "{1}" table.'.format(
                len(insert_entries), config.intermediate_model.__name__,
            ),
        )
        if len(insert_entries) > 0:
            config.intermediate_model.objects.bulk_create(insert_entries)
    else:
        logger.info('No {0} master data to update in current table: {1}.'.format(config.label, table_name))

    config.intermediate_model.set_last_version(table_current_version, table_name)
