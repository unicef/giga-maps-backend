import logging

import delta_sharing
from delta_sharing.reader import DeltaSharingReader
from django.conf import settings

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
        logger.error('Country with ISO3 Format ({0}) not found in DB. '
                     'Hence skipping the load for current table.'.format(table_name))
        raise ValueError(f"Invalid 'iso3_format': {table_name}")

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

    table_current_version = delta_sharing.get_table_version(table_url)
    logger.debug('Table current version from API: {0}'.format(table_current_version))

    if country_latest_school_master_data_version == table_current_version:
        logger.info('Both School Master data version in DB and Table version from API, are same. '
                    'Hence skipping the data update for current country ({0}).'.format(country))
        return

    loaded_data_df = delta_sharing.load_table_changes_as_pandas(
        table_url,
        country_latest_school_master_data_version,
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
                                                                              'country_id', 'status',]
        logger.debug('All School Master API response columns: {}'.format(df_columns))
        logger.debug('All School Master API response columns to delete: {}'.format(
            list(set(df_columns) - set(school_master_fields))))

        insert_entries = []

        loaded_data_df = sources_utilities.normalize_school_master_data_frame(loaded_data_df)

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
