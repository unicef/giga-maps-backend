# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
from django.core.management.base import BaseCommand

from django.core.exceptions import FieldDoesNotExist
from django.db import transaction

from proco.accounts import models as accounts_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.core import db_utils as db_utilities
from proco.core.utils import get_current_datetime_object
from proco.locations.models import Country
from proco.entities.models import Entity

logger = logging.getLogger('gigamaps.' + __name__)


def delete_relationships(country_id, layer_id, excluded_ids):
    relationships = accounts_models.DataLayerCountryRelationship.objects.all()

    if country_id:
        relationships = relationships.filter(country_id=country_id)

    if layer_id:
        relationships = relationships.filter(data_layer_id=layer_id)

    if len(excluded_ids) > 0:
        relationships = relationships.exclude(id__in=excluded_ids)

    relationships.update(deleted=get_current_datetime_object())


class Command(BaseCommand):
    help = 'Create/Update the DataLayer - Country relationship table.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset', action='store_true', dest='reset_mapping', default=False,
            help='If provided, already created mapping will be deleted and new mapping will be created.'
        )

        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Pass the Country ID in case want to perform the update for single country.'
        )

        parser.add_argument(
            '-layer_id', dest='layer_id', required=False, type=int,
            help='Pass the Layer ID in case want to perform the update for single layer.'
        )

    def handle(self, **options):
        logger.info('Active data layer for country mapping operations started.')

        country_id = options.get('country_id', None)
        layer_id = options.get('layer_id', None)
        ids_to_keep = []

        all_published_layers = accounts_models.DataLayer.objects.all()
        if layer_id:
            all_published_layers = all_published_layers.filter(id=layer_id)

        if country_id:
            all_country_ids = [country_id, ]
        else:
            all_country_ids = list(Country.objects.all().values_list('id', flat=True).order_by('id'))

        if all_published_layers.count() > 0 and len(all_country_ids) > 0:
            logger.info('Relationship creation - start')
            for data_layer_instance in all_published_layers:
                data_sources = data_layer_instance.data_sources.all()

                live_data_sources = ['UNKNOWN']

                for d in data_sources:
                    source_type = d.data_source.data_source_type
                    if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                        live_data_sources.append(statistics_configs.QOS_SOURCE)
                    elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                        live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

                applicable_country_ids = data_layer_instance.applicable_countries
                parameter_col = data_sources.first().data_source_column

                parameter_column_name = str(parameter_col['name'])
                parameter_column_type = str(parameter_col['type'])

                accounts_models.DataLayerCountryRelationship.objects.filter(
                    data_layer=data_layer_instance,
                ).exclude(
                    country_id__in=applicable_country_ids,
                ).update(
                    is_applicable=False,
                )

                is_entity_layer = (
                    data_layer_instance.entity_type is not None
                    and not data_layer_instance.entity_type.is_legacy
                )

                if is_entity_layer:
                    entity_detail_table = 'entities_{0}_entity'.format(
                        data_layer_instance.entity_type.code
                    )
                    detail_model_class = data_layer_instance.entity_type.get_detail_model_class()
                    col_in_detail_table = False
                    if detail_model_class:
                        try:
                            detail_model_class._meta.get_field(parameter_column_name)
                            col_in_detail_table = True
                        except FieldDoesNotExist:
                            pass

                if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                    parameter_table_name = str(parameter_col.get('table_name', 'sds'))
                    if is_entity_layer:
                        if col_in_detail_table:
                            sql = """
                            SELECT DISTINCT e."country_id"
                            FROM "entities_entity" AS e
                            INNER JOIN "{entity_detail_table}" AS ehe ON (ehe."entity_id" = e."id")
                            WHERE e."deleted" IS NULL
                              AND e."country_id" IN ({country_ids})
                              AND ehe."{col_name}" IS NOT NULL
                            ORDER BY e."country_id" ASC
                            """.format(
                                entity_detail_table=entity_detail_table,
                                country_ids=','.join([str(cid) for cid in all_country_ids]),
                                col_name=parameter_column_name,
                            )
                        else:
                            sql = """
                            SELECT DISTINCT e."country_id"
                            FROM "entities_entity" AS e
                            INNER JOIN "connection_statistics_entitydailystatus" AS eds
                                ON (eds."entity_id" = e."id")
                            WHERE e."deleted" IS NULL
                              AND eds."deleted" IS NULL
                              AND eds."live_data_source" IN ({live_source_types})
                              AND e."country_id" IN ({country_ids})
                              AND eds."{col_name}" IS NOT NULL
                            ORDER BY e."country_id" ASC
                            """.format(
                                live_source_types=','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                                country_ids=','.join([str(cid) for cid in all_country_ids]),
                                col_name=parameter_column_name,
                            )
                    else:
                        sql = """
                        SELECT DISTINCT schools_school."country_id"
                        FROM "connection_statistics_schooldailystatus" AS sds
                        INNER JOIN "schools_school" ON (sds."school_id" = schools_school."id")
                        WHERE schools_school."deleted" IS NULL
                          AND sds."deleted" IS NULL
                          AND sds."live_data_source" IN ({live_source_types})
                          AND schools_school."country_id" IN ({country_ids})
                          AND {table_name}.{col_name} IS NOT NULL
                        ORDER BY schools_school."country_id" ASC
                        """.format(
                            live_source_types=','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                            country_ids=','.join([str(cid) for cid in all_country_ids]),
                            table_name=parameter_table_name,
                            col_name=parameter_column_name,
                        )

                    try:
                        with transaction.atomic():
                            all_country_ids_has_layer_data = db_utilities.sql_to_response(sql,
                                                                                        label='DataLayerCountryRelationship')
                            # Handle None response from sql_to_response
                            if all_country_ids_has_layer_data is None:
                                all_country_ids_has_layer_data = []
                    except Exception as e:
                        logger.error(f'Error executing SQL query for layer {data_layer_instance.id}: {e}')
                        all_country_ids_has_layer_data = []

                    for country_id_has_layer_data in all_country_ids_has_layer_data:
                        country_id = country_id_has_layer_data['country_id']

                        is_applicable = True
                        if len(applicable_country_ids) > 0 and country_id not in applicable_country_ids:
                            is_applicable = False

                        relationship_instance, created = (
                            accounts_models.DataLayerCountryRelationship.objects.update_or_create(
                                data_layer=data_layer_instance,
                                country_id=country_id,
                                defaults={
                                    # 'is_default': not data_layer_instance.created_by,
                                    'is_applicable': is_applicable,
                                    'last_modified_at': get_current_datetime_object(),
                                },
                            )
                        )
                        ids_to_keep.append(relationship_instance.id)
                        if created:
                            logger.info('New dataLayers + country relationship created for live layer: {0}'.format(
                                relationship_instance.__dict__))
                        else:
                            logger.info(
                                'Existing dataLayers + country relationship updated for live layer: {0}'.format(
                                    relationship_instance.__dict__))
                elif data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_STATIC:
                    unknown_condition = ''
                    parameter_table_name = str(parameter_col.get('table_name', 'sws'))
                    if parameter_column_type == 'str':
                        if is_entity_layer:
                            table_prefix = 'ehe' if col_in_detail_table else 'e'
                            unknown_condition = "AND {t}.\"{c}\" != 'unknown'".format(
                                t=table_prefix, c=parameter_column_name
                            )
                        else:
                            unknown_condition = "AND {table_name}.{col_name} != 'unknown'".format(
                                table_name=parameter_table_name,
                                col_name=parameter_column_name
                            )

                    if is_entity_layer:
                        if col_in_detail_table:
                            sql = """
                            SELECT DISTINCT e."country_id"
                            FROM "entities_entity" AS e
                            INNER JOIN "{entity_detail_table}" AS ehe ON (ehe."entity_id" = e."id")
                            WHERE e."deleted" IS NULL
                              AND e."country_id" IN ({country_ids})
                              AND ehe."{col_name}" IS NOT NULL
                              {unknown_condition}
                            ORDER BY e."country_id" ASC
                            """.format(
                                entity_detail_table=entity_detail_table,
                                country_ids=','.join([str(cid) for cid in all_country_ids]),
                                col_name=parameter_column_name,
                                unknown_condition=unknown_condition,
                            )
                        else:
                            sql = """
                            SELECT DISTINCT e."country_id"
                            FROM "entities_entity" AS e
                            WHERE e."deleted" IS NULL
                              AND e."country_id" IN ({country_ids})
                              AND e."{col_name}" IS NOT NULL
                              {unknown_condition}
                            ORDER BY e."country_id" ASC
                            """.format(
                                country_ids=','.join([str(cid) for cid in all_country_ids]),
                                col_name=parameter_column_name,
                                unknown_condition=unknown_condition,
                            )
                    else:
                        sql = """
                            SELECT DISTINCT schools_school."country_id"
                            FROM "schools_school"
                            INNER JOIN "connection_statistics_schoolweeklystatus" AS sws
                                ON (sws."id" = schools_school."last_weekly_status_id")
                            WHERE schools_school."deleted" IS NULL
                              AND schools_school."country_id" IN ({country_ids})
                              AND {table_name}.{col_name} IS NOT NULL
                              {unknown_condition}
                            ORDER BY schools_school."country_id" ASC
                        """.format(
                            country_ids=','.join([str(cid) for cid in all_country_ids]),
                            table_name=parameter_table_name,
                            col_name=parameter_column_name,
                            unknown_condition=unknown_condition,
                        )

                    try:
                        with transaction.atomic():
                            all_country_ids_has_layer_data = db_utilities.sql_to_response(sql,
                                                                                        label='DataLayerCountryRelationship')
                            # Handle None response from sql_to_response
                            if all_country_ids_has_layer_data is None:
                                all_country_ids_has_layer_data = []
                    except Exception as e:
                        logger.error(f'Error executing SQL query for layer {data_layer_instance.id}: {e}')
                        all_country_ids_has_layer_data = []

                    for country_id_has_layer_data in all_country_ids_has_layer_data:
                        country_id = country_id_has_layer_data['country_id']

                        is_applicable = True
                        if len(applicable_country_ids) > 0 and country_id not in applicable_country_ids:
                            is_applicable = False

                        relationship_instance, created = (
                            accounts_models.DataLayerCountryRelationship.objects.update_or_create(
                                data_layer=data_layer_instance,
                                country_id=country_id,
                                defaults={
                                    'is_default': False,
                                    'is_applicable': is_applicable,
                                    'last_modified_at': get_current_datetime_object(),
                                },
                            )
                        )
                        ids_to_keep.append(relationship_instance.id)
                        if created:
                            logger.info('New dataLayers + country relationship created for static layer: {0}'.format(
                                relationship_instance.__dict__))
                        else:
                            logger.info(
                                'Existing dataLayers + country relationship updated for static layer: {0}'.format(
                                    relationship_instance.__dict__))

        if options.get('reset_mapping', False):
            logger.info('Delete records which are not active now - start')
            delete_relationships(country_id, layer_id, ids_to_keep)
            logger.info('Delete records which are not active now - end')

        logger.info('Active data layer for country mapping operations.')
