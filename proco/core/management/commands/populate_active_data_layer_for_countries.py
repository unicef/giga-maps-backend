# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.management.base import BaseCommand

from proco.accounts import models as accounts_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.core import db_utils as db_utilities
from proco.core.utils import get_current_datetime_object
from proco.locations.models import Country


def delete_relationships(country_id, layer_id):
    relationships = accounts_models.DataLayerCountryRelationship.objects.all()

    if country_id:
        relationships = relationships.filter(country_id=country_id)

    if layer_id:
        relationships = relationships.filter(data_layer_id=layer_id)

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
        print('*** Active Data Layer for Country Mapping operations STARTED ***')

        country_id = options.get('country_id', None)
        layer_id = options.get('layer_id', None)

        if options.get('reset_mapping', False):
            print('DELETE_OLD_RECORDS - START')
            delete_relationships(country_id, layer_id)
            print('DELETE_OLD_RECORDS - END')

        all_published_layers = accounts_models.DataLayer.objects.all()
        if layer_id:
            all_published_layers = all_published_layers.filter(id=layer_id)

        if country_id:
            all_country_ids = [country_id, ]
        else:
            all_country_ids = list(Country.objects.all().values_list('id', flat=True).order_by('id'))

        if all_published_layers.count() > 0 and len(all_country_ids) > 0:
            print('RELATIONSHIP_CREATION - START')
            for data_layer_instance in all_published_layers:
                data_sources = data_layer_instance.data_sources.all()

                live_data_sources = ['UNKNOWN']

                for d in data_sources:
                    source_type = d.data_source.data_source_type
                    if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                        live_data_sources.append(statistics_configs.QOS_SOURCE)
                    elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                        live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

                # country_ids = data_layer_instance.applicable_countries
                parameter_col = data_sources.first().data_source_column

                parameter_column_name = str(parameter_col['name'])
                parameter_column_type = str(parameter_col['type'])

                if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                    sql = """
                    SELECT DISTINCT s."country_id"
                    FROM "connection_statistics_schooldailystatus" AS sds
                    INNER JOIN "schools_school" AS s ON (sds."school_id" = s."id")
                    WHERE s."deleted" IS NULL
                      AND sds."deleted" IS NULL
                      AND sds."live_data_source" IN ({live_source_types})
                      AND s."country_id" IN ({country_ids})
                      AND sds.{col_name} IS NOT NULL
                    ORDER BY s."country_id" ASC
                    """.format(
                        live_source_types=','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                        country_ids=','.join([str(country_id) for country_id in all_country_ids]),
                        col_name=parameter_column_name
                    )
                    all_country_ids_has_layer_data = db_utilities.sql_to_response(sql,
                                                                                  label='DataLayerCountryRelationship')

                    for country_id_has_layer_data in all_country_ids_has_layer_data:
                        relationship_instance, created = accounts_models.DataLayerCountryRelationship.objects.update_or_create(
                            data_layer=data_layer_instance,
                            country_id=country_id_has_layer_data['country_id'],
                            defaults={
                                'is_default': not data_layer_instance.created_by,
                                'last_modified_at': get_current_datetime_object(),
                            },
                        )
                        if created:
                            print('New DataLayers + Country Relationship created for LIVE LAYER: {0}'.format(
                                relationship_instance.__dict__))
                        else:
                            print('Existing DataLayers + Country Relationship updated for LIVE LAYER: {0}'.format(
                                relationship_instance.__dict__))
                elif data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_STATIC:
                    unknown_condition = ''
                    if parameter_column_type == 'str':
                        unknown_condition = "AND sws.{col_name} != 'unknown'".format(col_name=parameter_column_name)

                    sql = """
                        SELECT DISTINCT s."country_id"
                        FROM "connection_statistics_schoolweeklystatus" AS sws
                        INNER JOIN "schools_school" AS s ON (sws."school_id" = s."id")
                        WHERE s."deleted" IS NULL
                          AND sws."deleted" IS NULL
                          AND s."country_id" IN ({country_ids})
                          AND sws.{col_name} IS NOT NULL
                          {unknown_condition}
                        ORDER BY s."country_id" ASC
                    """.format(
                        country_ids=','.join([str(country_id) for country_id in all_country_ids]),
                        col_name=parameter_column_name,
                        unknown_condition=unknown_condition,
                    )
                    all_country_ids_has_layer_data = db_utilities.sql_to_response(sql,
                                                                                  label='DataLayerCountryRelationship')

                    for country_id_has_layer_data in all_country_ids_has_layer_data:
                        relationship_instance, created = accounts_models.DataLayerCountryRelationship.objects.update_or_create(
                            data_layer=data_layer_instance,
                            country_id=country_id_has_layer_data['country_id'],
                            defaults={
                                'is_default': False,
                                'last_modified_at': get_current_datetime_object(),
                            },
                        )
                        if created:
                            print('New DataLayers + Country Relationship created for STATIC LAYER: {0}'.format(
                                relationship_instance.__dict__))
                        else:
                            print('Existing DataLayers + Country Relationship updated for STATIC LAYER: {0}'.format(
                                relationship_instance.__dict__))

        print('*** Active Data Layer for Country Mapping operations ***')
