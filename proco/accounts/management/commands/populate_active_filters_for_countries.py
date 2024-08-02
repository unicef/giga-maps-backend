# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from django.core.management.base import BaseCommand
from django.db.models import F

from proco.accounts import models as accounts_models
from proco.core.utils import get_current_datetime_object
from proco.schools.models import School

logger = logging.getLogger('gigamaps.' + __name__)


def delete_relationships(country_id, filter_id, excluded_ids):
    relationships = accounts_models.AdvanceFilterCountryRelationship.objects.all()

    if country_id:
        relationships = relationships.filter(country_id=country_id)

    if filter_id:
        relationships = relationships.filter(advance_filter_id=filter_id)

    if len(excluded_ids) > 0:
        relationships = relationships.exclude(id__in=excluded_ids)

    relationships.update(deleted=get_current_datetime_object())


class Command(BaseCommand):
    help = 'Create/Update the Advance Filter - Country relationship table.'

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
            '-filter_id', dest='filter_id', required=False, type=int,
            help='Pass the Advance Filter ID in case want to perform the update for single filter.'
        )

    def handle(self, **options):
        logger.info('Active advance filter + country mapping operations started.')

        country_id = options.get('country_id', None)
        filter_id = options.get('filter_id', None)
        ids_to_keep = []

        all_published_filters = accounts_models.AdvanceFilter.objects.all()
        if filter_id:
            all_published_filters = all_published_filters.filter(id=filter_id)

        if country_id:
            all_country_ids = [country_id, ]
        else:
            all_country_ids = list(School.objects.all().order_by('country_id').values_list(
                'country_id', flat=True).distinct('country_id'))

        if all_published_filters.count() > 0 and len(all_country_ids) > 0:
            logger.info('Relationship creation - start')
            for filter_instance in all_published_filters:
                parameter_details = filter_instance.column_configuration
                parameter_field = parameter_details.name
                parameter_table = parameter_details.table_alias
                param_options = parameter_details.options

                last_weekly_status_field = 'last_weekly_status__{}'.format(parameter_field)

                if isinstance(param_options, dict) and 'active_countries_filter' in param_options:
                    active_countries_sql_filter = param_options['active_countries_filter']

                    if active_countries_sql_filter:
                        country_qs = School.objects.all()
                        if parameter_table == 'school_static':
                            country_qs = country_qs.select_related('last_weekly_status').annotate(**{
                                parameter_table + '_' + parameter_field: F(last_weekly_status_field)
                            })

                        all_country_ids_has_filter_data = list(country_qs.extra(
                            where=[active_countries_sql_filter],
                        ).order_by('country_id').values_list('country_id', flat=True).distinct('country_id'))
                    else:
                        all_country_ids_has_filter_data = all_country_ids

                    for country_id_has_filter_data in all_country_ids_has_filter_data:
                        relationship_instance, created = (
                            accounts_models.AdvanceFilterCountryRelationship.objects.update_or_create(
                                advance_filter=filter_instance,
                                country_id=country_id_has_filter_data,
                            )
                        )
                        ids_to_keep.append(relationship_instance.id)
                        if created:
                            logger.debug('New AdvanceFilter + country relationship created: {0}'.format(
                                relationship_instance.__dict__))
                        else:
                            logger.debug(
                                'Existing AdvanceFilter + country relationship updated: {0}'.format(
                                    relationship_instance.__dict__))

        if options.get('reset_mapping', False):
            logger.info('Delete records which are not active now - start')
            delete_relationships(country_id, filter_id, ids_to_keep)
            logger.info('Delete records which are not active now - end')

        logger.info('Active advance filter for country mapping operations ended.')
