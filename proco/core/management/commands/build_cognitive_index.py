# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import time

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import CorsOptions, SearchIndex
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import (
    Prefetch,
    F,
    Case,
    IntegerField,
    Value,
    When,
    FilteredRelation,
    Q,
    CharField,
)
from django.db.models.functions import Concat

from proco.core.utils import is_blank_string
from proco.entities.models import Entity
from proco.locations.models import Country
from proco.locations.search_indexes import EntityIndex
from proco.schools.models import School

logger = logging.getLogger('gigamaps.' + __name__)

# Create a service client
cognitive_search_settings = settings.AZURE_CONFIG.get('COGNITIVE_SEARCH')


def delete_index():
    # Create a service client
    admin_client = SearchIndexClient(cognitive_search_settings['SEARCH_ENDPOINT'],
                                     AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    try:
        result = admin_client.delete_index(EntityIndex.Meta.index_name)
        logger.info('Index "{0}" deleted successfully'.format(EntityIndex.Meta.index_name))
        logger.info(result)
    except Exception as ex:
        logger.error(ex)


def create_index():
    # Create a service client
    admin_client = SearchIndexClient(cognitive_search_settings['SEARCH_ENDPOINT'],
                                     AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    # Create the index
    fields = [
        getattr(EntityIndex, attr)
        for attr in dir(EntityIndex)
        if not callable(getattr(EntityIndex, attr)) and not attr.startswith("__")
    ]

    cors_options = CorsOptions(allowed_origins=['*'], max_age_in_seconds=24 * 60 * 60)
    scoring_profiles = []

    logger.info('Index name: {0}'.format(EntityIndex.Meta.index_name))

    index = SearchIndex(
        name=EntityIndex.Meta.index_name,
        fields=fields,
        scoring_profiles=scoring_profiles,
        cors_options=cors_options
    )

    try:
        result = admin_client.create_index(index)
        logger.info('Index "{0}" created successfully'.format(result.name))
    except Exception as ex:
        logger.error(ex)


def clear_index():
    search_client = SearchClient(cognitive_search_settings['SEARCH_ENDPOINT'], EntityIndex.Meta.index_name,
                                 AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    doc_counts = search_client.get_document_count()
    logger.info("There are {0} documents in the {1} search index.".format(
        doc_counts, repr(EntityIndex.Meta.index_name)))

    if doc_counts > 0:
        all_docs = search_client.search('*')
        logger.info('All documents: {0}'.format(all_docs))

        search_client.delete_documents(all_docs)


def collect_data(country_id, school_id):
    qry_fields = [
        attr
        for attr in dir(EntityIndex)
        if not callable(getattr(EntityIndex, attr)) and not attr.startswith("__")
    ]

    queryset = School.objects.all()

    qs = queryset.prefetch_related(
        Prefetch('country', Country.objects.defer('geometry')),
    ).annotate(
        srr=FilteredRelation(
            'realtime_registration_status',
            condition=Q(realtime_registration_status__rt_registered=True)
                      & Q(realtime_registration_status__deleted__isnull=True),
        )
    ).annotate(
        entity_name=Value('school', output_field=CharField()),
        entity_id=Concat('id', Value('-'), 'giga_id_school', output_field=CharField()),
        giga_id=F('giga_id_school'),
        country_name=F('country__name'),
        country_code=F('country__code'),
        admin1_name=F('admin1__name'),
        admin2_name=F('admin2__name'),
        row_score=Case(When(srr__id__isnull=True, then=Value(0)), default=1, output_field=IntegerField())
    ).values(*qry_fields).order_by(*EntityIndex.Meta.ordering).distinct(*qry_fields)

    if country_id:
        qs = qs.filter(country_id=country_id)

    if school_id:
        qs = qs.filter(id=school_id)

    docs = []
    for qry_data in qs:
        if is_blank_string(qry_data['admin1_name']):
            qry_data['admin1_name'] = 'Unknown'
            del qry_data['admin1_id']
        if is_blank_string(qry_data['admin2_name']):
            del qry_data['admin2_name']
            del qry_data['admin2_id']
        docs.append(qry_data)

    logger.info('Total records to upload: {0}'.format(len(docs)))
    return docs


def collect_data_for_entities(country_id, entity_id, entity_name):
    qry_fields = [
        attr
        for attr in dir(EntityIndex)
        if not callable(getattr(EntityIndex, attr)) and not attr.startswith("__")
    ]

    queryset = EntityIndex.Meta.model.objects.all()

    qs = queryset.prefetch_related(
        Prefetch('country', Country.objects.defer('geometry')),
    ).annotate(
        srr=FilteredRelation(
            'realtime_registration_status',
            condition=Q(realtime_registration_status__rt_registered=True)
                      & Q(realtime_registration_status__deleted__isnull=True),
        )
    ).annotate(
        entity_id=Concat('id', Value('-'), 'giga_id', output_field=CharField()),
        country_name=F('country__name'),
        country_code=F('country__code'),
        admin1_name=F('admin1__name'),
        admin2_name=F('admin2__name'),
        row_score=Case(When(srr__id__isnull=True, then=Value(0)), default=1, output_field=IntegerField())
    ).values(*qry_fields).order_by(*EntityIndex.Meta.ordering).distinct(*qry_fields)

    if country_id:
        qs = qs.filter(country_id=country_id)

    if entity_id:
        qs = qs.filter(id=entity_id)

    if entity_name:
        qs = qs.filter(entity_name=entity_name)
    print(qs.query)

    docs = []
    for qry_data in qs:
        if is_blank_string(qry_data['admin1_name']):
            qry_data['admin1_name'] = 'Unknown'
            del qry_data['admin1_id']
        if is_blank_string(qry_data['admin2_name']):
            del qry_data['admin2_name']
            del qry_data['admin2_id']
        docs.append(qry_data)

    logger.info('Total records to upload: {0}'.format(len(docs)))
    return docs


def divide_chunks(data_list, batch_size=1000):
    # looping till length l
    for i in range(0, len(data_list), batch_size):
        yield data_list[i:i + batch_size]


def upload_docs(search_client, headers, data_chunk, failed_data_chunks, count, retry_no=1):
    uploaded = False

    while retry_no <= 3 and not uploaded:
        try:
            result = search_client.upload_documents(documents=data_chunk, headers=headers)
            logger.info("Upload of new document succeeded for count '{0}' in retry no: '{1}': {2}".format(
                count, retry_no, result[0].succeeded)
            )
            uploaded = True
            break
        except Exception as ex:
            logger.error(
                "Upload of new document failed for count '{0}' in retry no: '{1}': {2}".format(count, retry_no, ex))
            time.sleep(1.0)
            retry_no += 1
            uploaded = upload_docs(search_client, headers, data_chunk, failed_data_chunks, count, retry_no=retry_no)
    return uploaded


def load_index(docs, batch_size=1000):
    search_client = SearchClient(cognitive_search_settings['SEARCH_ENDPOINT'], EntityIndex.Meta.index_name,
                                 AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    count = 1
    # INFO: Trick to avoid the 104 exception
    # ("Connection broken: ConnectionResetError(104, 'Connection reset by peer')",
    # ConnectionResetError(104, 'Connection reset by peer'))
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }  # noqa

    failed_data_chunks = []
    for data_chunk in divide_chunks(docs, batch_size=batch_size):
        uploaded = upload_docs(search_client, headers, data_chunk, failed_data_chunks, count, retry_no=1)
        if not uploaded:
            logger.error('Failed to upload the docs even after 3 retries. Please check error file for more details.')
            failed_data_chunks.append(data_chunk)

        time.sleep(1.0)
        count += 1


class Command(BaseCommand):
    help = 'Completely rebuilds the search index by removing the old data and then updating.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-entity_name', dest='entity_name', default='school', help='Provide the name of the entity.',
        )

        parser.add_argument(
            '--delete_index', action='store_true', dest='delete_index', default=False,
            help='If provided, already created cognitive index will be deleted from configured endpoint.'
        )

        parser.add_argument(
            '--create_index', action='store_true', dest='create_index', default=False,
            help='If provided, Cognitive index will be created at configured endpoint.'
        )

        parser.add_argument(
            '--clean_index', action='store_true', dest='clean_index', default=False,
            help='If provided, already created cognitive index data will be wiped out.'
        )

        parser.add_argument(
            '--update_index', action='store_true', dest='update_index', default=False,
            help='If provided, already created cognitive index data will be uploaded again.'
        )

        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Pass the Country ID in case want to control the update.'
        )

        parser.add_argument(
            '-entity_id', dest='entity_id', required=False, type=int,
            help='Pass the Entity ID in case want to control the update.'
        )

    def handle(self, **options):
        logger.info('build_cognitive_index utility STARTED with args: {0}'.format(options))

        if settings.ENABLE_AZURE_COGNITIVE_SEARCH:
            if options.get('delete_index', False):
                logger.info('Delete index - Start')
                delete_index()

            if options.get('create_index', False):
                logger.info('Create index - Start')
                create_index()

            if options.get('clean_index', False):
                logger.info('Clear index - Start')
                clear_index()

            if options.get('update_index', False):
                logger.info('Collect index data - Start')
                country_id = options.get('country_id', None)
                entity_name = options.get('entity_name', None)
                all_countries = [country_id, ]

                if entity_name == 'school':
                    if not country_id:
                        all_countries = list(
                            School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
                                'country_id'))

                    school_id = options.get('entity_id', None)
                    for c_id in all_countries:
                        data_to_load = collect_data(c_id, school_id)

                        if len(data_to_load) > 0:
                            logger.info('Load index - Start - {0}'.format(country_id))
                            # load_index(data_to_load, batch_size=10000)
                else:
                    if not country_id:
                        all_countries = list(
                            Entity.objects.filter(entity_name=entity_name).values_list(
                                'country_id', flat=True).order_by('country_id').distinct('country_id')
                        )

                    entity_id = options.get('entity_id', None)
                    for country_id in all_countries:
                        data_to_load = collect_data_for_entities(country_id, entity_id, entity_name)

                        if len(data_to_load) > 0:
                            logger.info('Load index - Start - {0}'.format(country_id))
                            # load_index(data_to_load, batch_size=10000)

        logger.info('build_cognitive_index utility ENDED for : {0}'.format(EntityIndex.Meta.index_name))
