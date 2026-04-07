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
from django.db.models import Prefetch, F, Case, IntegerField, Value, When, FilteredRelation, Q

from proco.core.utils import is_blank_string
from proco.entities.models import Entity
from proco.locations.models import Country
from proco.locations.search_indexes import SchoolIndex, UnifiedEntityIndex
from proco.schools.models import School

logger = logging.getLogger('gigamaps.' + __name__)

# Create a service client
cognitive_search_settings = settings.AZURE_CONFIG.get('COGNITIVE_SEARCH')


def delete_index():
    # Create a service client
    admin_client = SearchIndexClient(cognitive_search_settings['SEARCH_ENDPOINT'],
                                     AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    try:
        result = admin_client.delete_index(UnifiedEntityIndex.Meta.index_name)
        logger.info('Index "{0}" deleted successfully'.format(UnifiedEntityIndex.Meta.index_name))
        logger.info(result)
    except Exception as ex:
        logger.error(ex)


def create_index():
    # Create a service client
    admin_client = SearchIndexClient(cognitive_search_settings['SEARCH_ENDPOINT'],
                                     AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    # Create the index
    fields = [
        getattr(UnifiedEntityIndex, attr)
        for attr in dir(UnifiedEntityIndex)
        if not callable(getattr(UnifiedEntityIndex, attr)) and not attr.startswith("__")
    ]

    cors_options = CorsOptions(allowed_origins=['*'], max_age_in_seconds=24 * 60 * 60)
    scoring_profiles = []

    logger.info('Index name: {0}'.format(UnifiedEntityIndex.Meta.index_name))

    index = SearchIndex(
        name=UnifiedEntityIndex.Meta.index_name,
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
    search_client = SearchClient(cognitive_search_settings['SEARCH_ENDPOINT'], UnifiedEntityIndex.Meta.index_name,
                                 AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    doc_counts = search_client.get_document_count()
    logger.info("There are {0} documents in the {1} search index.".format(
        doc_counts, repr(UnifiedEntityIndex.Meta.index_name)))

    if doc_counts > 0:
        all_docs = search_client.search('*')
        logger.info('All documents: {0}'.format(all_docs))

        search_client.delete_documents(all_docs)


def collect_unified_data(country_id=None):

    docs = []
    logger.info("Collect unified data STARTED")

    # =====================
    # SCHOOL DOCUMENTS
    # =====================
    schools = School.objects.select_related(
        'country', 'admin1', 'admin2'
    )

    if country_id:
        schools = schools.filter(country_id=country_id)
    logger.info(f"Schools queryset count: {schools.count()}")

    for i, s in enumerate(schools.iterator(chunk_size=1000)):
        doc = {
            "unified_id": f"school-{s.id}",
            "entity_type_code": "school",
            "id": s.id,
            "name": s.name,
            "country_id": s.country_id,
            "country_name": s.country.name,
            "admin1_name": getattr(s.admin1, "name", "Unknown"),
            "admin2_name": getattr(s.admin2, "name", None),
            "row_score": 1,
        }
        docs.append(doc)

        if i < 3:
            logger.info(f"Sample school doc: {doc}")
    # =====================
    # ENTITY DOCUMENTS
    # =====================
    entities = Entity.objects.select_related(
        'country', 'admin1', 'admin2', 'entity_type'
    )

    if country_id:
        entities = entities.filter(country_id=country_id)
    logger.info(f"Entities queryset count: {entities.count()}")
    for i, e in enumerate(entities.iterator(chunk_size=1000)):
        doc = {
            "unified_id": f"entity-{e.id}",
            "entity_type_code": e.entity_type.code,
            "id": e.id,
            "name": e.name,
            "country_id": e.country_id,
            "country_name": e.country.name,
            "admin1_name": getattr(e.admin1, "name", "Unknown"),
            "admin2_name": getattr(e.admin2, "name", None),
            "row_score": 1,
        }
        docs.append(doc)
        if i < 3:
            logger.info(f"Sample entity doc: {doc}")

    logger.info(f"Total unified docs: {len(docs)}")

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
    search_client = SearchClient(cognitive_search_settings['SEARCH_ENDPOINT'], UnifiedEntityIndex.Meta.index_name,
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
            '-school_id', dest='school_id', required=False, type=int,
            help='Pass the School ID in case want to control the update.'
        )

    def handle(self, **options):
        logger.info('Index operations STARTED ({0})'.format(UnifiedEntityIndex.Meta.index_name))
        if settings.ENABLE_AZURE_COGNITIVE_SEARCH:
            country_id = options.get('country_id', None)
            school_id = options.get('school_id', None)

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
                if country_id or school_id:
                    data_to_load = collect_unified_data(country_id)

                    if len(data_to_load) > 0:
                        logger.info('Load index - Start - {0}'.format(country_id))
                        load_index(data_to_load, batch_size=10000)
                else:
                    all_countries = list(
                        School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
                            'country_id'))
                    for country_id in all_countries:
                        data_to_load = collect_unified_data(country_id)

                        if len(data_to_load) > 0:
                            logger.info('Load index - Start - {0}'.format(country_id))
                            load_index(data_to_load, batch_size=10000)

        logger.info('Index operations ENDED ({0})'.format(UnifiedEntityIndex.Meta.index_name))
