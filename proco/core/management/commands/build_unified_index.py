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
from django.db.models import F, Case, IntegerField, Value, When, FilteredRelation, Q

from proco.core.utils import is_blank_string
from proco.entities.constants import LEGACY_MODEL
from proco.entities.models import Entity
from proco.locations.search_indexes import UnifiedEntityIndex
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
        all_docs = search_client.search(search_text='*', select=['unified_id'])
        ids = [doc['unified_id'] for doc in all_docs if doc.get('unified_id')]
        logger.info('Total docs to delete: {0}'.format(len(ids)))

        for data_chunk in divide_chunks(ids, batch_size=1000):
            search_client.delete_documents(documents=[{'unified_id': doc_id} for doc_id in data_chunk])


def collect_unified_data(country_id=None, school_id=None, entity_id=None, batch_size=1000):
    docs = []
    total_count = 0
    logger.info("Collect unified data STARTED")

    include_schools = entity_id is None
    include_entities = school_id is None

    # =====================
    # SCHOOL DOCUMENTS
    # =====================
    if include_schools:
        schools = School.objects.annotate(
            srr=FilteredRelation(
                'realtime_registration_status',
                condition=Q(realtime_registration_status__rt_registered=True)
                          & Q(realtime_registration_status__deleted__isnull=True),
            )
        ).annotate(
            country_name=F('country__name'),
            country_code=F('country__code'),
            admin1_name=F('admin1__name'),
            admin2_name=F('admin2__name'),
            row_score=Case(When(srr__id__isnull=True, then=Value(0)), default=1, output_field=IntegerField())
        ).values(
            'id',
            'name',
            'giga_id_school',
            'external_id',
            'admin1_id',
            'admin1_name',
            'admin2_id',
            'admin2_name',
            'country_id',
            'country_name',
            'country_code',
            'row_score',
        ).order_by('id').distinct()

        if country_id:
            schools = schools.filter(country_id=country_id)
        if school_id:
            schools = schools.filter(id=school_id)

        school_count = schools.count()
        logger.info(f"Schools queryset count: {school_count}")

        for i, school in enumerate(schools.iterator(chunk_size=batch_size)):
            doc = {
                "unified_id": f"school-{school['id']}",
                "entity_type_code": LEGACY_MODEL,
                "id": school['id'],
                "name": school['name'],
                "giga_id": school['giga_id_school'],
                "external_id": school['external_id'],
                "admin1_id": school['admin1_id'],
                "admin1_name": school['admin1_name'] or "Unknown",
                "admin2_id": school['admin2_id'],
                "admin2_name": school['admin2_name'],
                "country_id": school['country_id'],
                "country_name": school['country_name'],
                "country_code": school['country_code'],
                "row_score": school['row_score'],
            }
            if is_blank_string(doc['admin2_name']):
                del doc['admin2_name']
                del doc['admin2_id']
            docs.append(doc)
            total_count += 1

            if i < 3:
                logger.info(f"Sample school doc: {doc}")

            if len(docs) >= batch_size:
                yield docs
                docs = []

    # =====================
    # ENTITY DOCUMENTS
    # =====================
    if include_entities:
        entities = Entity.objects.annotate(
            entity_type_code=F('entity_type__code'),
            country_name=F('country__name'),
            country_code=F('country__code'),
            admin1_name=F('admin1__name'),
            admin2_name=F('admin2__name'),
            row_score=Value(1, output_field=IntegerField())
        ).values(
            'id',
            'entity_type_code',
            'name',
            'giga_id',
            'external_id',
            'admin1_id',
            'admin1_name',
            'admin2_id',
            'admin2_name',
            'country_id',
            'country_name',
            'country_code',
            'row_score',
        ).order_by('id')

        if country_id:
            entities = entities.filter(country_id=country_id)
        if entity_id:
            entities = entities.filter(id=entity_id)

        entity_count = entities.count()
        logger.info(f"Entities queryset count: {entity_count}")
        for i, entity in enumerate(entities.iterator(chunk_size=batch_size)):
            doc = {
                "unified_id": f"entity-{entity['id']}",
                "entity_type_code": entity['entity_type_code'],
                "id": entity['id'],
                "name": entity['name'],
                "giga_id": entity['giga_id'],
                "external_id": entity['external_id'],
                "admin1_id": entity['admin1_id'],
                "admin1_name": entity['admin1_name'] or "Unknown",
                "admin2_id": entity['admin2_id'],
                "admin2_name": entity['admin2_name'],
                "country_id": entity['country_id'],
                "country_name": entity['country_name'],
                "country_code": entity['country_code'],
                "row_score": entity['row_score'],
            }
            if is_blank_string(doc['admin2_name']):
                del doc['admin2_name']
                del doc['admin2_id']
            docs.append(doc)
            total_count += 1
            if i < 3:
                logger.info(f"Sample entity doc: {doc}")

            if len(docs) >= batch_size:
                yield docs
                docs = []

    if len(docs) > 0:
        yield docs

    logger.info(f"Total unified docs: {total_count}")


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


def load_index_batches(docs_batches):
    search_client = SearchClient(cognitive_search_settings['SEARCH_ENDPOINT'], UnifiedEntityIndex.Meta.index_name,
                                 AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }  # noqa

    failed_data_chunks = []
    for count, data_chunk in enumerate(docs_batches, start=1):
        uploaded = upload_docs(search_client, headers, data_chunk, failed_data_chunks, count, retry_no=1)
        if not uploaded:
            logger.error('Failed to upload the docs even after 3 retries. Please check error file for more details.')
            failed_data_chunks.append(data_chunk)

        time.sleep(1.0)


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

        parser.add_argument(
            '-entity_id', dest='entity_id', required=False, type=int,
            help='Pass the Entity ID in case want to control the update.'
        )

    def handle(self, **options):
        logger.info('Index operations STARTED ({0})'.format(UnifiedEntityIndex.Meta.index_name))
        if settings.ENABLE_AZURE_COGNITIVE_SEARCH:
            country_id = options.get('country_id', None)
            school_id = options.get('school_id', None)
            entity_id = options.get('entity_id', None)

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
                if country_id or school_id or entity_id:
                    logger.info('Load index - Start - country_id={0}, school_id={1}, entity_id={2}'.format(
                        country_id, school_id, entity_id))
                    load_index_batches(collect_unified_data(
                        country_id=country_id,
                        school_id=school_id,
                        entity_id=entity_id,
                        batch_size=1000,
                    ))
                else:
                    school_country_ids = set(
                        School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
                            'country_id'))
                    entity_country_ids = set(
                        Entity.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
                            'country_id'))
                    all_countries = sorted(school_country_ids | entity_country_ids)
                    for country_id in all_countries:
                        logger.info('Load index - Start - {0}'.format(country_id))
                        load_index_batches(collect_unified_data(country_id=country_id, batch_size=1000))

        logger.info('Index operations ENDED ({0})'.format(UnifiedEntityIndex.Meta.index_name))
