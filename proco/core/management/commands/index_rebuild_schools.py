# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import time

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import CorsOptions, SearchIndex
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Prefetch, F

from proco.core.utils import is_blank_string
from proco.locations.models import Country
from proco.locations.search_indexes import SchoolIndex
from proco.schools.models import School

# Create a service client
cognitive_search_settings = settings.AZURE_CONFIG.get('COGNITIVE_SEARCH')


def delete_index():
    # Create a service client
    admin_client = SearchIndexClient(cognitive_search_settings['SEARCH_ENDPOINT'],
                                     AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    try:
        result = admin_client.delete_index(SchoolIndex.Meta.index_name)
        print('Index', SchoolIndex.Meta.index_name, 'Deleted')
        print(result)
    except Exception as ex:
        print(ex)


def create_index():
    # Create a service client
    admin_client = SearchIndexClient(cognitive_search_settings['SEARCH_ENDPOINT'],
                                     AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    # Create the index
    fields = [
        getattr(SchoolIndex, attr)
        for attr in dir(SchoolIndex)
        if not callable(getattr(SchoolIndex, attr)) and not attr.startswith("__")
    ]

    cors_options = CorsOptions(allowed_origins=['*'], max_age_in_seconds=24 * 60 * 60)
    scoring_profiles = []

    print('Index name: ', SchoolIndex.Meta.index_name)

    index = SearchIndex(
        name=SchoolIndex.Meta.index_name,
        fields=fields,
        scoring_profiles=scoring_profiles,
        cors_options=cors_options
    )

    try:
        result = admin_client.create_index(index)
        print('Index', result.name, 'created')
    except Exception as ex:
        print(ex)


def clear_index():
    search_client = SearchClient(cognitive_search_settings['SEARCH_ENDPOINT'], SchoolIndex.Meta.index_name,
                                 AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY']))

    doc_counts = search_client.get_document_count()
    print("There are {0} documents in the {1} search index.".format(doc_counts, repr(SchoolIndex.Meta.index_name)))

    if doc_counts > 0:
        all_docs = search_client.search('*')
        print('All documents: {0}'.format(all_docs))

        search_client.delete_documents(all_docs)


def collect_data(country_id):
    qry_fields = [
        attr
        for attr in dir(SchoolIndex)
        if not callable(getattr(SchoolIndex, attr)) and not attr.startswith("__")
    ]

    queryset = SchoolIndex.Meta.model.objects.all()

    qs = queryset.prefetch_related(
        Prefetch('country',
                 Country.objects.defer('geometry', 'geometry_simplified')),
    ).annotate(
        school_id=F('id'),
        country_name=F('country__name'),
        country_code=F('country__code'),
        admin1_name=F('admin1__name'),
        admin2_name=F('admin2__name'),
    ).values(*qry_fields).order_by(*SchoolIndex.Meta.ordering).distinct(*qry_fields)

    if country_id:
        qs = qs.filter(country_id=country_id)

    docs = []
    for qry_data in qs:
        qry_data['school_id'] = str(qry_data['school_id'])
        if is_blank_string(qry_data['admin1_name']):
            qry_data['admin1_name'] = 'Unknown'
            del qry_data['admin1_id']
        if is_blank_string(qry_data['admin2_name']):
            del qry_data['admin2_name']
            del qry_data['admin2_id']
        docs.append(qry_data)

    print('Total records to upload: {0}'.format(len(docs)))
    # docs = docs[0:100000]
    # print('Total records to upload: {0}'.format(len(docs)))
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
            print("Upload of new document SUCCEEDED for count '{0}' in retry no: '{1}': {2}".format(
                count, retry_no, result[0].succeeded)
            )
            uploaded = True
            break
        except Exception as ex:
            print("Upload of new document FAILED for count '{0}' in retry no: '{1}': {2}".format(count, retry_no, ex))
            time.sleep(1.0)
            retry_no += 1
            uploaded = upload_docs(search_client, headers, data_chunk, failed_data_chunks, count, retry_no=retry_no)
    return uploaded


def load_index(docs, batch_size=1000):
    search_client = SearchClient(cognitive_search_settings['SEARCH_ENDPOINT'], SchoolIndex.Meta.index_name,
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
            print('ERROR: Failed to upload the docs even after 3 retries. Please check error file for more details.')
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

    def handle(self, **options):
        print('*** Index operations STARTED ({0}) ***'.format(SchoolIndex.Meta.index_name))
        if settings.ENABLE_AZURE_COGNITIVE_SEARCH:
            country_id = options.get('country_id', False)

            if options.get('delete_index', False):
                print('DELETE_INDEX - START')
                delete_index()

            if options.get('create_index', False):
                print('CREATE_INDEX - START')
                create_index()

            if options.get('clean_index', False):
                print('CLEAR_INDEX - START')
                clear_index()

            if options.get('update_index', False):
                print('COLLECT_INDEX_DATA - START')
                if country_id:
                    data_to_load = collect_data(country_id)

                    if len(data_to_load) > 0:
                        print('LOAD_INDEX - START - {0}'.format(country_id))
                        load_index(data_to_load, batch_size=10000)
                else:
                    all_countries = list(
                        School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
                            'country_id'))
                    for country_id in all_countries:
                        data_to_load = collect_data(country_id)

                        if len(data_to_load) > 0:
                            print('LOAD_INDEX - START - {0}'.format(country_id))
                            load_index(data_to_load, batch_size=10000)

        print('*** Index operations ENDED ({0}) ***'.format(SchoolIndex.Meta.index_name))
