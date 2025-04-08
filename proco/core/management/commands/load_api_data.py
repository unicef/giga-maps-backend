import json
import os
import sys

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models.functions import Lower

from proco.accounts.models import API, APIKey
from proco.core.utils import get_current_datetime_object


def is_file(fp):
    if not os.path.isfile(fp):
        raise CommandError("Provide a valid and an absolute file path, found '{0}'.".format(fp))
    return True


def get_file_data(file_path):
    file_data = []
    is_file(file_path)
    with open(file_path, 'r') as tsv_file:
        all_lines = tsv_file.readlines()
        headers = [tok.strip() for tok in all_lines[0].split('\t')]
        for row in all_lines[1:]:
            row_data = [
                tok.strip()
                for tok in row.split('\t')
            ]
            file_data.append(dict(zip(headers, row_data)))
    return headers, file_data


def load_data(input_file):
    file_headers, api_data = get_file_data(input_file)

    api_codes = [row_data['code'].lower() for row_data in api_data]

    apis_to_delete = API.objects.annotate(code_lower=Lower('code')).exclude(code_lower__in=api_codes)

    if apis_to_delete.exists():
        sys.stdout.write('\nAPI listed for deletion: {}'.format(apis_to_delete))
        APIKey.objects.filter(api__in=apis_to_delete).update(deleted=get_current_datetime_object())
        API.objects.filter(pk__in=apis_to_delete).update(deleted=get_current_datetime_object())
        sys.stdout.write('\nDeleted APIs successfully!')

    for row_data in api_data:
        try:
            default_filters = row_data.get('default_filters', '{}').replace('""', '"')
            default_filters = default_filters[1:] if default_filters.startswith('"') else default_filters
            default_filters = default_filters[:-1] if default_filters.endswith('"') else default_filters
            row_data['default_filters'] = json.loads(default_filters)

            instance, created = API.objects.update_or_create(
                code=row_data['code'] or str(row_data['name'] + '_' + row_data['category']).upper(),
                defaults={
                    'name': row_data['name'],
                    'category': row_data['category'],
                    'description': row_data['description'],
                    'documentation_url': row_data['documentation_url'],
                    'download_url': row_data['download_url'],
                    'report_title': row_data['report_title'],
                    'default_filters': row_data['default_filters'],
                    'last_modified_at': get_current_datetime_object(),
                },
            )
            if created:
                sys.stdout.write('\nNew API created: {}'.format(instance.__dict__))
            else:
                sys.stdout.write('\nExisting API updated: {}'.format(instance.__dict__))
        except:
            pass


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-af',
            '--api-file',
            type=str,
            dest='api_file',
            help='Path to API file.',
        )

    def handle(self, **options):
        input_file = options.get('api_file')
        if not input_file:
            sys.exit("Mandatory argument 'api-file' is missing.")

        with transaction.atomic():
            sys.stdout.write('\nLoading APIs data....')
            load_data(input_file)
            sys.stdout.write('\nData loaded successfully!\n')
