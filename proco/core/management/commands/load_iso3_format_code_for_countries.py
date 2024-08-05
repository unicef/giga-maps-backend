import os
import sys
import logging

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from proco.core.utils import is_blank_string
from proco.locations.models import Country

logger = logging.getLogger('gigamaps.' + __name__)


def is_file(fp):
    if not os.path.isfile(fp):
        raise CommandError("Provide a valid and an absolute file path, found '{0}'.".format(fp))
    return True


def load_data(input_file):
    is_file(input_file)

    country_data = pd.read_csv(input_file, delimiter=',', keep_default_na=False)

    for _, row in country_data.iterrows():
        try:
            country_code = row['code']
            country_iso3_code = row['iso3_format']
            if not is_blank_string(country_code) and not is_blank_string(country_iso3_code):
                Country.objects.filter(code=str(country_code).strip()).update(
                    iso3_format=str(country_iso3_code).strip(), )
            else:
                logger.error('Invalid country code ISO3 format submitted.')
                logger.error(country_code, '\t', country_iso3_code)
        except Exception as ex:
            logger.error('Error raised for creation: {0}'.format(ex))


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-cf',
            '--country-file',
            type=str,
            dest='country_file',
            help='Path to Country Mapping file.',
        )

    def handle(self, **options):
        input_file = options.get('country_file')
        if not input_file:
            sys.exit("Mandatory argument 'country-file' is missing.")

        with transaction.atomic():
            sys.stdout.write('\nLoading Country data from CSV....')
            load_data(input_file)
            sys.stdout.write('\nData loaded successfully!\n')
