import os
import shutil
import tempfile
from io import StringIO
from unittest import mock

from django.core.cache import cache
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.urls import reverse

import requests

from proco.core.management.commands import load_country_data_source_descriptions as loader
from proco.locations.models import Country
from proco.locations.tests.factories import CountryFactory
from proco.utils.cache import cache_manager

HEADER = 'code,name,data_source_description,health_data_source_description\n'


@mock.patch.object(loader, 'update_cached_value')
class LoadCountryDataSourceDescriptionsTestCase(TestCase):
    databases = ['default']

    @classmethod
    def setUpTestData(cls):
        cls.argentina = CountryFactory(
            code='AR', name='Argentina', data_source_description='', health_data_source_description='')
        cls.kenya = CountryFactory(
            code='KE', name='Kenya', data_source_description='Manual text', health_data_source_description='')

    def setUp(self):
        cache.clear()
        self.tmp_dir = tempfile.mkdtemp()
        super().setUp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        super().tearDown()

    def _write_csv(self, content, prefix=''):
        path = os.path.join(self.tmp_dir, 'descriptions.csv')
        with open(path, 'w', encoding='utf-8') as csv_file:
            csv_file.write(prefix + content)
        return path

    def _run(self, content, *args):
        out, err = StringIO(), StringIO()
        call_command(
            'load_country_data_source_descriptions', '--country-file', self._write_csv(content), *args,
            stdout=out, stderr=err,
        )
        return out.getvalue(), err.getvalue()

    def _descriptions(self, country):
        country = Country.objects.get(id=country.id)
        return country.data_source_description, country.health_data_source_description

    def test_parse_rows_strips_values_ignores_extra_columns_and_bom(self, _):
        csv_text = loader.read_csv_text(file_path=self._write_csv(
            ' code ,name,data_source_description,health_data_source_description\n'
            ' ar ,Argentina,  Text with [link](https://example.org)  ,\n',
            prefix='﻿',
        ))

        rows = loader.parse_rows(csv_text)

        self.assertEqual(rows, [(2, {
            'code': 'ar',
            'data_source_description': 'Text with [link](https://example.org)',
            'health_data_source_description': '',
        })])

    def test_missing_column_raises(self, _):
        with self.assertRaises(CommandError):
            loader.parse_rows('code,data_source_description\nAR,Text\n')

    def test_placeholder_is_replaced_by_country_name(self, _):
        self._run(HEADER + 'AR,,Schools of {country} from the Ministry,Facilities in {country}\n')

        self.assertEqual(
            self._descriptions(self.argentina),
            ('Schools of Argentina from the Ministry', 'Facilities in Argentina'),
        )

    def test_blank_cell_leaves_field_untouched(self, _):
        self._run(HEADER + 'KE,,  ,Health text\n')

        self.assertEqual(self._descriptions(self.kenya), ('Manual text', 'Health text'))

    def test_non_blank_cell_overwrites_existing_text_by_default(self, _):
        self._run(HEADER + 'KE,,New text,\n')

        self.assertEqual(self._descriptions(self.kenya)[0], 'New text')

    def test_only_empty_keeps_existing_text(self, _):
        out, _err = self._run(HEADER + 'KE,,New text,Health text\n', '--only-empty')

        self.assertEqual(self._descriptions(self.kenya), ('Manual text', 'Health text'))
        self.assertIn('KE data_source_description: already has a value, kept', out)

    def test_length_limit_is_checked_after_substitution(self, _):
        # 994 characters + '{country}' fits before substitution; with 'Argentina' (9 chars) it is 1003
        too_long = 'a' * 994 + '{country}'
        exact = 'b' * 1000

        out, err = self._run(HEADER + 'AR,,{0},ok\nKE,,{1},\n'.format(too_long, exact))

        self.assertEqual(self._descriptions(self.argentina), ('', 'ok'))
        self.assertEqual(self._descriptions(self.kenya)[0], exact)
        self.assertIn('AR data_source_description has 1003 characters after substitution', err)

    def test_unknown_repeated_and_empty_codes_are_reported_without_aborting(self, _):
        out, err = self._run(HEADER + 'XX,,Text,\n,,Text,\nar,,First,\nAR,,Second,\n')

        self.assertEqual(self._descriptions(self.argentina)[0], 'First')
        self.assertIn("line 2: unknown country code 'XX'", err)
        self.assertIn('line 3: empty code', err)
        self.assertIn("line 5: code 'AR' is repeated", err)
        self.assertIn('countries to update: 1, fields: 1, kept: 0, skipped: 3', out)

    def test_dry_run_writes_nothing_and_touches_no_cache(self, update_cached_value):
        cache_manager.set('V2_ENTITY_COUNTRY_DETAIL_pk_ar', {'id': 1}, request_path='/api/v2/x/')

        out, _err = self._run(HEADER + 'AR,,New {country},\n', '--dry-run')

        self.assertEqual(self._descriptions(self.argentina), ('', ''))
        self.assertIn("[DRY RUN] AR Argentina\n  data_source_description\n    old: ''\n    new: 'New Argentina'", out)
        self.assertIn('Nothing written.', out)
        self.assertFalse(cache.get('SOFT_CACHE_V2_ENTITY_COUNTRY_DETAIL_pk_ar')['invalidated'])
        update_cached_value.delay.assert_not_called()

    def test_write_invalidates_cache_and_queues_rebuild_for_changed_countries(self, update_cached_value):
        cache_manager.set('V2_ENTITY_COUNTRY_DETAIL_pk_ar', {'id': 1}, request_path='/api/v2/x/')
        cache_manager.set('COUNTRIES_LIST_', [], request_path='/api/locations/countries/')
        cache_manager.set('GLOBAL_STATS_', {}, request_path='/api/statistics/global-stat/')

        # KE is in the CSV with the text it already has: no change, so no rebuild for it
        self._run(HEADER + 'AR,,New text,\nKE,,Manual text,\n')

        self.assertTrue(cache.get('SOFT_CACHE_V2_ENTITY_COUNTRY_DETAIL_pk_ar')['invalidated'])
        self.assertTrue(cache.get('SOFT_CACHE_COUNTRIES_LIST_')['invalidated'])
        self.assertFalse(cache.get('SOFT_CACHE_GLOBAL_STATS_')['invalidated'])
        queued_urls = [call.kwargs['url'] for call in update_cached_value.delay.call_args_list]
        self.assertCountEqual(queued_urls, [
            reverse('locations:countries-detail', kwargs={'pk': 'ar'}),
            reverse('entities:retrieve-entity-country', kwargs={'pk': 'ar'}),
            reverse('locations:countries-list'),
            reverse('entities:list-entity-countries'),
        ])

    def test_no_changes_queues_nothing(self, update_cached_value):
        out, _err = self._run(HEADER + 'KE,,Manual text,\n')

        self.assertIn('countries to update: 0', out)
        update_cached_value.delay.assert_not_called()

    def test_reads_csv_from_https_url(self, _):
        response = mock.Mock(content=(HEADER + 'AR,,From URL,\n').encode('utf-8'))
        with mock.patch.object(loader.requests, 'get', return_value=response) as get:
            call_command(
                'load_country_data_source_descriptions', '--url', 'https://example.org/d.csv', stdout=StringIO(),
            )

        get.assert_called_once_with('https://example.org/d.csv', timeout=60)
        self.assertEqual(self._descriptions(self.argentina)[0], 'From URL')

    def test_url_must_be_https_and_download_errors_raise(self, _):
        with self.assertRaises(CommandError):
            loader.read_csv_text(url='http://example.org/d.csv')

        with mock.patch.object(loader.requests, 'get', side_effect=requests.ConnectionError('boom')):
            with self.assertRaises(CommandError):
                loader.read_csv_text(url='https://example.org/d.csv')
