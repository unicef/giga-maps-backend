from datetime import datetime, timedelta
from io import StringIO
from unittest.mock import patch

from django.core.cache import cache
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone

from proco.data_sources.models import QoSData
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory

GET_TABLE_VERSION = 'proco.data_sources.management.commands.seed_qos_data_version.delta_sharing.get_table_version'


def get_version_from_table_url(versions):
    def get_table_version(table_url):
        iso3 = table_url.rsplit('.', 1)[-1]
        if versions[iso3] is None:
            raise ValueError('table not found')
        return versions[iso3]
    return get_table_version


class SeedQoSDataVersionCommandTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.bra = CountryFactory(iso3_format='BRA')
        cls.ken = CountryFactory(iso3_format='KEN')
        cls.bra_school = SchoolFactory(country=cls.bra, admin1=None)
        cls.ken_school = SchoolFactory(country=cls.ken, admin1=None)

        start = timezone.make_aware(datetime(2026, 11, 16, 8, 0))
        for hour in range(3):
            ts = start + timedelta(hours=hour)
            QoSData.objects.create(school=cls.bra_school, country=cls.bra, timestamp=ts, date=ts.date(),
                                   school_id_giga=cls.bra_school.giga_id_school, version=4900 + hour)
            QoSData.objects.create(school=cls.ken_school, country=cls.ken, timestamp=ts, date=ts.date(),
                                   school_id_giga=cls.ken_school.giga_id_school, version=23857)

    def setUp(self):
        cache.clear()

    def run_command(self, *args):
        out, err = StringIO(), StringIO()
        call_command('seed_qos_data_version', *args, stdout=out, stderr=err)
        return out.getvalue(), err.getvalue()

    def test_dry_run_writes_nothing(self):
        with patch(GET_TABLE_VERSION, side_effect=get_version_from_table_url({'BRA': 700, 'KEN': 64})):
            out, _ = self.run_command()

        self.assertIn('DRY RUN', out)
        self.assertEqual(QoSData.get_last_version('BRA'), 4902)
        self.assertEqual(QoSData.get_last_version('KEN'), 23857)

    def test_apply_seeds_host_version_minus_margin_and_keeps_rows(self):
        cache.set(QoSData.DATA_VERSION_CACHE_KEY.format('BRA'), 4902)
        rows_before = QoSData.objects.count()

        with patch(GET_TABLE_VERSION, side_effect=get_version_from_table_url({'BRA': 700, 'KEN': 64})):
            out, _ = self.run_command('--apply')

        self.assertEqual(QoSData.objects.count(), rows_before)
        self.assertEqual(QoSData.get_last_version('BRA'), 698)
        self.assertEqual(QoSData.get_last_version('KEN'), 62)
        self.assertEqual(set(QoSData.objects.filter(country=self.bra).values_list('version', flat=True)), {698})
        self.assertIsNone(cache.get(QoSData.DATA_VERSION_CACHE_KEY.format('BRA')))
        self.assertNotIn('MISMATCH', out)

    def test_apply_only_given_country(self):
        with patch(GET_TABLE_VERSION, side_effect=get_version_from_table_url({'BRA': 700})):
            self.run_command('--country_code', 'bra', '--margin', '0', '--apply')

        self.assertEqual(QoSData.get_last_version('BRA'), 700)
        self.assertEqual(QoSData.get_last_version('KEN'), 23857)

    def test_set_version_skips_host_lookup(self):
        with patch(GET_TABLE_VERSION, side_effect=get_version_from_table_url({'KEN': 64})) as lookup:
            self.run_command('--set_version', 'BRA=650', '--apply')

        self.assertEqual(QoSData.get_last_version('BRA'), 650)
        self.assertEqual(QoSData.get_last_version('KEN'), 62)
        looked_up = [call.args[0].rsplit('.', 1)[-1] for call in lookup.call_args_list]
        self.assertEqual(looked_up, ['KEN'])

    def test_failed_lookup_leaves_country_unchanged(self):
        with patch(GET_TABLE_VERSION, side_effect=get_version_from_table_url({'BRA': None, 'KEN': 64})):
            _, err = self.run_command('--apply')

        self.assertIn('BRA: no target version, skipped', err)
        self.assertEqual(QoSData.get_last_version('BRA'), 4902)
        self.assertEqual(QoSData.get_last_version('KEN'), 62)

    def test_invalid_set_version_raises(self):
        with self.assertRaises(CommandError):
            self.run_command('--set_version', 'BRA:650')
