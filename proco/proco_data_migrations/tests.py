import importlib

from django.apps import apps
from django.test import TestCase

from proco.accounts.models import DataSource
from proco.core.management.commands.load_system_data_layers import data_source_json

data_migration = importlib.import_module(
    'proco.proco_data_migrations.migrations.0006_add_daily_speed_columns_to_live_data_sources')

NEW_COLUMNS = ['connectivity_speed_p90', 'connectivity_speed_median']


class AddDailySpeedColumnsMigrationTestCase(TestCase):
    databases = ['default']

    def setUp(self):
        self.app_source = DataSource.objects.create(
            name='Daily Check APP and MLab', data_source_type='DAILY_CHECK_APP',
            column_config=[{'name': 'connectivity_speed'}, {'name': 'uptime'}],
        )
        self.qos_source = DataSource.objects.create(
            name='QoS', data_source_type='QOS',
            column_config=[{'name': 'connectivity_speed'}, {'name': 'connectivity_speed_p90'}],
        )
        self.school_master_source = DataSource.objects.create(
            name='School Master', data_source_type='SCHOOL_MASTER', column_config=[{'name': 'school_id_giga'}],
        )

    def column_names(self, data_source):
        data_source.refresh_from_db()
        return [column['name'] for column in data_source.column_config]

    def test_adds_missing_columns_after_connectivity_speed_once(self):
        data_migration.add_daily_speed_columns(apps, None)
        data_migration.add_daily_speed_columns(apps, None)

        self.assertEqual(self.column_names(self.app_source), ['connectivity_speed'] + NEW_COLUMNS + ['uptime'])
        self.assertEqual(self.column_names(self.qos_source), ['connectivity_speed'] + NEW_COLUMNS)
        self.assertEqual(self.column_names(self.school_master_source), ['school_id_giga'])

    def test_reverse_removes_only_the_new_columns(self):
        data_migration.add_daily_speed_columns(apps, None)
        data_migration.remove_daily_speed_columns(apps, None)

        self.assertEqual(self.column_names(self.app_source), ['connectivity_speed', 'uptime'])
        self.assertEqual(self.column_names(self.qos_source), ['connectivity_speed'])

    def test_frozen_columns_match_load_system_data_layers(self):
        for data_source in data_source_json:
            if data_source['data_source_type'] not in data_migration.LIVE_DATA_SOURCE_TYPES:
                continue
            command_columns = [column for column in data_source['column_config'] if column['name'] in NEW_COLUMNS]
            self.assertEqual(command_columns, data_migration.DAILY_SPEED_COLUMNS)
