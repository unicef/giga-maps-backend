from unittest import TestCase
from unittest.mock import patch

from django.conf import settings

if not settings.configured:
    settings.configure(INSTALLED_APPS=[])

import django

django.setup()

from django.contrib.gis.db.backends.postgis import base as django_postgis  # noqa: E402
from django.db.utils import load_backend  # noqa: E402

from prometheus_client import REGISTRY, generate_latest  # noqa: E402

from config.db.backends.postgis import base as project_postgis  # noqa: E402


class FakeCursor:
    def execute(self, query, *args, **kwargs):
        if query == 'raise-sensitive-query-marker':
            raise ValueError('expected test error')
        return query


class PrometheusPostGISBackendTestCase(TestCase):
    aliases = ('default', 'read_only_database')
    vendor = 'postgresql'

    def _metric_value(self, metric_name, labels):
        return REGISTRY.get_sample_value(metric_name, labels) or 0

    def _cursor_factory(self, alias):
        wrapper = project_postgis.DatabaseWrapper.__new__(
            project_postgis.DatabaseWrapper,
        )
        wrapper.alias = alias
        with patch.object(
            project_postgis.psycopg2.extensions,
            'cursor',
            FakeCursor,
        ), patch.object(
            django_postgis.DatabaseWrapper,
            'get_connection_params',
            return_value={},
        ):
            return wrapper.get_connection_params()['cursor_factory']

    def _database_wrapper(self, alias):
        wrapper = project_postgis.DatabaseWrapper.__new__(
            project_postgis.DatabaseWrapper,
        )
        wrapper.alias = alias
        return wrapper

    def test_backend_loads_without_additional_package_markers(self):
        backend = load_backend('config.db.backends.postgis')

        self.assertIs(backend.DatabaseWrapper, project_postgis.DatabaseWrapper)

    def test_cursor_factory_is_psycopg2_compatible(self):
        wrapper = self._database_wrapper('default')
        with patch.object(
            django_postgis.DatabaseWrapper,
            'get_connection_params',
            return_value={},
        ):
            cursor_factory = wrapper.get_connection_params()['cursor_factory']

        self.assertTrue(issubclass(
            cursor_factory,
            project_postgis.psycopg2.extensions.cursor,
        ))

    def test_query_metrics_keep_database_aliases_distinct(self):
        metric_names = (
            'django_db_execute_total',
            'django_db_query_duration_seconds_count',
        )
        expected_increments = {'default': 1, 'read_only_database': 2}
        before = {}
        for alias in self.aliases:
            labels = {'alias': alias, 'vendor': self.vendor}
            before[alias] = {
                name: self._metric_value(name, labels)
                for name in metric_names
            }

        self._cursor_factory('default')().execute(
            'select-default-sensitive-query-marker',
        )
        read_only_cursor = self._cursor_factory('read_only_database')()
        read_only_cursor.execute('select-read-only-sensitive-query-marker')
        read_only_cursor.execute('select-read-only-sensitive-query-marker')

        for alias, increment in expected_increments.items():
            labels = {'alias': alias, 'vendor': self.vendor}
            for metric_name in metric_names:
                self.assertEqual(
                    self._metric_value(metric_name, labels)
                    - before[alias][metric_name],
                    increment,
                )

    def test_execution_errors_keep_the_real_alias(self):
        metric_name = 'django_db_errors_total'
        for alias in self.aliases:
            labels = {
                'alias': alias,
                'vendor': self.vendor,
                'type': 'ValueError',
            }
            before = self._metric_value(metric_name, labels)

            with self.assertRaisesRegex(ValueError, 'expected test error'):
                self._cursor_factory(alias)().execute(
                    'raise-sensitive-query-marker',
                )

            self.assertEqual(
                self._metric_value(metric_name, labels) - before,
                1,
            )

    def test_connection_metrics_keep_the_real_alias(self):
        for alias in self.aliases:
            labels = {'alias': alias, 'vendor': self.vendor}
            wrapper = self._database_wrapper(alias)
            connections_before = self._metric_value(
                'django_db_new_connections_total', labels,
            )

            with patch.object(
                django_postgis.DatabaseWrapper,
                'get_new_connection',
                return_value=object(),
            ):
                wrapper.get_new_connection({})

            self.assertEqual(
                self._metric_value(
                    'django_db_new_connections_total', labels,
                ) - connections_before,
                1,
            )

            connections_before = self._metric_value(
                'django_db_new_connections_total', labels,
            )
            errors_before = self._metric_value(
                'django_db_new_connection_errors_total', labels,
            )
            with patch.object(
                django_postgis.DatabaseWrapper,
                'get_new_connection',
                side_effect=RuntimeError('expected connection error'),
            ), self.assertRaisesRegex(RuntimeError, 'expected connection error'):
                wrapper.get_new_connection({})

            self.assertEqual(
                self._metric_value(
                    'django_db_new_connections_total', labels,
                ) - connections_before,
                1,
            )
            self.assertEqual(
                self._metric_value(
                    'django_db_new_connection_errors_total', labels,
                ) - errors_before,
                1,
            )

    def test_sql_text_is_not_exported(self):
        self._cursor_factory('default')().execute(
            'select-secret-sensitive-query-marker',
        )

        payload = generate_latest(REGISTRY).decode('utf-8')
        self.assertNotIn('select-secret-sensitive-query-marker', payload)
