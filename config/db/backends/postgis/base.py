import psycopg2.extensions
from django_prometheus.db.backends.postgis.base import DatabaseWrapper as PrometheusPostGISDatabaseWrapper
from django_prometheus.db.common import ExportingCursorWrapper


class DatabaseWrapper(PrometheusPostGISDatabaseWrapper):
    """Export PostGIS query metrics using the real Django database alias."""

    def get_connection_params(self):
        conn_params = super().get_connection_params()
        conn_params['cursor_factory'] = ExportingCursorWrapper(
            psycopg2.extensions.cursor, self.alias, self.vendor,
        )
        return conn_params
