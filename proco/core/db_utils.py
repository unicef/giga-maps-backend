import logging

from django.db import connections
from django.db.models import Aggregate, FloatField

logger = logging.getLogger('gigamaps.' + __name__)


class Percentile(Aggregate):
    """
    Percentile
        Continuous percentile of the grouped values, as PostgreSQL PERCENTILE_CONT computes it
        (linear interpolation, NULL values ignored).

    Inherits: `Aggregate`

    :param expression: field name or expression to rank
    :param percentile: fraction between 0 and 1, for example 0.9 for P90
    """
    function = 'PERCENTILE_CONT'
    name = 'Percentile'
    template = '%(function)s(%(percentile)s) WITHIN GROUP (ORDER BY %(expressions)s)'

    def __init__(self, expression, percentile, **extra):
        percentile = float(percentile)
        if not 0 <= percentile <= 1:
            raise ValueError('percentile must be between 0 and 1')
        super().__init__(expression, percentile=percentile, output_field=FloatField(), **extra)


def dictfetchall(cursor):
    """
    Return all rows from a cursor as a dict.
    Assume the column names are unique.
    """
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def sql_to_response(sql, label='', db_var='default', raise_exception=False):
    logger.debug('Query to execute for "{0}": {1}'.format(label, sql.replace('\n', '')))

    with connections[db_var].cursor() as cur:
        try:
            cur.execute(sql)
            if not cur:
                return None
            return dictfetchall(cur)
        except Exception as ex:
            logger.error('Exception on query execution - {0}'.format(str(ex)))
            if raise_exception:
                raise
    return None
