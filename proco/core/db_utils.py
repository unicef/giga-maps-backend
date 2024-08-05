import logging

from django.db import connection

logger = logging.getLogger('gigamaps.' + __name__)


def dictfetchall(cursor):
    """
    Return all rows from a cursor as a dict.
    Assume the column names are unique.
    """
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def sql_to_response(sql, label=''):
    logger.debug('Query to execute for "{0}": {1}'.format(label, sql.replace('\n', '')))
    try:
        with connection.cursor() as cur:
            cur.execute(sql)
            if not cur:
                return
            return dictfetchall(cur)
    except Exception as ex:
        logger.error('Exception on query execution - {0}'.format(str(ex)))
        return
