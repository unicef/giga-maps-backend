import logging
import threading

from django.conf import settings
from django.urls import resolve
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger('gigamaps.' + __name__)

THREAD_LOCAL = threading.local()


def get_app_model_code(model):
    return "{0}.{1}".format(model._meta.app_label, model.__name__)


class CustomRequestDBRouterMiddleware(MiddlewareMixin):
    """
    CustomRequestDBRouterMiddleware
        This middleware is designed to intercept each request to route the DB request to
         read only database. Expected when its an export request or
         a GET request on allowed request names.
    """

    def process_view(self, request, view_func, args, kwargs):
        if request.method == 'GET':
            url_name = resolve(request.path_info).url_name
            is_read_db_request = (url_name in settings.READ_ONLY_DATABASE_ALLOWED_REQUESTS)

            if is_read_db_request:
                THREAD_LOCAL.OVERRIDE_DB_FOR_READ = settings.READ_ONLY_DB_KEY
                logger.info('Using Read-Only DB for request: {0}:{1}:{2} '.format(
                    request.path_info,
                    request.method,
                    request.content_type,
                ), )
            else:
                logger.info('CHECK:: Using Write DB for GET request: {0}:{1}:{2} '.format(
                    request.path_info,
                    request.method,
                    request.content_type,
                ), )

    def process_response(self, request, response):
        # Make the database to default here after serving to the request
        if hasattr(THREAD_LOCAL, 'OVERRIDE_DB_FOR_READ'):
            del THREAD_LOCAL.OVERRIDE_DB_FOR_READ
        return response



class ReadOnlyDBRouter(object):
    """
    A router to control all database operations on models in the
    reports application.
    """

    def _db_for_read_by_request(self, model):
        if (
            hasattr(THREAD_LOCAL, 'OVERRIDE_DB_FOR_READ') and
            THREAD_LOCAL.OVERRIDE_DB_FOR_READ in settings.DATABASES
        ):
            logger.info('Using Read-Only DB Key by request: {}'.format(
                THREAD_LOCAL.OVERRIDE_DB_FOR_READ)
            )
            return THREAD_LOCAL.OVERRIDE_DB_FOR_READ
        return None

    def db_for_read(self, model, **hints):
        """
        Attempts to read report models go to read database.
        """
        app_model_code = get_app_model_code(model)
        db_url = settings.DATABASES.get(settings.READ_ONLY_DB_KEY, None)
        if db_url and app_model_code in settings.READ_ONLY_DATABASE_ALLOWED_MODELS:
            read_db = settings.READ_ONLY_DB_KEY
            logger.info('Using Read-Only DB Key by model: {}'.format(read_db))
            return read_db
        return self._db_for_read_by_request(model)

    def allow_relation(self, obj1, obj2, **hints):
        """
        allow_relation
            Return true in case of interaction with replica of database
            Refer: https://docs.djangoproject.com/en/2.1/topics/db/multi-db/
        :param obj1:
        :param obj2:
        :param hints:
        :return:
        """
        return True
