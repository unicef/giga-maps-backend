from django.core.exceptions import ImproperlyConfigured

from config.settings.base import *  # noqa: F403
from kombu import Exchange, Queue  # NOQA

DEBUG = True
TEMPLATES[0]['OPTIONS']['debug'] = DEBUG

SECRET_KEY = env('SECRET_KEY', default='test_key')

ALLOWED_HOSTS = ['*']
INTERNAL_IPS = ['127.0.0.1']

ADMINS = (
    ('Dev Email', env('DEV_ADMIN_EMAIL', default='admin@localhost')),
)
MANAGERS = ADMINS


# Database
# https://docs.djangoproject.com/en/1.9/ref/settings/#databases
# --------------------------------------------------------------------------

DATABASES = {
    'default': env.db(default='postgis://localhost/proco'),
    'read_only_database': env.db(default='postgis://localhost/proco'),
}

try:
    DATABASES['read_only_database'] = env.db_url(var='READ_ONLY_DATABASE_URL')
except ImproperlyConfigured:
    pass

# Email settings
# --------------------------------------------------------------------------

# DEFAULT_FROM_EMAIL = 'noreply@example.com'
# SERVER_EMAIL = DEFAULT_FROM_EMAIL
# EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'

if CELERY_ENABLED:
    MAILING_USE_CELERY = False


# Debug toolbar installation
# --------------------------------------------------------------------------

INSTALLED_APPS += (
    'debug_toolbar',
)

MIDDLEWARE += [
    'debug_toolbar.middleware.DebugToolbarMiddleware',
]
INTERNAL_IPS = ('127.0.0.1',)


if CELERY_ENABLED:
    # Celery configurations
    # http://docs.celeryproject.org/en/latest/configuration.html
    # --------------------------------------------------------------------------

    CELERY_BROKER_URL = env('CELERY_BROKER_URL')
    CELERY_RESULT_BACKEND = env('CELERY_RESULT_BACKEND_URL')

    if CELERY_BROKER_URL.startswith('rediss://') or CELERY_RESULT_BACKEND.startswith('rediss://'):
        import ssl

        CELERY_REDIS_BACKEND_USE_SSL = {
            'ssl_cert_reqs': ssl.CERT_NONE,
        }

    CELERY_TASK_DEFAULT_QUEUE = 'proco-celery-queue'
    CELERY_TASK_DEFAULT_EXCHANGE = 'proco-exchange'
    CELERY_TASK_DEFAULT_ROUTING_KEY = 'celery.proco'
    CELERY_TASK_QUEUES = (
        Queue(
            CELERY_TASK_DEFAULT_QUEUE,
            Exchange(CELERY_TASK_DEFAULT_EXCHANGE),
            routing_key=CELERY_TASK_DEFAULT_ROUTING_KEY,
        ),
    )


# Sentry config
# -------------

SENTRY_ENABLED = False


# Mapbox
# --------------

MAPBOX_KEY = env('MAPBOX_KEY', default='')

ANYMAIL['DEBUG_API_REQUESTS'] = True
