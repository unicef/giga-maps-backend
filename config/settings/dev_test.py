from kombu import Exchange, Queue  # NOQA

from config.settings.base import *  # noqa: F403

# Pytest speed improvements configuration
# Disable debugging for test case execution
DEBUG = False
TEMPLATES[0]['OPTIONS']['debug'] = DEBUG

SECRET_KEY = env('SECRET_KEY', default='test_key')

ALLOWED_HOSTS = ['*']
INTERNAL_IPS = []

ADMINS = (
    ('Dev Email', env('DEV_ADMIN_EMAIL', default='admin@localhost')),
)
MANAGERS = ADMINS


# Database
# https://docs.djangoproject.com/en/1.9/ref/settings/#databases
# --------------------------------------------------------------------------

DATABASES = {
    'default': env.db(default='postgis://localhost/proco'),
    READ_ONLY_DB_KEY: env.db(var='READ_ONLY_DATABASE_URL', default=env.db(default='postgis://localhost/proco')),
}

DATABASES['default']['CONN_MAX_AGE'] = 1000

# Email settings
# --------------------------------------------------------------------------

# DEFAULT_FROM_EMAIL = 'noreply@example.com'
# SERVER_EMAIL = DEFAULT_FROM_EMAIL
# EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'

if CELERY_ENABLED:
    MAILING_USE_CELERY = False

INTERNAL_IPS = ('127.0.0.1',)

# Sentry config
# -------------

SENTRY_ENABLED = False


# Mapbox
# --------------

MAPBOX_KEY = env('MAPBOX_KEY', default='')

ANYMAIL['DEBUG_API_REQUESTS'] = False

ENABLE_AZURE_COGNITIVE_SEARCH = False

AZURE_CONFIG['COGNITIVE_SEARCH'] = {
    'SEARCH_ENDPOINT': env('SEARCH_ENDPOINT', default='test.endpoint'),
    'SEARCH_API_KEY': env('SEARCH_API_KEY', default='testsearchapikey'),
    'COUNTRY_INDEX_NAME': env('COUNTRY_INDEX_NAME', default='giga_countries'),
    'SCHOOL_INDEX_NAME': env('SCHOOL_INDEX_NAME', default='giga_schools'),
}
