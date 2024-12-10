import sys
import warnings

import environ
from django.utils import timezone
from rest_framework.settings import DEFAULTS as default_settings

# Build paths inside the project like this: root(...)
env = environ.Env()

# Read in our environment variables
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    env.read_env()

root = environ.Path(__file__) - 3
apps_root = root.path('proco')

BASE_DIR = root()

# Project name
# ------------
PROJECT_FULL_NAME = env('PROJECT_FULL_NAME', default='Project Connect')
PROJECT_SHORT_NAME = env('PROJECT_SHORT_NAME', default='Proco')

# Base configurations
# --------------------------------------------------------------------------

ROOT_URLCONF = 'config.urls'
WSGI_APPLICATION = 'config.wsgi.application'

AUTH_USER_MODEL = 'custom_auth.ApplicationUser'

ENABLED_BACKEND_PROMETHEUS_METRICS = env.bool('ENABLED_BACKEND_PROMETHEUS_METRICS', default=True)
# Application definition
# --------------------------------------------------------------------------

DJANGO_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.sites',
    'django.contrib.sitemaps',
    'django.contrib.gis',
]

THIRD_PARTY_APPS = [
    'rest_framework',
    'rest_framework_gis',
    # 'drf_secure_token',
    'mptt',
    'crispy_forms',
    'mapbox_location_field',
    'corsheaders',
    'admin_reorder',
    'django_filters',
    'django_mptt_admin',
    'constance',
    'unicef_restlib',
    'simple_history',
    'anymail',
    'django_prometheus',
]

LOCAL_APPS = [
    'proco.accounts',
    'proco.about_us',
    'proco.core',
    'proco.mailing',
    'proco.custom_auth',
    'proco.schools',
    'proco.locations',
    'proco.connection_statistics',
    'proco.contact',
    'proco.background',
    'proco.proco_data_migrations',
    'proco.data_sources',
]

INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

# Middleware configurations
# --------------------------------------------------------------------------

MIDDLEWARE = [
    'django.middleware.gzip.GZipMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    # 'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.contrib.sites.middleware.CurrentSiteMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    # 'drf_secure_token.middleware.UpdateTokenMiddleware',
    'admin_reorder.middleware.ModelAdminReorder',
    'proco.utils.middleware.CustomCorsMiddleware',
    'proco.utils.db_routers.CustomRequestDBRouterMiddleware',
]

if ENABLED_BACKEND_PROMETHEUS_METRICS:
    MIDDLEWARE.insert(0, 'django_prometheus.middleware.PrometheusBeforeMiddleware')
    MIDDLEWARE.insert(len(MIDDLEWARE), 'django_prometheus.middleware.PrometheusAfterMiddleware')

# Custom authentication backend
AUTHENTICATION_BACKENDS = ['proco.custom_auth.backends.RemoteAndModelBackend']

# Rest framework configuration
# http://www.django-rest-framework.org/api-guide/settings/
# --------------------------------------------------------------------------

DATE_FORMAT = '%d-%m-%Y'
DATETIME_FORMAT = '%d-%m-%Y %H:%M:%S'

DATE_INPUT_FORMATS = list(default_settings.get('DATE_INPUT_FORMATS'))
DATE_INPUT_FORMATS.append(DATE_FORMAT)

DATETIME_INPUT_FORMATS = list(default_settings.get('DATETIME_INPUT_FORMATS'))
DATETIME_INPUT_FORMATS.extend([
    '%d-%m-%Y %H:%M:%S.%f',
    DATETIME_FORMAT,
    '%d-%m-%Y %H:%M',
    '%d-%m-%Y',
])

REST_FRAMEWORK = {
    'PAGE_SIZE': 10,
    'DEFAULT_PAGINATION_CLASS': 'unicef_restlib.pagination.DynamicPageNumberPagination',
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'proco.custom_auth.authentication.JSONWebTokenAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ],
    'DATE_FORMAT': DATE_FORMAT,
    'DATETIME_FORMAT': DATETIME_FORMAT,
    'DATETIME_INPUT_FORMATS': DATETIME_INPUT_FORMATS,
    'DATE_INPUT_FORMATS': DATE_INPUT_FORMATS,
}

# Template configurations
# --------------------------------------------------------------------------

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            root('proco', 'templates'),
        ],
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
            'loaders': [
                'django.template.loaders.filesystem.Loader',
                'django.template.loaders.app_directories.Loader',
            ],
        },
    },
]

# Fixture configurations
# --------------------------------------------------------------------------

FIXTURE_DIRS = [
    root('proco', 'fixtures'),
]

# Password validation
# https://docs.djangoproject.com/en/1.9/ref/settings/#auth-password-validators
# --------------------------------------------------------------------------

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Django cache settings
# ------------------------------------
NO_EXPIRY_CACHE_PREFIX = env('NO_EXPIRY_CACHE_PREFIX', default='NO_EXPIRY_CACHE')

CACHES = {
    'default': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': env('REDIS_URL', default='redis://localhost:6379/0'),
        'TIMEOUT': 24 * 60 * 60,  # one day
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            'CONNECTION_POOL_KWARGS': {
                'retry_on_timeout': True,
                'socket_timeout': 5,
            },
        },
    },
}

# Internationalization
# https://docs.djangoproject.com/en/1.9/topics/i18n/
# --------------------------------------------------------------------------

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

SITE_ID = 1

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.9/howto/static-files/
# --------------------------------------------------------------------------

STATIC_URL = '/static/'
STATIC_ROOT = root('static')

STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'django.contrib.staticfiles.finders.FileSystemFinder',
)

STATICFILES_DIRS = [
    root('proco', 'assets'),
]

MEDIA_URL = '/media/'
MEDIA_ROOT = root('media')

CELERY_ENABLED = env.bool('CELERY_ENABLED', default=True)
if CELERY_ENABLED:
    # Celery configuration
    # --------------------------------------------------------------------------

    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_TASK_SERIALIZER = 'json'
    CELERY_TASK_IGNORE_RESULT = True

# Django mailing configuration
# --------------------------------------------------------------------------

if CELERY_ENABLED:
    TEMPLATED_EMAIL_BACKEND = 'proco.mailing.backends.AsyncTemplateBackend'
    MAILING_USE_CELERY = env.bool('MAILING_USE_CELERY', default=False)

TEMPLATED_EMAIL_TEMPLATE_DIR = 'email'
TEMPLATED_EMAIL_FILE_EXTENSION = 'html'

DEFAULT_FROM_EMAIL = env('DEFAULT_FROM_EMAIL', default=None)
SERVER_EMAIL = DEFAULT_FROM_EMAIL
EMAIL_BACKEND = 'anymail.backends.mailjet.EmailBackend'

DATA_LAYER_DASHBOARD_URL = env('DATA_LAYER_DASHBOARD_URL', default=None)
API_KEY_ADMIN_DASHBOARD_URL = env('API_KEY_ADMIN_DASHBOARD_URL', default=None)

ANYMAIL = {
    'MAILJET_API_URL': env('MAILJET_API_URL', default='https://api.mailjet.com/v3'),
    'MAILJET_API_KEY': env('MAILJET_API_KEY', default=None),
    'MAILJET_SECRET_KEY': env('MAILJET_SECRET_KEY', default=None),
    'IGNORE_RECIPIENT_STATUS': True,
    'IGNORE_UNSUPPORTED_FEATURES': True,
    'REQUESTS_TIMEOUT': 60.0,
}

NO_REPLY_EMAIL_ID_OPTIONS = env('NO_REPLY_EMAIL_ID_OPTIONS', default=DEFAULT_FROM_EMAIL)
SUPPORT_EMAIL_ID = env('SUPPORT_EMAIL_ID', default=DEFAULT_FROM_EMAIL)
SUPPORT_PHONE_NUMBER = env('SUPPORT_PHONE_NUMBER', default='Test Phone No for Support')

# Images
# ---------------

IMAGES_PATH = 'images'

# Crispy forms
# ---------------

CRISPY_TEMPLATE_PACK = 'bootstrap4'

# CORS headers
# --------------

CORS_ORIGIN_ALLOW_ALL = True
CORS_ALLOW_HEADERS = (
    'x-requested-with',
    'content-type',
    'accept',
    'origin',
    'authorization',
    'api-key',
)

CORS_ALLOW_ORIGINS = env('CORS_ALLOW_ORIGINS', default=None)

CORS_EXPOSE_HEADERS = (
    'content-disposition',
    'access-control-allow-origin',
)

# Admin Reorder Models
# --------------

ADMIN_REORDER = (
    'constance',
    {
        'app': 'custom_auth',
        'label': 'Authentication and authorization',
        'models': (
            'custom_auth.ApplicationUser',
            'custom_auth.Role',
            'custom_auth.UserRoleRelationship',
            'custom_auth.RolePermission',
        ),
    },
    {
        'app': 'connection_statistics',
        'label': 'Summary',
        'models': (
            'connection_statistics.CountryWeeklyStatus',
            'connection_statistics.SchoolWeeklyStatus',
        ),
    },
    {
        'app': 'connection_statistics',
        'label': 'Real Time Connectivity Data',
        'models': (
            'connection_statistics.RealTimeConnectivity',
            'connection_statistics.CountryDailyStatus',
            'connection_statistics.SchoolDailyStatus',
        ),
    },
    'locations',
    'schools',
    'background',
    'contact',
    'accounts',
    'data_sources',
)

RANDOM_SCHOOLS_DEFAULT_AMOUNT = env('RANDOM_SCHOOLS_DEFAULT_AMOUNT', default=20000)

CONTACT_MANAGERS = env.list('CONTACT_MANAGERS', default=['test@test.test'])

CONSTANCE_REDIS_CONNECTION = env('REDIS_URL', default='redis://localhost:6379/0')
CONSTANCE_ADDITIONAL_FIELDS = {
    'email_input': ['django.forms.fields.CharField', {
        'required': False,
        'widget': 'django.forms.EmailInput',
    }],
}
CONSTANCE_CONFIG = {
    'CONTACT_EMAIL': (env.list('CONTACT_EMAIL', default=[]), 'Email to receive contact messages', 'email_input'),
}

# Cache control headers
CACHE_CONTROL_MAX_AGE = 24 * 60 * 60
CACHE_CONTROL_MAX_AGE_FOR_FE = env('CACHE_CONTROL_MAX_AGE_FOR_FE', default=CACHE_CONTROL_MAX_AGE)

LIVE_LAYER_CACHE_FOR_COUNTRY_IDS = env.list('LIVE_LAYER_CACHE_FOR_COUNTRY_IDS', default=['144'])
LIVE_LAYER_CACHE_FOR_WEEKS = env('LIVE_LAYER_CACHE_FOR_WEEKS', default=5)

SECRET_KEY = env('DJANGO_SECRET_KEY', default=None)

JWT_AUTH = {
    # 'JWT_SECRET_KEY': SECRET_KEY,
    # 'JWT_GET_USER_SECRET_KEY': 'proco.custom_auth.utils.get_jwt_secret_key',
    'JWT_EXPIRATION_DELTA': timezone.timedelta(days=1),
    'JWT_AUTH_HEADER_PREFIX': 'Bearer',
    'JWT_ALGORITHM': 'RS256',
    'JWT_VERIFY': True,
    'JWT_VERIFY_EXPIRATION': True,
    'JWT_PUBLIC_KEY': None,
    'JWT_PRIVATE_KEY': None,
    'JWT_LEEWAY': 0,
    'JWT_PAYLOAD_GET_USERNAME_HANDLER': 'proco.custom_auth.utils.jwt_get_username_from_payload_handler',
    'JWT_DECODE_HANDLER': 'proco.custom_auth.utils.jwt_decode_handler',
}

AZURE_CONFIG = {
    'AD_B2C': {
        'TENANT_ID': env('AD_B2C_TENANT_ID', default=None),
        'CLIENT_ID': env('AD_B2C_CLIENT_ID', default=None),
        'BASE_URL': env('AD_B2C_BASE_URL', default=None),
        'DOMAIN': env('AD_B2C_DOMAIN', default=None),
        'SIGNUP_SIGNIN_POLICY': env('AD_B2C_SIGNUP_SIGNIN_POLICY', default=None),
        'FORGOT_PASSWORD_POLICY': env('AD_B2C_FORGOT_PASSWORD_POLICY', default=None),
        'EDIT_PROFILE_POLICY': env('AD_B2C_EDIT_PROFILE_POLICY', default=None),
    }
}

ENABLE_AZURE_COGNITIVE_SEARCH = True

if ENABLE_AZURE_COGNITIVE_SEARCH:
    AZURE_CONFIG['COGNITIVE_SEARCH'] = {
        'SEARCH_ENDPOINT': env('SEARCH_ENDPOINT', default=None),
        'SEARCH_API_KEY': env('SEARCH_API_KEY', default=None),
        'COUNTRY_INDEX_NAME': env('COUNTRY_INDEX_NAME', default='giga_countries'),
        'SCHOOL_INDEX_NAME': env('SCHOOL_INDEX_NAME', default='giga_schools'),
    }

DATA_SOURCE_CONFIG = {
    'SCHOOL_MASTER': {
        'SHARE_NAME': env('SCHOOL_MASTER_SHARE_NAME', default='gold'),
        'SCHEMA_NAME': env('SCHOOL_MASTER_SCHEMA_NAME', default='school-master'),
        'REVIEW_GRACE_PERIOD_IN_HRS': env('SCHOOL_MASTER_REVIEW_GRACE_PERIOD_IN_HRS', default='48'),
        'DASHBOARD_URL': env('SCHOOL_MASTER_DASHBOARD_URL', default=None),
        'SHARE_CREDENTIALS_VERSION': env('SCHOOL_MASTER_SHARE_CREDENTIALS_VERSION', default=1),
        'ENDPOINT': env('SCHOOL_MASTER_ENDPOINT', default=None),
        'BEARER_TOKEN': env('SCHOOL_MASTER_BEARER_TOKEN', default=None),
        'EXPIRATION_TIME': env('SCHOOL_MASTER_EXPIRATION_TIME', default=None),
        'COUNTRY_EXCLUSION_LIST': env('SCHOOL_MASTER_COUNTRY_EXCLUSION_LIST', default='').split(','),
    },
    'QOS': {
        'SHARE_NAME': env('QOS_SHARE_NAME', default='gold'),
        'SCHEMA_NAME': env('QOS_SCHEMA_NAME', default='qos'),
        'SHARE_CREDENTIALS_VERSION': env('QOS_SHARE_CREDENTIALS_VERSION', default=1),
        'ENDPOINT': env('QOS_ENDPOINT', default=None),
        'BEARER_TOKEN': env('QOS_BEARER_TOKEN', default=None),
        'EXPIRATION_TIME': env('QOS_EXPIRATION_TIME', default=None),
        'COUNTRY_EXCLUSION_LIST': env('QOS_COUNTRY_EXCLUSION_LIST', default='').split(','),
    },
    'DAILY_CHECK_APP': {
        'BASE_URL': env('DAILY_CHECK_APP_BASE_URL', default=None),
        'API_CODE': env('DAILY_CHECK_APP_API_CODE', default='DAILY_CHECK_APP'),
    },
}

INVALIDATE_CACHE_HARD = env('INVALIDATE_CACHE_HARD', default='false')

READ_ONLY_DB_KEY = 'read_only_database'

DATABASE_ROUTERS = [
    'proco.utils.db_routers.ReadOnlyDBRouter',
    'dynamic_db_router.DynamicDbRouter',
]

GIGAMAPS_LOG_LEVEL = env('GIGAMAPS_LOG_LEVEL', default='INFO')

# LOGGING
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        },
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        },
        'hostname_filter': {
            '()': 'proco.core.filters.HostInfoFilter',
        },
    },
    'formatters': {
        'verbose': {
            'format': '%(hostname)s %(hostip)s %(asctime)s %(levelname)s %(pathname)s: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': GIGAMAPS_LOG_LEVEL,
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
            'stream': sys.stderr,
            'filters': ['hostname_filter'],
        },
    },
    'loggers': {
        'gigamaps': {
            'level': GIGAMAPS_LOG_LEVEL,
            'handlers': ['console'],
            'filters': ['hostname_filter'],
        },
    },
}


COUNTRY_MAP_API_SAMPLING_LIMIT = env('COUNTRY_MAP_API_SAMPLING_LIMIT', default=None)
ADMIN_MAP_API_SAMPLING_LIMIT = env('ADMIN_MAP_API_SAMPLING_LIMIT', default=None)

READ_ONLY_DATABASE_ALLOWED_REQUESTS = [
    'global-stat',
    'get-time-player-data',
    'tiles-connectivity-view',
    'get-latest-week-and-month',
    'list-published-advance-filters',
    'list-published-data-layers',
    'info-data-layer',
    'map-data-layer',
    'download-schools',
    'download-countries',
    'search-countries-admin-schools',
    'get-time-player-data-v2',
    'tiles-view',
]

READ_ONLY_DATABASE_ALLOWED_MODELS = []

AI_TRANSLATION_ENDPOINT = env('AI_TRANSLATION_ENDPOINT', default=None)
AI_TRANSLATION_KEY = env('AI_TRANSLATION_KEY', default=None)
AI_TRANSLATION_REGION = env('AI_TRANSLATION_REGION', default=None)
AI_TRANSLATION_SUPPORTED_TARGETS = env.list('AI_TRANSLATION_SUPPORTED_TARGETS', default=[])
AI_TRANSLATION_CACHE_KEY_LIMIT = env('AI_TRANSLATION_CACHE_KEY_LIMIT', default=2000)
