from django.conf import settings
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from jsonfield import JSONField

from proco.core import models as core_models
from proco.locations.models import Country


class API(core_models.BaseModel):
    """
    API
        This class define model used to store API entity details.
    Inherits : `BaseModelMixin`
    """

    API_CATEGORY_PUBLIC = 'public'
    API_CATEGORY_PRIVATE = 'private'
    API_CATEGORY_CHOICES = (
        (API_CATEGORY_PUBLIC, 'Public API'),
        (API_CATEGORY_PRIVATE, 'Private API'),
    )

    code = models.CharField(max_length=32, default='CODE')

    name = models.CharField(
        max_length=255,
        null=False,
        verbose_name='API Name',
        db_index=True,
    )
    description = models.CharField(max_length=500, null=True, blank=True)
    category = models.CharField(
        max_length=10,
        choices=API_CATEGORY_CHOICES,
        verbose_name='API Category',
        default=API_CATEGORY_PRIVATE,
    )

    documentation_url = models.URLField(null=True)

    download_url = models.URLField(null=True)
    report_title = models.CharField(max_length=255, null=True, blank=True)

    default_filters = JSONField(null=True, default=dict)

    class Meta:
        ordering = ['last_modified_at']


class APIKey(core_models.BaseModel):
    """
    APIKey
        This class define model used to store all the API keys created for documentation or download.
    Inherits : `BaseModelMixin`
    """

    # Key approval status by PROCO admin
    INITIATED = 'INITIATED'
    APPROVED = 'APPROVED'
    DECLINED = 'DECLINED'

    STATUS_CHOICES = (
        (INITIATED, 'Initiated'),
        (APPROVED, 'Approved'),
        (DECLINED, 'Declined'),
    )

    api_key = models.CharField(max_length=500, null=False, verbose_name='API Key Value', db_index=True)
    description = models.CharField(max_length=500, null=True, blank=True)

    valid_from = models.DateField(
        auto_now=True,
        verbose_name='API Key Valid From Date',
    )
    valid_to = models.DateField(
        null=True,
        blank=True,
        verbose_name='API Key Valid Till Date',
        db_index=True,
    )

    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='user_api_keys', on_delete=models.DO_NOTHING)
    api = models.ForeignKey(API, related_name='api_keys', on_delete=models.DO_NOTHING)

    # API query param filters
    filters = JSONField(null=True, default=dict)

    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=INITIATED, db_index=True)
    status_updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name='user_updated_api_keys',
        null=True,
        blank=True,
        on_delete=models.DO_NOTHING,
    )

    extension_valid_to = models.DateField(
        null=True,
        blank=True,
        verbose_name='API Key Extension Requested Till Date',
    )
    extension_status = models.CharField(
        max_length=10,
        choices=STATUS_CHOICES,
        null=True,
        blank=True,
    )
    extension_status_updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name='user_updated_api_keys_extensions',
        null=True,
        blank=True,
        on_delete=models.DO_NOTHING,
    )

    has_write_access = models.BooleanField(default=False)
    write_access_reason = models.TextField(null=True, blank=True)

    class Meta:
        ordering = ['last_modified_at']


class APIKeyCountryRelationship(core_models.BaseModelMixin):
    """
    APIKeyCountryRelationship
        This model is used to store the Api Key and Country relationship.
    """
    api_key = models.ForeignKey(APIKey, related_name='active_countries', on_delete=models.DO_NOTHING)
    country = models.ForeignKey(Country, related_name='active_api_keys', on_delete=models.DO_NOTHING)

    class Meta:
        ordering = ['last_modified_at']
        constraints = [
            UniqueConstraint(fields=['api_key', 'country', 'deleted'],
                             name='unique_with_deleted_for_api_key_country'),
            UniqueConstraint(fields=['api_key', 'country'],
                             condition=Q(deleted=None),
                             name='unique_without_deleted_for_api_key_country'),
        ]


class Message(core_models.BaseModel):
    SEVERITY_CRITICAL = 'CRITICAL'
    SEVERITY_LOW = 'LOW'
    SEVERITY_MEDIUM = 'MEDIUM'
    SEVERITY_HIGH = 'HIGH'

    MESSAGE_SEVERITY_TYPE_CHOICES = (
        (SEVERITY_HIGH, 'High'),
        (SEVERITY_LOW, 'Low'),
        (SEVERITY_MEDIUM, 'Medium'),
        (SEVERITY_CRITICAL, 'Critical'),
    )

    TYPE_SMS = 'SMS'
    TYPE_EMAIL = 'EMAIL'
    TYPE_NOTIFICATION = 'NOTIFICATION'

    MESSAGE_MODE_CHOICES = (
        (TYPE_SMS, 'SMS'),
        (TYPE_EMAIL, 'Email'),
        (TYPE_NOTIFICATION, 'Notification'),
    )

    severity = models.CharField(
        max_length=20,
        choices=MESSAGE_SEVERITY_TYPE_CHOICES,
        default='MEDIUM',
    )

    type = models.CharField(
        max_length=20,
        choices=MESSAGE_MODE_CHOICES,
        db_index=True,
    )

    # Email address/Phone No/User ID
    sender = models.CharField(max_length=100, null=True, blank=True)
    recipient = JSONField(null=True, default=list)

    retry_count = models.IntegerField(default=0)
    is_sent = models.BooleanField(default=False)
    is_read = models.BooleanField(default=False)

    subject_text = models.TextField(null=True)
    message_text = models.TextField()
    template = models.TextField(null=True)
    description = models.CharField(max_length=500, null=True, blank=True)

    class Meta:
        ordering = ['last_modified_at']


class DataSource(core_models.BaseModel):
    """
    DataSource
        This class define model used to store Data source entity details.
    Inherits : `BaseModel`
    """

    DATA_SOURCE_TYPE_SCHOOL_MASTER = 'SCHOOL_MASTER'
    DATA_SOURCE_TYPE_DAILY_CHECK_APP = 'DAILY_CHECK_APP'
    DATA_SOURCE_TYPE_QOS = 'QOS'

    DATA_SOURCE_TYPE_CHOICES = (
        (DATA_SOURCE_TYPE_SCHOOL_MASTER, 'School Master'),
        (DATA_SOURCE_TYPE_DAILY_CHECK_APP, 'Daily Check App'),
        (DATA_SOURCE_TYPE_QOS, 'QoS'),
    )

    DATA_SOURCE_STATUS_DRAFT = 'DRAFT'
    DATA_SOURCE_STATUS_READY_TO_PUBLISH = 'READY_TO_PUBLISH'
    DATA_SOURCE_STATUS_PUBLISHED = 'PUBLISHED'
    DATA_SOURCE_STATUS_DISABLED = 'DISABLED'

    DATA_SOURCE_STATUS_CHOICES = (
        (DATA_SOURCE_STATUS_DRAFT, 'In Draft'),
        (DATA_SOURCE_STATUS_READY_TO_PUBLISH, 'Ready to Publish'),
        (DATA_SOURCE_STATUS_PUBLISHED, 'Published'),
        (DATA_SOURCE_STATUS_DISABLED, 'Disabled'),
    )

    name = models.CharField(
        max_length=255,
        null=False,
        verbose_name='Data Source Name',
        db_index=True,
    )

    description = models.CharField(max_length=500, null=True, blank=True)

    data_source_type = models.CharField(
        max_length=50,
        choices=DATA_SOURCE_TYPE_CHOICES,
        verbose_name='Data Source Type',
        default=DATA_SOURCE_TYPE_SCHOOL_MASTER,
    )

    request_config = JSONField(null=True, default=dict)
    # {
    #     'url': 'https://uni-connect-services-dev.azurewebsites.net/api/v1/measurements',
    #     'method': 'get',
    #     'headers': {
    #           'Content-Type' : 'application/json',
    #           'Authorization' : 'Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ilg1ZVhrNHh5b2pORnVtMW'
    #     }
    # }

    column_config = JSONField(null=True, default=list)
    # [{
    #     'name': 'connectivity_status',
    #     'type': 'str',
    #     'aggregation_applicable': False,
    #     'aggregation_options': [] # Sum, min, max, avg, median, mean
    #     'is_parameter': True,
    #     'alias': 'Connectivity Status'
    # }]

    version = models.CharField(max_length=255, null=True, blank=True)

    status = models.CharField(max_length=50, choices=DATA_SOURCE_STATUS_CHOICES, default=DATA_SOURCE_STATUS_DRAFT)
    published_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='source_published_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Published By'
    )
    published_at = core_models.CustomDateTimeField(db_index=True, null=True, blank=True)

    class Meta:
        ordering = ['last_modified_at']


class DataLayer(core_models.BaseModel):
    """
    DataLayer
        This class define model used to store Data Layer entity details.
    Inherits : `BaseModel`
    """

    LAYER_TYPE_LIVE = 'LIVE'
    LAYER_TYPE_STATIC = 'STATIC'

    LAYER_TYPE_CHOICES = (
        (LAYER_TYPE_LIVE, 'Live'),
        (LAYER_TYPE_STATIC, 'Static'),
    )

    LAYER_CATEGORY_CONNECTIVITY = 'CONNECTIVITY'
    LAYER_CATEGORY_COVERAGE = 'COVERAGE'

    LAYER_CATEGORY_CHOICES = (
        (LAYER_CATEGORY_CONNECTIVITY, 'Connectivity Layer Category'),
        (LAYER_CATEGORY_COVERAGE, 'Coverage Layer Category'),
    )

    LAYER_STATUS_DRAFT = 'DRAFT'
    LAYER_STATUS_READY_TO_PUBLISH = 'READY_TO_PUBLISH'
    LAYER_STATUS_PUBLISHED = 'PUBLISHED'
    LAYER_STATUS_DISABLED = 'DISABLED'

    STATUS_CHOICES = (
        (LAYER_STATUS_DRAFT, 'In Draft'),
        (LAYER_STATUS_READY_TO_PUBLISH, 'Ready to Publish'),
        (LAYER_STATUS_PUBLISHED, 'Published'),
        (LAYER_STATUS_DISABLED, 'Disabled'),
    )

    icon = models.TextField(null=True, blank=True)

    # Unique
    code = models.CharField(
        max_length=255,
        null=False,
        verbose_name='Layer Code',
        default='UNKNOWN',
        db_index=True,
    )

    name = models.CharField(
        max_length=255,
        null=False,
        verbose_name='Layer Name',
        db_index=True,
    )

    description = models.CharField(max_length=500, null=True, blank=True)

    version = models.CharField(max_length=255, null=True, blank=True)

    type = models.CharField(
        max_length=10,
        choices=LAYER_TYPE_CHOICES,
        db_index=True,
    )

    category = models.CharField(
        max_length=20,
        choices=LAYER_CATEGORY_CHOICES,
        verbose_name='Layer Category',
        default=LAYER_CATEGORY_CONNECTIVITY,
    )

    applicable_countries = JSONField(null=True, default=list)
    global_benchmark = JSONField(null=True, default=dict)

    # GIGA - Add Data Layer - Legend Categorization - Static Layer
    legend_configs = JSONField(null=True, default=list)

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=LAYER_STATUS_DRAFT, db_index=True)
    published_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='layer_published_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Published By'
    )
    published_at = core_models.CustomDateTimeField(db_index=True, null=True, blank=True)

    is_reverse = models.BooleanField(default=False)

    class Meta:
        ordering = ['last_modified_at']

    def save(self, **kwargs):
        self.code = str(self.code).upper()
        super().save(**kwargs)


class DataLayerDataSourceRelationship(core_models.BaseModelMixin):
    """
    DataLayerDataSourceRelationship
        This model is used to store the Data Layer and Data Source relationship.
    """
    data_layer = models.ForeignKey(DataLayer, related_name='data_sources', on_delete=models.DO_NOTHING)
    data_source = models.ForeignKey(DataSource, related_name='layers', on_delete=models.DO_NOTHING)

    # API Parameter:
    data_source_column = JSONField(null=True, default=dict)

    class Meta:
        ordering = ['last_modified_at']


class DataLayerCountryRelationship(core_models.BaseModelMixin):
    """
    DataLayerCountryRelationship
        This model is used to store the Data Layer and Country relationship.
    """
    data_layer = models.ForeignKey(DataLayer, related_name='active_countries', on_delete=models.DO_NOTHING)
    country = models.ForeignKey(Country, related_name='active_layers', on_delete=models.DO_NOTHING)

    is_default = models.BooleanField(default=False)
    data_sources = JSONField(null=True, default=dict)

    class Meta:
        ordering = ['last_modified_at']
        constraints = [
            UniqueConstraint(fields=['data_layer', 'country', 'deleted'],
                             name='unique_with_deleted'),
            UniqueConstraint(fields=['data_layer', 'country'],
                             condition=Q(deleted=None),
                             name='unique_without_deleted'),
        ]
