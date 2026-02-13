import logging

from django.apps.registry import apps
from django.contrib.gis.db.models import PointField
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from django.utils import timezone
from django.utils.translation import ugettext as _
from jsonfield import JSONField
from timezone_field import TimeZoneField

from proco.core import exceptions as core_exceptions
from proco.core.managers import BaseManager
from proco.core.models import CustomDateTimeField
from proco.locations.models import Country, CountryAdminMetadata
from proco.core import models as core_models

logger = logging.getLogger('gigamaps.' + __name__)


class EntityType(core_models.BaseModelMixin):
    """ EntityType: Database-driven registry of entity types. """

    code = models.CharField(
        max_length=20,
        unique=True,
        db_index=True,
        verbose_name='Type Code',
        help_text='Unique short code, e.g. "school", "health", "library"',
    )

    name = models.CharField(
        max_length=100,
        verbose_name='Display Name',
        help_text='Human-readable name, e.g. "School", "Health Facility"',
    )

    description = models.TextField(blank=True, default='')

    is_active = models.BooleanField(
        default=True,
        db_index=True,
        help_text='Inactive types are hidden from APIs and UI',
    )

    display_order = models.PositiveSmallIntegerField(
        default=0,
        help_text='Lower values appear first in lists',
    )

    detail_model = models.CharField(
        max_length=100,
        blank=True,
        default='',
        help_text='App label + model name for entity-specific model, e.g. "entities.HealthEntity"',
    )

    # Related name on Entity that points to the OneToOne detail model (e.g. "health_entity")
    detail_related_name = models.CharField(
        max_length=50,
        blank=True,
        default='',
        help_text='Related name on Entity for the OneToOne detail model, e.g. "health_entity"',
    )

    # App label + model name for the master data model (e.g. "data_sources.HealthEntityMasterIntermediateData")
    master_data_model = models.CharField(
        max_length=100,
        blank=True,
        default='',
        help_text='App label + model name for master data model, e.g. "data_sources.HealthEntityMasterIntermediateData"',
    )

    extra_config = JSONField(
        null=True,
        blank=True,
        default=dict,
        help_text='Additional type-specific configuration as JSON',
    )

    is_legacy = models.BooleanField(
        default=False,
        help_text='If True, this type maps to the legacy School model instead of Entity',
    )

    class Meta:
        db_table = 'entities_entity_type'
        ordering = ('display_order', 'code')
        verbose_name = 'Entity Type'
        verbose_name_plural = 'Entity Types'
        constraints = [
            UniqueConstraint(
                fields=['code', 'deleted'],
                name='entity_type_code_unique_with_deleted',
            ),
            UniqueConstraint(
                fields=['code'],
                condition=Q(deleted=None),
                name='entity_type_code_unique_without_deleted',
            ),
        ]

    def __str__(self):
        return f'{self.name} ({self.code})'

    def save(self, **kwargs):
        self.code = str(self.code).lower().strip()
        super().save(**kwargs)

    def get_detail_model_class(self):
        """
        Returns the Django model class for the entity-specific detail model.
        Returns None if not configured.
        """
        if not self.detail_model:
            return None
        try:
            app_label, model_name = self.detail_model.split('.')
            return apps.get_model(app_label, model_name)
        except (ValueError, LookupError) as e:
            logger.warning('Failed to resolve detail model "%s" for entity type "%s": %s',
                           self.detail_model, self.code, e)
            return None

    def get_tile_config(self):
        """
        Returns tile generation configuration dict for this entity type.
        Uses extra_config JSON field for tile_cache_prefix and tile_master_data_table.
        """
        extra = self.extra_config or {}
        return {
            'table': 'entities_entity',
            'srid': '4326',
            'geomColumn': 'geopoint',
            'attrColumns': 'id',
            'cache_prefix': extra.get('tile_cache_prefix', f'{self.code.upper()}_STATUS_CONNECTIVITY_TILES_MAP'),
            'master_data_table': extra.get('tile_master_data_table', ''),
        }

    def get_master_data_model_class(self):
        """
        Returns the Django model class for the master data model.
        Returns None if not configured.
        """
        if not self.master_data_model:
            return None
        try:
            app_label, model_name = self.master_data_model.split('.')
            return apps.get_model(app_label, model_name)
        except (ValueError, LookupError) as e:
            logger.warning('Failed to resolve master data model "%s" for entity type "%s": %s',
                           self.master_data_model, self.code, e)
            return None

    def get_detail_instance(self, entity):
        """
        Given an Entity instance, returns the related entity-specific detail instance.
        Returns None if not configured or not found.
        """
        if not self.detail_related_name:
            return None
        return getattr(entity, self.detail_related_name, None)

    @classmethod
    def get_by_code(cls, code):
        """
        Retrieve an active EntityType by its code. Returns None if not found.
        """
        try:
            return cls.objects.get(code=code, deleted__isnull=True, is_active=True)
        except cls.DoesNotExist:
            return None

    @classmethod
    def get_all_active(cls):
        """
        Returns queryset of all active entity types.
        """
        return cls.objects.filter(deleted__isnull=True, is_active=True).order_by('display_order', 'code')

    @classmethod
    def get_choices(cls):
        """
        Returns entity type choices suitable for CharField choices parameter.
        """
        choices = cls.objects.filter(
            deleted__isnull=True,
            is_active=True,
        ).order_by('display_order', 'code').values_list('code', 'name')
        if choices.exists():
            return tuple(choices)
        return []


class Entity(core_models.BaseModelMixin):
    entity_type = models.ForeignKey(
        EntityType,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name='entities',
    )

    external_id = models.CharField(max_length=50, blank=True, db_index=True)
    giga_id = models.CharField(max_length=50, db_index=True)
    name = models.CharField(max_length=1000, default='Name unknown')
    name_lower = models.CharField(max_length=1000, blank=True, editable=False, db_index=True)

    country = models.ForeignKey(Country, related_name='entities', on_delete=models.CASCADE)
    admin1 = models.ForeignKey(
        CountryAdminMetadata,
        related_name='admin1_entities',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    admin2 = models.ForeignKey(
        CountryAdminMetadata,
        related_name='admin2_entities',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    geopoint = PointField(verbose_name=_('Point'), null=True, blank=True)

    ENVIRONMENT_TYPE_RURAL = 'rural'
    ENVIRONMENT_TYPE_URBAN = 'urban'
    ENVIRONMENT_TYPE_CHOICES = (
        (ENVIRONMENT_TYPE_RURAL, 'Rural'),
        (ENVIRONMENT_TYPE_URBAN, 'Urban'),
    )

    environment = models.CharField(
        max_length=20,
        choices=ENVIRONMENT_TYPE_CHOICES,
        null=True,
        blank=True,
        db_index=True,
    )

    last_weekly_status = models.ForeignKey(
        'connection_statistics.EntityWeeklyStatus', null=True, blank=True,
        on_delete=models.SET_NULL, related_name='_entity',
    )

    connectivity_status = models.CharField(max_length=10, blank=True, default='unknown')
    coverage_status = models.CharField(max_length=10, blank=True, default='unknown')

    # Common Infrastructure Fields
    water_availability = models.BooleanField(null=True, blank=True, default=None)
    electricity_availability = models.BooleanField(null=True, blank=True, default=None)
    electricity_type = models.CharField(blank=True, null=True, max_length=255)

    # Population Data
    pop_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_5km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_10km = models.PositiveIntegerField(blank=True, default=None, null=True)

    # Government Connectivity Data
    connectivity_govt = models.BooleanField(null=True, blank=True, default=None)
    connectivity_type_govt = models.CharField(blank=True, null=True, max_length=255)
    connectivity_govt_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    # Data Source Information
    data_source = models.CharField(blank=True, null=True, max_length=255)
    data_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    data_collection_modality = models.CharField(blank=True, null=True, max_length=255)

    last_master_status_id = core_models.PositiveBigIntegerField(null=True, blank=True, default=None)

    class Meta:
        db_table = 'entities_entity'
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['entity_type', 'country', 'giga_id', 'deleted'],
                             name='entities_giga_id_unique_with_deleted'),
            UniqueConstraint(fields=['entity_type', 'country', 'giga_id'],
                             condition=Q(deleted=None),
                             name='entities_giga_id_unique_without_deleted'),
        ]

    def __str__(self):
        return f'{self.entity_type} - {self.country} - {self.admin1} - {self.name}'

    def save(self, **kwargs):
        self.name_lower = str(self.name).lower()
        if self.external_id:
            self.external_id = str(self.external_id).lower()
        super().save(**kwargs)

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()

        # Cascade to entity-specific models
        entity_specific = self.get_entity_specific_model()
        if entity_specific:
            entity_specific.delete(force=force)

        self.daily_status.all().update(deleted=timezone.now())
        self.weekly_status.all().update(deleted=timezone.now())

    def get_entity_type_config(self):
        """
        Returns the EntityType registry object for this entity.
        """
        return self.entity_type

    def get_entity_specific_model(self):
        """
        Returns the entity-specific model instance using the EntityType registry.
        """
        entity_type_config = self.get_entity_type_config()
        if entity_type_config:
            return entity_type_config.get_detail_instance(self)

    def get_master_data_model_class(self):
        """
        Returns master data model class for approval workflow using the EntityType registry.
        """
        entity_type_config = self.get_entity_type_config()
        if entity_type_config:
            model_class = entity_type_config.get_master_data_model_class()
            if model_class:
                return model_class

class HealthEntity(core_models.BaseModelMixin):
    """
    HealthEntity - Operational health facility data
    Contains health-specific fields that are actively used in the system
    """
    entity = models.OneToOneField(
        Entity,
        on_delete=models.CASCADE,
        related_name='health_entity',
        limit_choices_to={'entity_type__code': 'health'}
    )

    # Health Facility Identifiers
    dhis2_id = models.CharField(blank=True, null=True, max_length=255, db_index=True)
    hims_id = models.CharField(blank=True, null=True, max_length=255, db_index=True)
    hfml_id = models.CharField(blank=True, null=True, max_length=255, db_index=True)
    facility_id_govt = models.CharField(max_length=50, blank=True, db_index=True)

    # Basic Facility Information
    facility_type = models.CharField(blank=True, null=True, max_length=255)
    facility_ownership = models.CharField(blank=True, null=True, max_length=255)
    facility_level = models.CharField(max_length=20, blank=True)  # Primary, Secondary, Tertiary
    health_service_provider = models.CharField(blank=True, null=True, max_length=255)

    # Facility Status
    is_facility_open = models.BooleanField(null=True, blank=True, default=None)
    licensing_status = models.CharField(max_length=50, blank=True)
    facility_hours = models.CharField(max_length=50, blank=True)
    establishment_year = models.PositiveSmallIntegerField(blank=True, null=True)

    # Capacity Information
    num_beds_total = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_beds_icu = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_theatres = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_toilets = models.PositiveIntegerField(blank=True, default=None, null=True)

    # Staff Information
    num_healthworkers = models.PositiveIntegerField(blank=True, default=None, null=True)
    staff_doctors = models.PositiveIntegerField(blank=True, default=None, null=True)
    staff_nurses = models.PositiveIntegerField(blank=True, default=None, null=True)
    staff_midwives = models.PositiveIntegerField(blank=True, default=None, null=True)
    staff_laboratorians = models.PositiveIntegerField(blank=True, default=None, null=True)
    staff_pharmacists = models.PositiveIntegerField(blank=True, default=None, null=True)

    # Services
    emergency_services_available = models.BooleanField(null=True, blank=True, default=None)
    services_offered = models.CharField(blank=True, null=True, max_length=500)

    # Infrastructure
    power_backup_system = models.BooleanField(null=True, blank=True, default=None)
    cold_chain_available = models.BooleanField(null=True, blank=True, default=None)
    waste_management_system = models.CharField(blank=True, null=True, max_length=50)
    hmis_system = models.BooleanField(null=True, blank=True, default=None)

    # Population and Outreach
    catchment_population = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_outpatients = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_inpatients = models.PositiveIntegerField(blank=True, default=None, null=True)

    # Contact Information
    facility_address = models.CharField(blank=True, null=True, max_length=255)

    # Network Infrastructure (for telemedicine)
    fiber_node_distance = models.FloatField(blank=True, default=None, null=True)
    microwave_node_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_lte_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_umts_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_gsm_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_nr_distance = models.FloatField(blank=True, default=None, null=True)

    # Administrative
    num_adm_personnel = models.PositiveIntegerField(blank=True, default=None, null=True)
    disputed_region = models.BooleanField(default=False)

    class Meta:
        db_table = 'entities_health_entity'
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['entity', 'deleted'],
                           name='health_entity_unique_with_deleted'),
            UniqueConstraint(fields=['entity'],
                           condition=Q(deleted=None),
                           name='health_entity_unique_without_deleted'),
        ]

    def __str__(self):
        return f'Health: {self.entity.name}'