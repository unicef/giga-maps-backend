from django.apps.registry import apps
from django.contrib.contenttypes.models import ContentType
from django.contrib.gis.db.models import PointField
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from django.utils import timezone
from django.utils.translation import ugettext as _
from timezone_field import TimeZoneField

from proco.core import exceptions as core_exceptions
from proco.core.managers import BaseManager
from proco.core.models import CustomDateTimeField
from proco.locations.models import Country, CountryAdminMetadata
from proco.core import models as core_models


ENTITY_TYPE_CHOICES = (
    ("school", "School"),
    ("health", "Health"),
    ("library", "Library"),
)


class Entity(core_models.BaseModelMixin):
    entity_type = models.CharField(
        max_length=20,
        choices=ENTITY_TYPE_CHOICES,
        default="health",
        db_index=True,
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

    def get_entity_specific_model(self):
        """
        Returns the entity-specific model instance
        """
        if self.entity_type == 'health':
            return getattr(self, 'health_entity', None)
        elif self.entity_type == 'school':
            return getattr(self, 'school_entity', None)
        elif self.entity_type == 'library':
            return getattr(self, 'library_entity', None)
        return None

    def get_master_data_model_class(self):
        """
        Returns master data model class for approval workflow
        """
        model_mapping = {
            'health': 'data_sources.HealthEntityMasterData',
            'school': 'data_sources.SchoolMasterData',
            'library': 'data_sources.LibraryEntityMasterData',  # Future
        }

        model_path = model_mapping.get(self.entity_type)
        if not model_path:
            raise core_exceptions.InvalidModelNameFormatError(model=f'No master data model for entity_type: {self.entity_type}')

        try:
            app_name, model_name = model_path.split('.')
            return apps.get_model(app_name, model_name)
        except (ValueError, ContentType.DoesNotExist):
            raise core_exceptions.InvalidModelNameFormatError(model=model_path)


class HealthEntity(core_models.BaseModelMixin):
    """
    HealthEntity - Operational health facility data
    Contains health-specific fields that are actively used in the system
    """
    entity = models.OneToOneField(
        Entity,
        on_delete=models.CASCADE,
        related_name='health_entity',
        limit_choices_to={'entity_type': 'health'}
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


class SchoolEntity(core_models.BaseModelMixin):
    """
    SchoolEntity - Operational school data
    Mirrors existing School model fields for consistency
    """
    entity = models.OneToOneField(
        Entity,
        on_delete=models.CASCADE,
        related_name='school_entity',
        limit_choices_to={'entity_type': 'school'}
    )

    # School Identifiers
    school_id_govt = models.CharField(blank=True, null=True, max_length=255, db_index=True)

    # Geographic Details
    timezone = TimeZoneField(blank=True, null=True)
    gps_confidence = models.FloatField(null=True, blank=True)
    altitude = models.PositiveIntegerField(blank=True, default=0)
    address = models.CharField(blank=True, max_length=255)
    postal_code = models.CharField(blank=True, max_length=128)
    email = models.EmailField(max_length=128, null=True, blank=True, default=None)

    # Education Information
    education_level = models.CharField(blank=True, max_length=255)
    education_level_lower = models.CharField(blank=True, max_length=255, editable=False, db_index=True)
    education_level_govt = models.CharField(blank=True, null=True, max_length=255)
    education_level_govt_lower = models.CharField(blank=True, null=True, max_length=255, editable=False, db_index=True)
    education_level_regional = models.CharField(max_length=255, blank=True)
    school_type = models.CharField(blank=True, max_length=64, db_index=True)
    school_type_lower = models.CharField(blank=True, max_length=64, editable=False, db_index=True)

    # School Status
    establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    # Education Technology Infrastructure
    computer_lab = models.BooleanField(null=True, blank=True, default=None)
    computer_availability = models.BooleanField(null=True, blank=True, default=None)
    num_computers = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_computers_desired = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_tablets = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_robotic_equipment = models.PositiveIntegerField(blank=True, default=None, null=True)
    device_availability = models.BooleanField(null=True, blank=True, default=None)
    # Education Business Model
    sustainable_business_model = models.BooleanField(null=True, blank=True, default=None)

    class Meta:
        db_table = 'entities_school_entity'
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['entity', 'deleted'],
                           name='school_entity_unique_with_deleted'),
            UniqueConstraint(fields=['entity'],
                           condition=Q(deleted=None),
                           name='school_entity_unique_without_deleted'),
        ]

    def save(self, **kwargs):
        # Maintain lowercase fields for search optimization
        if self.education_level:
            self.education_level_lower = str(self.education_level).lower()
        if self.education_level_govt:
            self.education_level_govt_lower = str(self.education_level_govt).lower()
        if self.school_type:
            self.school_type_lower = str(self.school_type).lower()
        super().save(**kwargs)

    def __str__(self):
        return f'School: {self.entity.name}'
