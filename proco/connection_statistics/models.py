from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from django.utils import timezone
from django.utils.translation import ugettext as _
from isoweek import Week
from model_utils import Choices
from model_utils.models import TimeStampedModel

from proco.connection_statistics.config import app_config as statistics_configs
from proco.core import models as core_models
from proco.core.managers import BaseManager
from proco.locations.models import Country
from proco.schools.constants import statuses_schema
from proco.schools.models import School
from proco.utils.dates import get_current_week, get_current_year


class ConnectivityStatistics(models.Model):
    connectivity_speed = core_models.PositiveBigIntegerField(help_text=_('bps'), blank=True, null=True, default=None)
    connectivity_upload_speed = core_models.PositiveBigIntegerField(help_text=_('bps'), blank=True, null=True,
                                                                    default=None)

    connectivity_latency = models.FloatField(help_text=_('ms'), blank=True, null=True, default=None)

    connectivity_speed_probe = models.PositiveIntegerField(help_text=_('bps'), blank=True, null=True, default=None)
    connectivity_upload_speed_probe = models.PositiveIntegerField(help_text=_('bps'),
                                                                  blank=True, null=True, default=None)

    connectivity_latency_probe = models.FloatField(help_text=_('ms'), blank=True, null=True, default=None)

    connectivity_speed_mean = models.PositiveIntegerField(help_text=_('bps'), blank=True, null=True, default=None)
    connectivity_upload_speed_mean = models.PositiveIntegerField(help_text=_('bps'),
                                                                 blank=True, null=True, default=None)

    roundtrip_time = models.FloatField(help_text=_('ms'), blank=True, null=True, default=None)
    jitter_download = models.FloatField(help_text=_('ms'), blank=True, null=True, default=None)
    jitter_upload = models.FloatField(help_text=_('ms'), blank=True, null=True, default=None)
    rtt_packet_loss_pct = models.FloatField(help_text=_('percentage'), blank=True, null=True, default=None)

    live_data_source = models.CharField(
        max_length=50,
        choices=statistics_configs.LIVE_DATA_SOURCE_CHOICES,
        default=statistics_configs.UNKNOWN_SOURCE,
    )

    deleted = core_models.CustomDateTimeField(db_index=True, null=True, blank=True)

    class Meta:
        abstract = True


class CountryWeeklyStatus(ConnectivityStatistics, TimeStampedModel, models.Model):
    JOINED = 0
    SCHOOL_MAPPED = 1
    STATIC_MAPPED = 2
    REALTIME_MAPPED = 3
    COUNTRY_CREATED = 4
    SCHOOL_OSM_MAPPED = 5

    INTEGRATION_STATUS_TYPES = Choices(
        (COUNTRY_CREATED, _('Default Country Status')),
        (SCHOOL_OSM_MAPPED, _('School OSM locations mapped')),
        (JOINED, _('Country Joined Project Connect')),
        (SCHOOL_MAPPED, _('School locations mapped')),
        (STATIC_MAPPED, _('Static connectivity mapped')),
        (REALTIME_MAPPED, _('Real time connectivity mapped')),
    )
    CONNECTIVITY_TYPES_AVAILABILITY = Choices(
        ('no_connectivity', _('No data')),
        ('connectivity', _('Using availability information')),
        ('static_speed', _('Using actual static speeds')),
        ('realtime_speed', _('Using actual realtime speeds')),
    )
    COVERAGE_TYPES_AVAILABILITY = Choices(
        ('no_coverage', _('No data')),
        ('coverage_availability', _('Using availability information')),
        ('coverage_type', _('Using actual coverage type')),
    )

    country = models.ForeignKey(Country, related_name='weekly_status', on_delete=models.CASCADE)
    year = models.PositiveSmallIntegerField(default=get_current_year)
    week = models.PositiveSmallIntegerField(default=get_current_week)
    date = models.DateField()
    schools_total = models.PositiveIntegerField(blank=True, default=0)
    schools_connected = models.PositiveIntegerField(blank=True, default=0)

    # connectivity pie chart
    schools_connectivity_good = models.PositiveIntegerField(blank=True, default=0)
    schools_connectivity_moderate = models.PositiveIntegerField(blank=True, default=0)
    schools_connectivity_no = models.PositiveIntegerField(blank=True, default=0)
    schools_connectivity_unknown = models.PositiveIntegerField(blank=True, default=0)

    global_schools_connectivity_good = models.PositiveIntegerField(blank=True, default=0)
    global_schools_connectivity_moderate = models.PositiveIntegerField(blank=True, default=0)
    global_schools_connectivity_no = models.PositiveIntegerField(blank=True, default=0)
    global_schools_connectivity_unknown = models.PositiveIntegerField(blank=True, default=0)

    # coverage pie chart
    schools_coverage_good = models.PositiveIntegerField(blank=True, default=0)
    schools_coverage_moderate = models.PositiveIntegerField(blank=True, default=0)
    schools_coverage_no = models.PositiveIntegerField(blank=True, default=0)
    schools_coverage_unknown = models.PositiveIntegerField(blank=True, default=0)

    integration_status = models.PositiveSmallIntegerField(choices=INTEGRATION_STATUS_TYPES, default=COUNTRY_CREATED)
    avg_distance_school = models.FloatField(blank=True, default=None, null=True)
    schools_with_data_percentage = models.DecimalField(
        decimal_places=5, max_digits=6, default=0, validators=[MaxValueValidator(1), MinValueValidator(0)],
    )
    connectivity_availability = models.CharField(max_length=32, choices=CONNECTIVITY_TYPES_AVAILABILITY,
                                                 default=CONNECTIVITY_TYPES_AVAILABILITY.no_connectivity)
    coverage_availability = models.CharField(max_length=32, choices=COVERAGE_TYPES_AVAILABILITY,
                                             default=COVERAGE_TYPES_AVAILABILITY.no_coverage)

    objects = BaseManager()

    class Meta:
        verbose_name = _('Country Summary')
        verbose_name_plural = _('Country Summary')
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['year', 'week', 'country', 'deleted'],
                             name='countryweeklystatus_unique_with_deleted'),
            UniqueConstraint(fields=['year', 'week', 'country'],
                             condition=Q(deleted=None),
                             name='countryweeklystatus_unique_without_deleted'),
        ]

    def __str__(self):
        return (f'{self.year} {self.country.name} Week {self.week} Speed - {self.connectivity_speed}'
                f'{self.schools_connected} {self.schools_total} {self.schools_connectivity_unknown} '
                f'{self.schools_connectivity_no}'
                f'{self.schools_connectivity_moderate} {self.schools_connectivity_good} '
                f'{self.schools_coverage_unknown} {self.schools_coverage_no} {self.schools_coverage_moderate}'
                f'{self.schools_coverage_good} {self.global_schools_connectivity_good}'
                )

    def save(self, **kwargs):
        self.date = Week(self.year, self.week).monday()
        super().save(**kwargs)

    @property
    def is_verified(self):
        return self.integration_status not in [self.COUNTRY_CREATED, self.SCHOOL_OSM_MAPPED]

    def update_country_status_to_joined(self):
        if self.integration_status == self.SCHOOL_OSM_MAPPED:
            for school in self.country.schools.all():
                school.delete()

        self.integration_status = self.JOINED
        self.save(update_fields=('integration_status',))
        self.country.date_of_join = timezone.now().date()
        self.country.save(update_fields=('date_of_join',))

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()


class SchoolWeeklyStatus(ConnectivityStatistics, TimeStampedModel, models.Model):
    # unable to use choices as should be (COVERAGE_TYPES.4g), because digit goes first
    COVERAGE_UNKNOWN = 'unknown'
    COVERAGE_NO = 'no'
    COVERAGE_2G = '2g'
    COVERAGE_3G = '3g'
    COVERAGE_4G = '4g'
    COVERAGE_5G = '5g'
    COVERAGE_TYPES = Choices(
        (COVERAGE_UNKNOWN, _('Unknown')),
        (COVERAGE_NO, _('No')),
        (COVERAGE_2G, _('2G')),
        (COVERAGE_3G, _('3G')),
        (COVERAGE_4G, _('4G')),
        (COVERAGE_5G, _('5G')),
    )

    school = models.ForeignKey(School, related_name='weekly_status', on_delete=models.CASCADE)
    year = models.PositiveSmallIntegerField(default=get_current_year)
    week = models.PositiveSmallIntegerField(default=get_current_week)
    date = models.DateField()
    num_students = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_teachers = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    num_classroom = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    num_latrines = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    running_water = models.BooleanField(default=False)
    electricity_availability = models.BooleanField(default=False)
    computer_lab = models.BooleanField(default=False)
    num_computers = models.PositiveIntegerField(blank=True, default=None, null=True)
    connectivity = models.NullBooleanField(default=None)
    connectivity_type = models.CharField(_('Type of internet connection'), max_length=64, default='unknown')
    coverage_availability = models.NullBooleanField(default=None)
    coverage_type = models.CharField(max_length=8, default=COVERAGE_TYPES.unknown, choices=COVERAGE_TYPES)

    # New Fields added for School Master Data Source
    download_speed_contracted = models.FloatField(blank=True, default=None, null=True)
    num_computers_desired = models.PositiveIntegerField(blank=True, default=None, null=True)
    electricity_type = models.CharField(max_length=255, null=True, blank=True)
    num_adm_personnel = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    fiber_node_distance = models.FloatField(blank=True, default=None, null=True)
    microwave_node_distance = models.FloatField(blank=True, default=None, null=True)

    schools_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)
    schools_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)
    schools_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)

    nearest_lte_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_umts_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_gsm_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_nr_distance = models.FloatField(blank=True, default=None, null=True)

    pop_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)

    school_data_source = models.CharField(blank=True, null=True, max_length=255)
    school_data_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    school_data_collection_modality = models.CharField(blank=True, null=True, max_length=255)
    school_location_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    connectivity_govt_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    connectivity_govt_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    disputed_region = models.BooleanField(default=False)

    download_speed_benchmark = models.FloatField(blank=True, default=None, null=True)

    num_students_girls = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_students_boys = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_students_other = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_teachers_female = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_teachers_male = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_tablets = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_robotic_equipment = models.PositiveIntegerField(blank=True, default=None, null=True)

    computer_availability = models.NullBooleanField(default=None)
    teachers_trained = models.NullBooleanField(default=None)
    sustainable_business_model = models.NullBooleanField(default=None)
    device_availability = models.NullBooleanField(default=None)

    building_id_govt = models.CharField(blank=True, null=True, max_length=255)
    num_schools_per_building = models.PositiveIntegerField(blank=True, default=None, null=True)

    objects = BaseManager()

    class Meta:
        verbose_name = _('School Summary')
        verbose_name_plural = _('School Summary')
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['year', 'week', 'school', 'deleted'],
                             name='schoolweeklystatus_unique_with_deleted'),
            UniqueConstraint(fields=['year', 'week', 'school'],
                             condition=Q(deleted=None),
                             name='schoolweeklystatus_unique_without_deleted'),
        ]

    def __str__(self):
        return f'{self.year} {self.school.name} Week {self.week} Speed - {self.connectivity_speed}'

    def save(self, **kwargs):
        self.date = self.get_date()
        super().save(**kwargs)

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()

    def get_date(self):
        return Week(self.year, self.week).monday()

    def get_connectivity_status(self, availability):
        if availability in [CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.static_speed,
                            CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.realtime_speed]:
            return statuses_schema.get_connectivity_status_by_connectivity_speed(self.connectivity_speed)

        elif availability == CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.connectivity:
            return statuses_schema.get_status_by_availability(self.connectivity)

    def get_coverage_status(self, availability):
        if availability == CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY.coverage_type:
            return statuses_schema.get_coverage_status_by_coverage_type(self.coverage_type)

        elif availability == CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY.coverage_availability:
            return statuses_schema.get_status_by_availability(self.coverage_availability)


class CountryDailyStatus(ConnectivityStatistics, TimeStampedModel, models.Model):
    country = models.ForeignKey(Country, related_name='daily_status', on_delete=models.CASCADE)
    date = models.DateField()

    objects = BaseManager()

    class Meta:
        verbose_name = _('Country Daily Connectivity Summary')
        verbose_name_plural = _('Country Daily Connectivity Summary')
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['date', 'country', 'live_data_source', 'deleted'],
                             name='countrydailystatus_unique_with_deleted'),
            UniqueConstraint(fields=['date', 'country', 'live_data_source'],
                             condition=Q(deleted=None),
                             name='countrydailystatus_unique_without_deleted'),
        ]

    def __str__(self):
        year, week, weekday = self.date.isocalendar()
        return f'{year} {self.country.name} Week {week} Day {weekday} Speed - {self.connectivity_speed}'

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()


class SchoolDailyStatus(ConnectivityStatistics, TimeStampedModel, models.Model):
    school = models.ForeignKey(School, related_name='daily_status', on_delete=models.CASCADE)
    date = models.DateField()

    objects = BaseManager()

    class Meta:
        verbose_name = _('School Daily Connectivity Summary')
        verbose_name_plural = _('School Daily Connectivity Summary')
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['date', 'school', 'live_data_source', 'deleted'],
                             name='schooldailystatus_unique_with_deleted'),
            UniqueConstraint(fields=['date', 'school', 'live_data_source'],
                             condition=Q(deleted=None),
                             name='schooldailystatus_unique_without_deleted'),
        ]

    def __str__(self):
        year, week, weekday = self.date.isocalendar()
        return f'{year} {self.school.name} Week {week} Day {weekday} Speed - {self.connectivity_speed}'

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()


class RealTimeConnectivity(ConnectivityStatistics, TimeStampedModel, models.Model):
    school = models.ForeignKey(School, related_name='realtime_status', on_delete=models.CASCADE)

    objects = BaseManager()

    class Meta:
        verbose_name = _('Real Time Connectivity Data')
        verbose_name_plural = _('Real Time Connectivity Data')
        ordering = ('id',)

    def __str__(self):
        return f'{self.created} {self.school.name} Speed - {self.connectivity_speed}'

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()


class SchoolRealTimeRegistration(core_models.BaseModelMixin):
    school = models.ForeignKey(School, related_name='realtime_registration_status', on_delete=models.CASCADE)

    rt_registered = models.BooleanField(default=False)
    rt_registration_date = core_models.CustomDateTimeField(db_index=True, null=True, blank=True)
    rt_source = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        verbose_name = _('School Real Time Registration Status')
        verbose_name_plural = _('School Real Time Registration Data')
        ordering = ('id',)
