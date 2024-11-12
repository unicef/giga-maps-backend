from django.conf import settings
from django.contrib.gis.db.models import PointField
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from django.utils import timezone
from django.utils.translation import ugettext as _
from model_utils import Choices
from model_utils.models import TimeStampedModel
from timezone_field import TimeZoneField

from proco.core.managers import BaseManager
from proco.core.models import CustomDateTimeField
from proco.locations.models import Country, Location, CountryAdminMetadata
from proco.schools.utils import get_imported_file_path


class School(TimeStampedModel):
    ENVIRONMENT_STATUSES = Choices(
        ('rural', _('Rural')),
        ('urban', _('Urban')),
    )

    external_id = models.CharField(max_length=50, blank=True, db_index=True)
    giga_id_school = models.CharField(max_length=50, blank=True, db_index=True)
    name = models.CharField(max_length=1000, default='Name unknown')
    name_lower = models.CharField(max_length=1000, blank=True, editable=False, db_index=True)

    country = models.ForeignKey(Country, related_name='schools', on_delete=models.CASCADE)
    location = models.ForeignKey(Location, null=True, blank=True, related_name='schools', on_delete=models.CASCADE)
    admin1 = models.ForeignKey(
        CountryAdminMetadata,
        related_name='admin1_schools',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    admin2 = models.ForeignKey(
        CountryAdminMetadata,
        related_name='admin2_schools',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    # admin_1_name = models.CharField(max_length=100, blank=True)
    # admin_2_name = models.CharField(max_length=100, blank=True)
    # admin_3_name = models.CharField(max_length=100, blank=True)
    # admin_4_name = models.CharField(max_length=100, blank=True)
    geopoint = PointField(verbose_name=_('Point'), null=True, blank=True)

    timezone = TimeZoneField(blank=True, null=True)
    gps_confidence = models.FloatField(null=True, blank=True)
    altitude = models.PositiveIntegerField(blank=True, default=0)
    address = models.CharField(blank=True, max_length=255)
    postal_code = models.CharField(blank=True, max_length=128)
    email = models.EmailField(max_length=128, null=True, blank=True, default=None)

    education_level = models.CharField(blank=True, max_length=255)
    education_level_lower = models.CharField(blank=True, max_length=255, editable=False, db_index=True)

    education_level_regional = models.CharField(max_length=6400007, blank=True)
    environment = models.CharField(choices=ENVIRONMENT_STATUSES, blank=True, max_length=64)

    school_type = models.CharField(blank=True, max_length=64, db_index=True)
    school_type_lower = models.CharField(blank=True, max_length=64, editable=False, db_index=True)

    last_weekly_status = models.ForeignKey(
        'connection_statistics.SchoolWeeklyStatus', null=True, blank=True,
        on_delete=models.SET_NULL, related_name='_school',
    )

    coverage_type = models.CharField(max_length=10, blank=True, default='unknown')
    connectivity_status = models.CharField(max_length=10, blank=True, default='unknown')
    coverage_status = models.CharField(max_length=10, blank=True, default='unknown')

    establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    deleted = CustomDateTimeField(db_index=True, null=True, blank=True)

    objects = BaseManager()

    class Meta:
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['country', 'giga_id_school', 'deleted'],
                             name='schools_giga_id_unique_with_deleted'),
            UniqueConstraint(fields=['country', 'giga_id_school'],
                             condition=Q(deleted=None),
                             name='schools_giga_id_unique_without_deleted'),
        ]

    def __str__(self):
        return f'{self.country} - {self.admin1} - {self.name}'

    def save(self, **kwargs):
        self.name_lower = str(self.name).lower()
        if self.education_level:
            self.education_level_lower = str(self.education_level).lower()
        if self.school_type:
            self.school_type_lower = str(self.school_type).lower()

        self.external_id = str(self.external_id).lower()
        super().save(**kwargs)

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()

        self.daily_status.all().update(deleted=timezone.now())
        self.weekly_status.all().update(deleted=timezone.now())


class FileImport(TimeStampedModel):
    STATUSES = Choices(
        ('pending', _('Pending')),
        ('started', _('Started')),
        ('completed', _('Completed')),
        ('failed', _('Failed')),
        ('completed_with_errors', _('Completed with errors')),
    )
    PROCESS_STATUSES = [STATUSES.pending, STATUSES.started]

    country = models.ForeignKey(Country, null=True, on_delete=models.CASCADE)
    uploaded_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    uploaded_file = models.FileField(upload_to=get_imported_file_path)
    status = models.CharField(max_length=21, choices=STATUSES, default=STATUSES.pending)
    errors = models.TextField(blank=True)
    statistic = models.TextField(blank=True)

    objects = models.Manager()

    def __str__(self):
        return self.uploaded_file.name

    class Meta:
        ordering = ('id',)

    @property
    def filename(self):
        return self.uploaded_file.name.split('/')[-1]
