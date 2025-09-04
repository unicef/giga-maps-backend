from django.apps.registry import apps
from django.contrib.contenttypes.models import ContentType
from django.contrib.gis.db.models import PointField
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from django.utils import timezone
from django.utils.translation import ugettext as _
from model_utils.models import TimeStampedModel

from proco.core import exceptions as core_exceptions
from proco.core.managers import BaseManager
from proco.core.models import CustomDateTimeField
from proco.locations.models import Country, CountryAdminMetadata
from proco.core import models as core_models


class Entity(TimeStampedModel):
    # School/Health/Library
    entity_name = models.CharField(max_length=20, db_index=True)

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

    last_master_status_id = core_models.PositiveBigIntegerField(null=True, blank=True, default=None)

    deleted = CustomDateTimeField(db_index=True, null=True, blank=True)

    objects = BaseManager()

    class Meta:
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['entity_name', 'country', 'giga_id', 'deleted'],
                             name='entities_giga_id_unique_with_deleted'),
            UniqueConstraint(fields=['entity_name', 'country', 'giga_id'],
                             condition=Q(deleted=None),
                             name='entities_giga_id_unique_without_deleted'),
        ]

    def __str__(self):
        return f'{self.entity_name} - {self.country} - {self.admin1} - {self.name}'

    def save(self, **kwargs):
        self.name_lower = str(self.name).lower()
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

    def get_model_class_from_entity_name(self):
        """
        Returns model class from a name of app_label.ModelName format.
        :return Model:
        """
        model = 'connection_statistics.SchoolWeeklyStatus'

        try:
            if self.entity_name == 'health':
                model = 'entities.HealthMasterStatus'
            app_name, model_name = model.split('.')
            model = apps.get_model(app_name, model_name)
            return model
        except (ValueError, ContentType.DoesNotExist):
            raise core_exceptions.InvalidModelNameFormatError(model=model)


class HealthMasterStatus(core_models.BaseModelMixin, core_models.BaseMasterStatusModel):
    """
    SchoolMasterStatus
        This class define model used to store School Master Data.
    Inherits : `BaseModelMixin`
    """

    entity = models.ForeignKey(
        Entity,
        blank=False,
        null=False,
        related_name='master_status',
        on_delete=models.DO_NOTHING,
        verbose_name='Master Sync'
    )

    class Meta:
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['entity', 'version', 'deleted'],
                             name='entities_master_status_unique_with_deleted'),
            UniqueConstraint(fields=['entity', 'version'],
                             condition=Q(deleted=None),
                             name='entities_master_status_unique_without_deleted'),
        ]

    def __str__(self):
        return f'{self.entity} - {self.version}'
