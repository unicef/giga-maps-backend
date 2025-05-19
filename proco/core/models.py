import datetime

import pytz
from django.conf import settings
from django.db import models
from django.utils import timezone
from simple_history.models import HistoricalRecords

from proco.core.managers import BaseManager
from django.utils.translation import gettext_lazy as _


class PositiveBigIntegerField(models.IntegerField):
    description = _("Big (8 byte) integer")
    MAX_BIGINT = 9223372036854775807

    def get_internal_type(self):
        return "BigIntegerField"

    def formfield(self, **kwargs):
        return super().formfield(**{
            'min_value': 0,
            'max_value': PositiveBigIntegerField.MAX_BIGINT,
            **kwargs,
        })


class CustomDateTimeField(models.DateTimeField):

    def _get_timezone_converted_value(self, value):
        """
        _get_timezone_converted_value
            Method to convert the timezone of the datetime field value
        :param value: DateTime instance
        :return: DateTime instance
        """
        response_timezone = pytz.timezone(settings.TIME_ZONE)
        return value.astimezone(response_timezone)

    def from_db_value(self, value, expression, connection, context):
        """
        from_db_value
             Method called at the time of getting the value from database.
        """
        if isinstance(value, datetime.datetime):
            return self._get_timezone_converted_value(value)
        return value

    def pre_save(self, model_instance, add):
        """
        pre_save
            Method to add current date time to the field which has 'auto_now' or 'auto_now_add'
            attributes.
        """
        if self.auto_now or (self.auto_now_add and add):
            value = self._get_timezone_converted_value(timezone.now())
            setattr(model_instance, self.attname, value)
            return value
        else:
            return super().pre_save(model_instance, add)

    def get_default(self):
        """
        get_default
            Method to return the default value of a DateTime field
        """
        default_value = super().get_default()
        if isinstance(default_value, datetime.datetime):
            default_value = self._get_timezone_converted_value(default_value)
        return default_value


class BaseModelMixin(models.Model):
    """
    BaseModelMixin
        This represents the BaseModel for the project without any creation, modification
        or deletion history.
    Inherits : `models.Model`
    """
    """ Project level variables """

    deleted = CustomDateTimeField(db_index=True, null=True, blank=True)
    last_modified_at = CustomDateTimeField(auto_now=True, verbose_name='Last Updated Date')
    last_modified_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='updated_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Last Updated By'
    )
    created = CustomDateTimeField(auto_now_add=True, verbose_name='Created Date')
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='created_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Created By'
    )
    objects = BaseManager()

    """
    delete
        Method to delete BaseModel Object

    * Overrides delete method by updating deleted with current time
    """

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()

    class Meta:
        abstract = True


class BaseModel(BaseModelMixin, models.Model):
    """
    BaseModel
        This represents the BaseModel for the project with all creation, modification
        or deletion history.
    Inherits : `BaseModelMixin`
    """
    """ Project level variables """
    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True
        ordering = ['last_modified_at']


class DataSourceModelMixin(models.Model):
    """
    DataSourceModelMixin
        This represents the common properties of the Data Source Models.
    Inherits : `models.Model`
    """

    pulled_at = CustomDateTimeField(null=True, blank=True, verbose_name='Pulled at Date')

    objects = models.Manager()

    class Meta:
        abstract = True
