from django.db import models
from model_utils.models import TimeStampedModel

from proco.utils.dates import format_datetime


class ContactMessage(TimeStampedModel, models.Model):
    full_name = models.CharField(max_length=256)
    organisation = models.CharField(max_length=256)
    purpose = models.CharField(max_length=256)
    message = models.TextField()

    objects = models.Manager()

    def __str__(self):
        return 'Message from: {0} on ({1})'.format(self.full_name, format_datetime(self.created))
