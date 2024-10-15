from django.db import models
from django.utils.translation import ugettext_lazy as _
from model_utils.models import TimeStampedModel

from proco.utils.dates import format_datetime


class ContactMessage(TimeStampedModel, models.Model):

    CATEGORY_GIGAMAPS = 'gigamaps'
    CATEGORY_GIGAMETER = 'gigameter'
    CATEGORY_DONATION = 'donation'

    CATEGORY_CHOICES = (
        (CATEGORY_GIGAMAPS, 'Giga Maps'),
        (CATEGORY_GIGAMETER, 'Giga Meter'),
        (CATEGORY_DONATION, 'Donation'),
    )

    full_name = models.CharField(max_length=256)
    organisation = models.CharField(max_length=256)
    purpose = models.CharField(max_length=256)
    message = models.TextField()

    email = models.EmailField(_('email address'), blank=True, null=True)
    category = models.CharField(max_length=100, db_index=True, default=CATEGORY_GIGAMAPS)

    objects = models.Manager()

    def __str__(self):
        return 'Message from: {0} on ({1})'.format(self.full_name, format_datetime(self.created))
