from django.conf import settings
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from django.utils.translation import ugettext as _
from model_utils import Choices

from proco.core import models as core_models
from proco.core import utils as core_utilities
from proco.core.managers import BaseManager
from proco.utils import dates as date_utilities


class BackgroundTask(models.Model):
    STATUSES = Choices(
        ('running', _('Running')),
        ('completed', _('Completed')),
    )
    PROCESS_STATUSES = [STATUSES.running]

    task_id = models.CharField(max_length=50, primary_key=True)
    created_at = core_models.CustomDateTimeField(null=True, blank=True)
    completed_at = core_models.CustomDateTimeField(null=True, blank=True)

    name = models.CharField(
        max_length=255,
        null=False,
        verbose_name='Task Unique Name',
        db_index=True,
    )
    description = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='Task Readable Name',
        db_index=True,
    )

    status = models.CharField(default=STATUSES.running, choices=STATUSES, max_length=10)
    log = models.TextField()

    deleted = core_models.CustomDateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='deleted_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Deleted By'
    )

    objects = BaseManager()

    class Meta:
        verbose_name = _('Background Task')
        constraints = [
            UniqueConstraint(fields=['name', 'status', 'deleted'],
                             name='background_task_unique_with_deleted'),
            UniqueConstraint(fields=['name', 'status'],
                             condition=Q(deleted=None),
                             name='background_task_unique_without_deleted'),
        ]

    def __str__(self):
        return f'Task: {self.name}, Status: {self.status}'

    def info(self, text: str):
        if self.log:
            self.log += '\n'

        self.log += '{0}: {1}'.format(
            date_utilities.format_datetime(core_utilities.get_current_datetime_object(), frmt='%d-%m-%Y %H:%M:%S'),
            text)
        self.save()
