from typing import List
from django.utils import timezone
from celery import current_task
from proco.background.models import BackgroundTask
from proco.locations.models import Country
from datetime import datetime
import pytz
import uuid, random
from django.test import TestCase
from django.core.cache import cache


class BackgroundCeleryTaskTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.data = {"name": str(uuid.uuid4())[0:10],
                    "code": str(uuid.uuid4())[0:2].upper(),
                    "last_weekly_status_id": 2091,
                    "flag": "images/7962e7d2-ea1f-4571-a031-bb830fd575c6.png"}
        cls.country_id = [Country.objects.create(**cls.data).id]

        data = {'task_id': uuid.uuid4(), 'status': 'running', 'log': "",
                'completed_at': datetime.now(pytz.timezone('Africa/Lagos'))}

        cls.task_id = str(BackgroundTask.objects.create(**data).task_id)

    def setUp(self):
        cache.clear()
        super().setUp()

    # def test_reset_countries_data(self):
    #     ids = self.country_id
    #     task_id = self.task_id
    #     task = BackgroundTask.objects.get_or_create(task_id=task_id)[0]
    #
    #     queryset = Country.objects.filter(id__in=ids)
    #     task.info(f'{", ".join(map(str, queryset))} reset started')
    #
    #     for obj in queryset:
    #         task.info(f'{obj} started')
    #         obj._clear_data_country()
    #         obj.invalidate_country_related_cache()
    #         task.info(f'{obj} completed')
    #
    #     task.status = BackgroundTask.STATUSES.completed
    #     task.completed_at = timezone.now()
    #     task.save()
    #
    #     self.assertEqual(task_id, task.task_id)

    def test_validate_countries(self):
        ids = self.country_id
        task_id = self.task_id
        if BackgroundTask.objects.filter(task_id=task_id).exists():
            task = BackgroundTask.objects.get(task_id=task_id)
        else:
            task = BackgroundTask.objects.create(task_id=task_id)

        queryset = Country.objects.filter(id__in=ids)
        task.info(f'{", ".join(map(str, queryset))} validation started')

        for obj in queryset:
            task.info(f'{obj} started')
            if not obj.last_weekly_status.is_verified:
                obj.last_weekly_status.update_country_status_to_joined()
                obj.invalidate_country_related_cache()
                task.info(f'{obj} completed')
            else:
                task.info(f'{obj} already verified')

        task.status = BackgroundTask.STATUSES.completed
        task.completed_at = timezone.now()
        task.save()
        self.assertEqual(task_id, task.task_id)
