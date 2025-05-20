from django.core.cache import cache
from django.test import TestCase

from proco.background import tasks as bg_tasks
from proco.background.models import BackgroundTask
from proco.background.tests.factories import BackgroundTaskFactory
from proco.locations.tests.factories import CountryFactory
from proco.background import utils as bg_utilities


class BackgroundCeleryTaskTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()
        cls.task = BackgroundTaskFactory(status=BackgroundTask.STATUSES.running)
        cls.task_two = BackgroundTaskFactory(status=BackgroundTask.STATUSES.running)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_reset_countries_data(self):
        old_count = BackgroundTask.objects.all().count()
        ids = [self.country.id,]
        bg_tasks.reset_countries_data(ids)

        self.assertGreater(BackgroundTask.objects.all().count(), old_count)

    def test_validate_countries(self):
        old_count = BackgroundTask.objects.all().count()
        ids = [self.country.id, ]
        bg_tasks.validate_countries(ids)

        self.assertGreater(BackgroundTask.objects.all().count(), old_count)

    def test_task_on_start_new(self):
        old_count = BackgroundTask.objects.all().count()
        new_task = bg_utilities.task_on_start('task_id_11111', 'U Name 11111', 'Description')
        self.assertGreater(BackgroundTask.objects.all().count(), old_count)
        self.assertIsNotNone(new_task)

    def test_task_on_start_existing(self):
        old_count = BackgroundTask.objects.all().count()
        new_task = bg_utilities.task_on_start('task_id_2222', self.task.name, 'Description')
        self.assertEquals(BackgroundTask.objects.all().count(), old_count)
        self.assertIsNone(new_task)

    def test_task_on_complete(self):
        self.assertEquals(BackgroundTask.objects.get(task_id=self.task_two.task_id).status,
                          BackgroundTask.STATUSES.running)
        bg_utilities.task_on_complete(self.task_two)
        self.assertEquals(BackgroundTask.objects.get(task_id=self.task_two.task_id).status,
                          BackgroundTask.STATUSES.completed)
