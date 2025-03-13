import datetime

from factory import django as django_factory
from factory import fuzzy

from proco.background import models as background_models


class BackgroundTaskFactory(django_factory.DjangoModelFactory):
    task_id = fuzzy.FuzzyText(length=20)
    name = fuzzy.FuzzyText(length=20)
    description = fuzzy.FuzzyText(length=20)
    status = fuzzy.FuzzyChoice(background_models.BackgroundTask.STATUSES)

    created_at = fuzzy.FuzzyDateTime(datetime.datetime(year=1970, month=1, day=1, tzinfo=datetime.timezone.utc))
    completed_at = fuzzy.FuzzyDateTime(datetime.datetime(year=1970, month=1, day=1, tzinfo=datetime.timezone.utc))

    class Meta:
        model = background_models.BackgroundTask
