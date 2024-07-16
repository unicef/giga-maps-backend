from datetime import timedelta

from proco.background.models import BackgroundTask
from proco.core.utils import get_current_datetime_object


def task_on_start(task_id, unique_name, description, check_previous=False):
    try:
        task = BackgroundTask.objects.filter(name=unique_name).first()
        if task:
            return
        else:
            if check_previous and BackgroundTask.objects.filter(
                description=description,
                created_at__gte=get_current_datetime_object() - timedelta(hours=12),
                status=BackgroundTask.STATUSES.running,
            ).exists():
                return
            else:
                task = BackgroundTask.objects.create(
                    task_id=task_id,
                    name=unique_name,
                    description=description,
                    created_at=get_current_datetime_object(),
                    status=BackgroundTask.STATUSES.running,
                )
                return task
    except:
        return


def task_on_complete(task):
    task.status = BackgroundTask.STATUSES.completed
    task.completed_at = get_current_datetime_object()
    task.save()
