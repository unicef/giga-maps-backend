# Background task console

`/admin/background-task` — watching scheduled jobs and, crucially, clearing stuck ones.

| Endpoint | Methods | Permission |
|---|---|---|
| `/api/background/backgroundtask/` | `GET`, `DELETE` | `can_view_background_task`, `can_delete_background_task` |
| `/api/background/backgroundtask/{task_id}/` | `GET` | `can_view_background_task` |
| `/api/background/backgroundtask/{task_id}/history` | `GET` | `can_view_background_task` |

---

## The model

`BackgroundTask` ([proco/background/models.py:14](../../proco/background/models.py#L14)):

| Field | Purpose |
|---|---|
| `task_id` | Celery task id — **the primary key** |
| `name` | Unique key, including a time bucket |
| `description` | Human-readable label |
| `status` | `running` / `completed` |
| `log` | Text accumulated by `task_instance.info(...)` |
| `created_at`, `completed_at` | Timing |
| `deleted`, `deleted_by` | Soft delete |

> **There is no `failed` status.** `STATUSES = Choices(('running', …), ('completed', …))`. A task
> that crashes stays `running` indefinitely, indistinguishable from one still working. The only
> signal is elapsed time against the task's declared limit — which is why
> [../05-background-jobs/README.md](../05-background-jobs/README.md#time-limits) lists them.

## Why this console matters

Task names double as **locks**. `task_on_start`
([proco/background/utils.py:7](../../proco/background/utils.py#L7)) returns `None` — and the task
silently no-ops — when a row with the same `name` already exists:

```python
task = BackgroundTask.objects.filter(name=unique_name).first()
if task:
    return None
```

So a worker killed mid-task leaves a `running` row that **blocks future runs** whose key lands in
the same time bucket. Deleting that row is how you unblock the job, and this console is the
supported way to do it.

Most keys bucket to the hour (`%d%m%Y_%H`), so a stale row blocks the rest of that hour. Some bucket
to the day (`email_reminder_…`) — a stale row there blocks for 24 hours.

## Finding stuck tasks

Filter for `status=running` and sort by `created_at`. Anything older than its task's time limit is
stale.

From a shell:

```bash
pipenv run python manage.py shell -c "
from proco.background.models import BackgroundTask
from django.utils import timezone
from datetime import timedelta
cutoff = timezone.now() - timedelta(hours=12)
for t in BackgroundTask.objects.filter(status='running', created_at__lt=cutoff):
    print(t.created_at, t.name, '|', t.description)
"
```

12 hours is a reasonable cutoff — it is longer than the longest declared limit (10 h).

## Clearing a stuck task

Through the console (needs `can_delete_background_task`), or:

```bash
pipenv run python manage.py shell -c "
from proco.background.models import BackgroundTask
from proco.core.utils import get_current_datetime_object
BackgroundTask.objects.filter(
    status='running', name__startswith='update_static_data_status_'
).update(deleted=get_current_datetime_object())
"
```

This is a **soft delete**, and `BaseManager` filters `deleted__isnull=True`, so `task_on_start` no
longer sees the row. Then re-dispatch the task.

> Before deleting, make sure the task really is dead — check Flower or the worker process. Deleting
> the row of a **running** task lets a second copy start, and several of these jobs are not safe to
> run twice concurrently (the master-data publish handler, the cleanup jobs).

## Reading progress

`BackgroundTask.log` accumulates text as a job runs. For most jobs it is the only progress signal:

| Job | Logs |
|---|---|
| `handle_published_school_master_data_row` | `Total published records to update: N` |
| `cleanup_school_master_rows` | `Deleted duplicate rows for same School GIGA ID chunked by country` |
| `email_reminder_…` | Which reviewer group was notified, or **why nothing was sent** |
| `fetch_and_aggregate_ping_data` | Retry attempts and completion |

The reminder task is the best-behaved: it writes `ERROR: School Master data source email
notification is disabled.` or `ERROR: MailJet creds are not configured…` into the log, so the
console tells you why no emails went out.

## A completed task does not mean the work is done

`update_live_data` and `update_qos_data` build a Celery chain and return immediately:

```python
chain(load_data_from_daily_check_app_api.s(), load_data_from_qos_apis.s(), chord(...)).delay()
background_task_utilities.task_on_complete(task_instance)
```

> The `BackgroundTask` row reads `completed` seconds after the job starts, while the actual
> ingestion runs for an hour or more in child tasks. **Do not use this console to confirm live data
> ingestion succeeded** — use Flower, or the child tasks' own rows.

## Table growth

One row per job run, soft-deleted at most. With 25 scheduled jobs, several running 4× daily, the
table grows steadily and is **never pruned by any cleanup job**. Worth a periodic manual archive.

## Related

- [../05-background-jobs/README.md](../05-background-jobs/README.md)
- [../08-operations/runbooks.md](../08-operations/runbooks.md#2-a-scheduled-job-will-not-start)
- [../08-operations/monitoring.md](../08-operations/monitoring.md)
