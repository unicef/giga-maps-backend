# Celery architecture

## Processes

| Process | Entrypoint | What it runs |
|---|---|---|
| Worker | [celery.sh](../../celery.sh) | `celery --app=proco.taskapp worker --concurrency=3` |
| Beat | [celerybeat.sh](../../celerybeat.sh) | `celery --app=proco.taskapp beat --scheduler=redbeat.RedBeatScheduler` |
| Flower | [celery.sh](../../celery.sh) | Started in the same container when `ENABLED_FLOWER_METRICS` |

Both worker and beat containers also start an SSH daemon and a Flask sidecar
([hello.py](../../hello.py)) on port 8000, used as a health/exec surface.

## Configuration

[proco/taskapp/__init__.py](../../proco/taskapp/__init__.py):

```python
app = Celery('proco')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
app.conf.timezone = 'UTC'
app.conf.broker_transport_options = {"visibility_timeout": 36000}   # 10h
app.conf.worker_deduplicate_successful_tasks = True
app.conf.redbeat_key_prefix = 'gigamaps:'
app.conf.redbeat_lock_timeout = 36000
```

| Setting | Why |
|---|---|
| `timezone = 'UTC'` | All crontabs are UTC. There is no local-time scheduling anywhere |
| `visibility_timeout = 36000` | Matches the longest task (10 h). Without it Redis would redeliver a still-running task |
| `worker_deduplicate_successful_tasks = True` | Drops duplicate deliveries of already-successful tasks |
| `redbeat_key_prefix = 'gigamaps:'` | Namespace for schedule keys in Redis |
| `redbeat_lock_timeout = 36000` | Beat's leader lock, also 10 h |

Autodiscovery finds `tasks.py` in each installed app — six have one: `background`, `data_sources`,
`giga_meter`, `mailing`, `schools`, `utils`.

## Queues

**There are none.** No `task_routes`, no `CELERY_TASK_QUEUES`, no `queue=` arguments anywhere.
Everything goes to the default queue and is picked up by any worker.

> With `--concurrency=3`, three slots serve everything from 10-hour ingestion jobs to
> sub-second cache refreshes. A single long task occupies a third of the worker's capacity for its
> entire duration, and three concurrent long tasks block cache refreshes completely.
>
> Introducing at least a `long`/`default` split would be a contained change: the task decorators
> already encode which is which via their time limits. This is worth considering if cache warming
> is observed lagging behind the 04:45 schedule.

## Broker and results

| Setting | Conventional value |
|---|---|
| `CELERY_BROKER_URL` | `redis://redis:6379/1` |
| `CELERY_RESULT_BACKEND_URL` | `redis://redis:6379/2` |
| Django cache (`REDIS_URL`) | `redis://redis:6379/0` |

Three separate logical databases on the same Redis instance. Flushing the wrong one is an easy
mistake: `FLUSHDB` on db 0 clears the cache, on db 1 clears queued tasks, on db 2 clears results —
and RedBeat's schedule lives under `gigamaps:*` in whichever database the broker URL points at.

## RedBeat

The schedule is **stored in Redis, not the database**. `finalize_setup` runs on
`@app.on_after_finalize.connect` and calls `app.conf.beat_schedule.update({...})`, which RedBeat
persists under `gigamaps:`.

Practical consequences:

- **Removing an entry from the code does not remove it from Redis.** A deleted job can keep firing
  after a deploy. Delete the key explicitly:
  ```bash
  redis-cli --scan --pattern 'gigamaps:*'
  redis-cli DEL 'gigamaps:<entry-name>'
  ```
- **Changing a schedule requires a beat restart** for the new value to be written.
- Flushing the broker database clears the schedule; beat rewrites it on next start.

## Task-level conventions

### Every long task registers itself

```python
task_key = '<name>_status_{current_time}'.format(
    current_time=format_date(get_current_datetime_object(), frmt='%d%m%Y_%H'))
task_id = current_task.request.id or str(uuid.uuid4())
task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Human description')

if task_instance:
    ...
    background_task_utilities.task_on_complete(task_instance)
else:
    logger.info('Found running Job with "{0}" name so skipping current iteration'.format(task_key))
```

The time-bucket granularity varies and matters:

| Format | Bucket | Used by |
|---|---|---|
| `%d%m%Y` | Day | `email_reminder_…` |
| `%d%m%Y_%H` | Hour | Most tasks |
| `%d%m%Y_%H%M` | Minute | `update_all_cached_values` |
| `%d%m%Y_%H%M%S` | Second | `clean_historic_data` |

A second-granularity key effectively disables the guard — two `clean_historic_data` runs a second
apart will both proceed.

### Progress reporting

`task_instance.info(...)` appends to `BackgroundTask.log`, readable through
`/api/background/backgroundtask/<task_id>/` and the admin console. This is the only progress
signal for most jobs.

### Chains, groups and chords

`update_live_data` and `update_qos_data` build workflows rather than doing work inline:

```python
chain(
    load_data_from_daily_check_app_api.s(),
    load_data_from_qos_apis.s(),
    chord(group([finalize_previous_day_data.s(cid, date) for cid in country_ids]),
          finalize_task.si()),
).delay()
```

> The parent task returns immediately after `.delay()` and marks its own `BackgroundTask` complete.
> **A "completed" `BackgroundTask` for `update_live_data` says nothing about whether the ingestion
> succeeded** — the real work is in the chain. Use Flower or the child tasks' own rows.

`finalize_task` is a no-op that exists purely to give the chord a callback.

### Delegating to management commands

Several tasks are thin wrappers:

```python
call_command('build_unified_index', '--delete_index', '--create_index', '--clean_index', '--update_index')
call_command('data_source_additional_steps', '--clean_school_master_historical_rows')
```

So the scheduled behaviour and the manual command are literally the same code path — a task can
always be reproduced from a shell.

## Retries

**No task declares `autoretry_for`, `retry_backoff` or `max_retries`.** A failed task is simply
failed; the next scheduled occurrence is the retry. For a 4-hourly job that means a 4-hour hole; for
a daily job, 24 hours.

Combined with per-country exception swallowing in the ingestion tasks, this means transient upstream
failures produce silent data gaps rather than alerts. Recovery is manual —
[../08-operations/runbooks.md](../08-operations/runbooks.md).

## Time limits

`celery.sh` passes `--time-limit=300 --soft-time-limit=60` on the command line. Almost every task
overrides these in its decorator. Tasks **without** decorator arguments inherit the 60-second soft
limit:

| Task | Risk |
|---|---|
| `finalize_task` | None — it is a no-op |
| `update_country_related_cache` | Low |
| `email_reminder_to_editor_and_publisher_…` | **Real** — four queries plus one email per recipient |
| `send_email` | Low, per message |

## Monitoring

| Tool | Scope |
|---|---|
| Flower | Live task state, when `ENABLED_FLOWER_METRICS` |
| `BackgroundTask` table | Application-level start/complete/log |
| Sentry (`CELERY_SENTRY_DSN`) | Exceptions that escape a task |
| Worker logs | `/code/logs/celeryd-*.log` |

Note that Flower in the committed compose file uses a **hard-coded `FLOWER_BASIC_AUTH` credential
pair**. Do not reuse it.

## Running a task by hand

```bash
pipenv run python manage.py shell -c "
from proco.data_sources.tasks import update_static_data
update_static_data.delay()"
```

Synchronously, for debugging:

```bash
pipenv run python manage.py shell -c "
from proco.data_sources.tasks import update_static_data
update_static_data()"
```

Called directly, `current_task.request.id` is `None` and the code falls back to `uuid.uuid4()`, so
the `BackgroundTask` guard still applies. Clear any conflicting row first.
