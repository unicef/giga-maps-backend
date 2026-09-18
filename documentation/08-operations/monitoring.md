# Monitoring and observability

Five independent mechanisms, none of which alert on the most common failure mode.

---

## What exists

| Concern | Mechanism | Enabled by |
|---|---|---|
| HTTP + DB metrics | `django_prometheus` at `/` | `ENABLED_BACKEND_PROMETHEUS_METRICS` |
| Database metrics | instrumented backends | `ENABLED_DB_METRICS` |
| Task monitoring | Flower | `ENABLED_FLOWER_METRICS` |
| Exceptions | Sentry | `SENTRY_DSN`, `CELERY_SENTRY_DSN` |
| APM | New Relic | agent installed |
| Job progress | `BackgroundTask` model | always |
| Data-change alerts | Slack webhook | `SCHOOL_MASTER_DATA_CHANGES_SLACK_WEBHOOK_URL` |
| Logs | `gigamaps.*` logger tree | `GIGAMAPS_LOG_LEVEL` |

## Logging

[config/settings/base.py:482](../../config/settings/base.py#L482).

### Loggers

| Logger | Level | Handlers |
|---|---|---|
| `gigamaps` | `GIGAMAPS_LOG_LEVEL` (default `INFO`) | console |
| `django.request` | `ERROR` | console + file |
| `django.server` | `ERROR` | console + file |

Every module logs as `gigamaps.<module path>`:

```python
logger = logging.getLogger('gigamaps.' + __name__)
```

So `GIGAMAPS_LOG_LEVEL` is a single dial over all application logging.

### Handlers

**console** — `stderr`, with a `HostInfoFilter` adding hostname and IP:

```
%(hostname)s %(hostip)s %(asctime)s %(levelname)s %(pathname)s: %(message)s
```

**file** — `django_errors.log`, `ERROR` only, rotated at midnight, 14 days retained, in a
multi-line human-readable format. Note this handler is attached only to `django.request` and
`django.server`, **not** to `gigamaps` — application errors go to stderr only and are not
written to that file.

### Log lines worth knowing

| Pattern | Meaning |
|---|---|
| `Exception caught for "<ISO3>"` | A country failed during ingestion — **silent data gap** |
| `Failed to get <X> share` | Delta Share unreachable or token expired |
| `Found running Job with "<key>" name so skipping` | `BackgroundTask` guard blocked a run |
| `Using Read-Only DB for request:` | GET routed to the replica |
| `CHECK:: Using Write DB for GET request:` | GET **not** routed to the replica |
| `Slack notifications webhook url not provided.` | Webhook unset |
| `Concurrent PostGIS extension creation detected` | Two processes starting at once — benign |
| `Giga Meter - Ping data sync is disabled from config.` | Feature flag off |

The two DB-routing lines log on **every GET** at INFO, which is verbose in production but is the
fastest way to confirm replica routing.

## Prometheus

`django_prometheus` URLs are mounted **at the root**
([config/urls.py:63](../../config/urls.py#L63)):

```python
if settings.ENABLED_BACKEND_PROMETHEUS_METRICS:
    urlpatterns += [path('', include('django_prometheus.urls'))]
```

Because gunicorn runs 8 workers, metrics are aggregated through
`config.prometheus_multiproc`, which sets up the multiprocess collector directory before starting
gunicorn. Without that wrapper, each worker would report only its own counters.

> Mounting at `''` means the metrics endpoint is served from the same host and path space as the
> API, with **no authentication**. Confirm it is blocked at the ingress, or scraped over a private
> network.

## Flower

Started inside the worker container when `ENABLED_FLOWER_METRICS` is true:

```bash
pipenv run celery --app=proco.taskapp flower >> "$LOG_DIR/flower.log" 2>&1
```

Configured by `FLOWER_BASIC_AUTH`, `FLOWER_PORT`, `FLOWER_DEBUG`.

> [docker/docker-compose.local.yml](../../docker/docker-compose.local.yml) contains a **hard-coded
> `FLOWER_BASIC_AUTH` credential pair** in the commented-out worker service. Do not reuse it, and do
> not enable Flower on anything internet-reachable with those credentials.

Flower is the only place to see live task state — `BackgroundTask` records start and completion, not
queue depth or worker health.

## Sentry

Two DSNs, so web and worker errors can be separated: `SENTRY_DSN` and `CELERY_SENTRY_DSN`.
`APP_ENVIRONMENT` tags events.

`/sentry-debug/` deliberately raises `ZeroDivisionError` to verify wiring:

```bash
curl https://<host>/sentry-debug/
```

> **What does not reach Sentry** is the important part. Ingestion tasks catch per-country exceptions
> and log at `warning`; `task_on_start` swallows all exceptions with a bare `except`. The most common
> real failure — one country silently ceasing to update — produces no Sentry event.
>
> The exceptions that *do* reach Sentry: `fetch_and_aggregate_ping_data` re-raises non-retryable
> errors, and `SlackNotificationService` re-raises send failures.

## `BackgroundTask`

[proco/background/models.py:14](../../proco/background/models.py#L14). The application's own job
registry.

| Field | Purpose |
|---|---|
| `task_id` | Celery task id (primary key) |
| `name` | Unique key including a time bucket |
| `description` | Human-readable |
| `status` | `running` / `completed` — **there is no `failed`** |
| `log` | Text accumulated via `task_instance.info(...)` |
| `created_at`, `completed_at` | Timing |

Exposed at `/api/background/backgroundtask/` and `/admin/background-task`.

> **There is no failure state.** A task that crashes stays `running` forever, indistinguishable from
> one still working. The only way to tell is elapsed time against the task's declared limit.
>
> Stale `running` rows also **block future runs** whose key lands in the same time bucket. Clearing
> them is a routine operation — [runbooks.md](runbooks.md#2-a-scheduled-job-will-not-start).

Useful query:

```bash
pipenv run python manage.py shell -c "
from proco.background.models import BackgroundTask
from datetime import timedelta
from django.utils import timezone
cutoff = timezone.now() - timedelta(hours=12)
for t in BackgroundTask.objects.filter(status='running', created_at__lt=cutoff):
    print(t.created_at, t.name)
"
```

## Health checks

| Endpoint | Returns |
|---|---|
| `/health/` | `OK` — a bare `APIView`, **no database or Redis check** |
| `/test/` | All request headers as JSON |

> `/health/` does not touch the database, Redis, or the replica. It confirms the process is up and
> nothing else — a container with a dead database will report healthy. And `/test/` echoes every
> request header, including `Authorization`, to any caller, registered unconditionally rather than
> behind `DEBUG`. Both are worth addressing at the ingress.

## Suggested alerts

Nothing in the repository configures alerting. Based on the failure modes documented here, the
highest-value alerts would be:

| Priority | Alert | Signal |
|---|---|---|
| 1 | Country ingestion failing | `Exception caught for` in worker logs |
| 2 | Stale `running` task | `BackgroundTask` `running` beyond its time limit |
| 3 | Replica down | Tile endpoints returning `204` |
| 4 | Delta Share token expiring | `SCHOOL_MASTER_EXPIRATION_TIME` / `QOS_EXPIRATION_TIME` |
| 5 | Search index missing | `rebuild_unified_index` without a `completed_at` |
| 6 | Cache warmer overrun | `update_all_entity_cached_values` beyond 2 h |
| 7 | Staging table growth | `data_sources_schoolmasterdata` row count trending up |

## Related

- [runbooks.md](runbooks.md)
- [caching-and-performance.md](caching-and-performance.md)
- [../05-background-jobs/celery-architecture.md](../05-background-jobs/celery-architecture.md)
