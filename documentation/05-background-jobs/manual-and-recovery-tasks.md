# Manual and recovery tasks

Celery tasks that are **defined but not scheduled** — invoked by hand, by a management command, or
by an API call.

---

## Recovery tasks

| Task | Limit | Triggered by |
|---|---|---|
| `redo_aggregations_task(country_id, year, week_no)` | 10 h | `redo_aggregations` |
| `redo_entity_aggregations_task(country_id, year, week_no, entity_type_code)` | 10 h | `redo_entity_aggregations` |
| `data_loss_recovery_for_pcdc_weekly_task(start_week_no, end_week_no, year, pull_data)` | 10 h | `data_loss_recovery_for_pcdc_weekly` |
| `scheduler_for_data_loss_recovery_for_qos_dates(...)` | 2 h | `data_loss_recovery_for_qos_dates` |
| `populate_school_new_fields_task(start_school_id, end_school_id, country_id, school_ids=None)` | 10 h | `populate_school_new_fields` |

Worked procedures: [../08-operations/runbooks.md](../08-operations/runbooks.md).

## Disabled scheduled tasks

Still callable, just not in the beat schedule:

| Task | Replaced by |
|---|---|
| `update_all_cached_values(clean_cache=False)` | `update_all_entity_cached_values` |
| `rebuild_school_index()` | `rebuild_unified_index` |

Note that `InvalidateCache` **still calls `update_all_cached_values.delay()`**
([proco/accounts/api.py:538](../../proco/accounts/api.py#L538)), so a manual cache invalidation runs
the legacy warmer alongside the entity one.

## Giga Meter tasks without a schedule

| Task | Limit | Manual command |
|---|---|---|
| `giga_meter_update_static_data(country_iso3_format, force_tasks)` | 6 h | — |
| `scheduler_for_populate_school_geopoint_field(country_iso3_format)` | 4 h | `populate_school_geopoint_field` |
| `giga_meter_load_data_from_school_master_apis(country_iso3_format)` | — | — |

## Utility tasks

| Task | Limit | Purpose |
|---|---|---|
| `update_cached_value(url, query_params)` | 10 min | Warm one URL — also queued automatically on a stale cache read |
| `update_country_related_cache(country_code)` | **none** | Refresh one country's cache entries |
| `finalize_task()` | **none** | No-op chord callback |
| `reset_countries_data(ids)` | 30 min | Reset a country's data |
| `validate_countries(ids)` | 30 min | Validate country records |
| `send_email(backend, ...)` | **none** | One email |

`reset_countries_data` and `validate_countries` live in
[proco/background/tasks.py](../../proco/background/tasks.py) and are the only tasks in that app.

> Tasks with **no declared limits** inherit the worker CLI default of 60 s soft / 300 s hard. For
> `finalize_task` and `send_email` that is fine. `update_country_related_cache` is borderline — it
> can touch many keys.

## Invoking a task

```bash
# Async
pipenv run python manage.py shell -c "
from proco.utils.tasks import redo_aggregations_task
redo_aggregations_task.delay(country_id=5, year=2026, week_no=37)"

# Synchronous, for debugging
pipenv run python manage.py shell -c "
from proco.utils.tasks import redo_aggregations_task
redo_aggregations_task(country_id=5, year=2026, week_no=37)"
```

> Called directly, `current_task.request.id` is `None` and the code falls back to `uuid.uuid4()`, so
> the **`BackgroundTask` guard still applies**. If a row for the current time bucket exists, the task
> silently no-ops. Clear it first — see
> [../04-admin-flows/background-task-console.md](../04-admin-flows/background-task-console.md).

## Prefer `--schedule`

Several management commands accept `--schedule`, which dispatches the work as a Celery task instead
of running inline:

`populate_school_geopoint_field` · `sync_school_master_data_with_giga_meter_db` ·
`fetch_giga_meter_ping_data` · `backup_giga_meter_connectivity_ping_data` ·
`data_loss_recovery_for_school_master_version`

On a production host this matters — a 10-hour inline command dies with your SSH session.

## HTTP triggers

| Endpoint | Runs |
|---|---|
| `GET /api/sources/load/static_live/` | Static + live loaders |
| `GET /api/sources/load/school_master/` | School master pull, then publish and delete handlers **synchronously** |
| `GET /api/sources/load/qos/` | QoS loader |
| `GET /api/sources/load/daily_check_app/` | Daily Check App loader |
| `GET /api/accounts/invalidate-cache/` | Invalidation + both cache warmers |

> `load/school_master/` calls the handlers **without `.delay()`**
> ([proco/data_sources/api.py:51](../../proco/data_sources/api.py#L51)). On a large dataset the
> request exceeds gunicorn's 300 s timeout and returns `504` while the work continues in the
> worker process. The `504` is not a failure signal.

## Related

- [../08-operations/runbooks.md](../08-operations/runbooks.md)
- [../08-operations/management-commands.md](../08-operations/management-commands.md)
