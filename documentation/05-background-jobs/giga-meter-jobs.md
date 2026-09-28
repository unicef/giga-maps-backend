# Giga Meter jobs

Giga Meter is the measurement product (desktop app + Chrome extension) that produces connectivity
readings. It has its **own PostgreSQL database**, exposed here as the `gigameter_database` alias.
These jobs move data in both directions and back it up to Azure Delta Lake.

**Jobs covered**

| Schedule (UTC) | Task | Limit |
|---|---|---|
| `20:30` | `handle_giga_meter_school_master_data_sync` | 10 h |
| `09:15, 15:15, 21:15, 23:15` | `fetch_and_aggregate_ping_data` | 4 h |
| `21:30` | `scheduler_for_backup_giga_meter_connectivity_ping_data` | 4 h |

---

## Cross-database routing

`GigaMeterDBRouter` ([proco/giga_meter/db_router.py](../../proco/giga_meter/db_router.py)) routes
**by app label**, not by request:

```python
def db_for_read(self, model, **hints):
    if model._meta.app_label == app_config.app_name:
        return settings.GIGA_METER_DB_KEY
    return None
```

So every model in `proco.giga_meter` — `GigaMeter_Country`, `GigaMeter_School`,
`GigaMeter_SchoolMasterData`, `GigaMeter_SchoolStatic`, `GigaMeter_ConnectivityPingChecks`,
`ConnectivityPingChecksDailyAggr` — reads and writes the other database, always.

`allow_migrate` keeps the two schemas apart:

```python
if app_label == app_config.app_name:
    return db == settings.GIGA_METER_DB_KEY
return db == 'default'
```

> **Consequences to keep in mind:**
> - You cannot `JOIN` between a `giga_meter` model and a `schools`/`entities` model. Cross-database
>   work is done by fetching ids into Python and querying each side separately — which is why
>   `fetch_all_school_map(giga_ids)` exists ([proco/giga_meter/tasks.py:547](../../proco/giga_meter/tasks.py#L547)).
> - Foreign keys across the boundary are not enforced by either database.
> - `migrate` must be run for both aliases. `web-worker.sh` runs a plain `manage.py migrate`, which
>   targets `default` only — the Giga Meter schema is migrated separately.

## 1. School master sync — 20:30

`handle_giga_meter_school_master_data_sync`
([proco/giga_meter/tasks.py:415](../../proco/giga_meter/tasks.py#L415)) pushes school master data
**from** GigaMaps **into** the Giga Meter database, so the measurement clients know which schools
exist.

The heavy lifting is in `giga_meter_handle_published_school_master_data_row` and
`giga_meter_handle_deleted_school_master_data_row` (both `10 * 55 * 60` ≈ 9 h 10 m), mirroring the
main publish/delete handlers against `GigaMeter_SchoolMasterData`.

Manual equivalent:

```bash
pipenv run python manage.py sync_school_master_data_with_giga_meter_db \
  -country_iso3_format='BRA' --force --schedule
```

`giga_meter_update_static_data` (6 h) and `scheduler_for_populate_school_geopoint_field` (4 h) are
defined but **not scheduled** — they are manual tools.

## 2. Ping aggregation — 09:15, 15:15, 21:15, 23:15

`fetch_and_aggregate_ping_data` ([proco/giga_meter/tasks.py:806](../../proco/giga_meter/tasks.py#L806)).
The most carefully written task in the codebase, and the only one with a real retry policy.

### Gated by a feature flag

```python
if not settings.GIGA_METER_ENABLE_AUTO_SYNC:
    logger.warning('Giga Meter - Ping data sync is disabled from config. ...')
    return
```

Default is `True`. It returns **before** creating a `BackgroundTask`, so a disabled sync leaves no
trace in the task table — only a log line.

### Task-key granularity depends on `force_tasks`

```python
frmt="%d%m%Y_%H%M%S" if force_tasks else "%d%m%Y_%H%M"
```

Second-granularity when forced, so a manual run is never blocked by the scheduled one.

### Retry on `AggregationOngoingException`

The only genuine retry loop in the project:

```python
except AggregationOngoingException:
    next_attempt = retry_attempt + 1
    if next_attempt > MAX_RETRIES:
        task_instance.info("Maximum retries reached. Task will not be rescheduled.")
        return
    fetch_and_aggregate_ping_data.apply_async(
        kwargs={"force_tasks": force_tasks, "retry_attempt": next_attempt},
        countdown=15 * 60,
    )
    return
```

When the upstream API reports aggregation still in progress, it reschedules itself 15 minutes later,
carrying the attempt counter. Other exceptions are logged and **re-raised**, so they reach Sentry —
unlike the swallow-and-warn pattern used elsewhere.

> The re-queued call passes `force_tasks` and `retry_attempt` but **not** `date_str`. A retry of a
> run that targeted a specific historical date will silently re-target *today*. Only relevant when
> invoking manually with `date_str`; the scheduled path passes nothing.

The `finally` block always calls `task_on_complete`, so this task does not leave stale `running`
rows on failure.

### Four runs a day, at odd times

`09:15, 15:15, 21:15, 23:15` — six hours apart except the last, two hours after. The 23:15 run
catches late-arriving measurements before the day closes.

### The pipeline

| Function | Role |
|---|---|
| `fetch_aggregated_ping_data_from_api` | Page through the upstream API |
| `fetch_all_school_map(giga_ids)` | Resolve Giga IDs → `School` rows across the DB boundary |
| `stream_school_daily_status` | Stream existing daily rows |
| `aggregate_ping_rows` | Combine measurements |
| `process_batch` / `bulk_upsert_school_status` | Bulk upsert into `SchoolDailyStatus` |
| `run_ping_aggregation` | Orchestrates the above |

Manual equivalent:

```bash
pipenv run python manage.py fetch_giga_meter_ping_data --date='2026-09-17' --schedule
```

## 3. Delta Lake backup — 21:30

`scheduler_for_backup_giga_meter_connectivity_ping_data`
([proco/giga_meter/tasks.py:896](../../proco/giga_meter/tasks.py#L896)) archives raw ping data to
Azure Delta Lake, then prunes locally.

Configuration — `AZURE_DELTALAKE_CONFIG['GIGA_METER_PING_BACKUP']`
([config/settings/base.py:592](../../config/settings/base.py#L592)):

| Setting | Default |
|---|---|
| `AZURE_SAS_TOKEN` | `None` |
| `AZURE_STORAGE_ACCOUNT_NAME` | `saunigiga` |
| `AZURE_BLOB_CONTAINER_NAME` | **`giga-dataops-dev`** |
| `DELTA_LAKE_PATH` | `giga_meter/ping` |
| `DATA_RETENTION_DAYS` | from `GIGA_METER_PING_BACKUP_RAW_DATA_RETENTION_DAYS` |
| Hard-delete grace | from `GIGA_METER_PING_BACKUP_HARD_DELETE_GRACE_PERIOD_DAYS` |

> The container default is **`giga-dataops-dev`**. Production must override
> `GIGA_METER_PING_BACKUP_AZURE_BLOB_CONTAINER_NAME`, or production ping data is written into a dev
> container — and none of these variables are in `.env_example`.

When `start_date`/`end_date` are omitted the task derives `till_date` from the retention window:

```python
retention_days = settings.AZURE_DELTALAKE_CONFIG['GIGA_METER_PING_BACKUP']['DATA_RETENTION_DAYS']
n_days_old = datetime.now() - timedelta(days=retention_days)
```

Manual equivalent:

```bash
pipenv run python manage.py backup_giga_meter_connectivity_ping_data \
  -start_date='2026-09-01' -end_date='2026-09-15' --schedule
```

> This job **deletes local data after archiving**. Confirm the Azure credentials and container are
> correct before running it manually — a failed upload followed by a successful prune loses data.

## Entity equivalent

`update_entity_live_data_from_giga_meter` at `10:30, 16:30, 22:30` is the entity-side path, gated by
`HEALTH_GIGA_METER_ENABLE_AUTO_SYNC` and `ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC` — **both default to
`False`**. Full backfill:

```bash
pipenv run python manage.py entity_giga_meter_sync -entity_type='health'
```

That command fetches **all** data, not a delta.

## Failure modes

| Symptom | Cause |
|---|---|
| No ping data, no `BackgroundTask` row | `GIGA_METER_ENABLE_AUTO_SYNC` is false — the task returns before registering |
| Ping task keeps rescheduling | Upstream aggregation ongoing; check `MAX_RETRIES` and the 15-min countdown |
| Cross-database query errors | A queryset joining `giga_meter` models to core models — not possible |
| Backup succeeds, data missing in Azure | Container/SAS misconfigured; local prune already ran |
| Entity Giga Meter data absent | Both entity flags default to `False` |
