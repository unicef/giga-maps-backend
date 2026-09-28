# Giga Meter

Giga Meter is the measurement product (desktop app + Chrome extension) that produces connectivity
readings. It is integrated three ways: a **second database**, a **ping API**, and a **Delta Lake
backup**.

---

## 1. The second database

`GIGA_METER_DATABASE_URL` → alias `gigameter_database`
([config/settings/base.py:470](../../config/settings/base.py#L470)).

Routing is by **app label**, via `GigaMeterDBRouter`
([proco/giga_meter/db_router.py](../../proco/giga_meter/db_router.py)):

```python
def db_for_read(self, model, **hints):
    if model._meta.app_label == app_config.app_name:
        return settings.GIGA_METER_DB_KEY
    return None
```

Every model in `proco.giga_meter` lives there:

| Model | Purpose |
|---|---|
| `GigaMeter_Country` | Countries as Giga Meter knows them |
| `GigaMeter_School` | Schools as Giga Meter knows them |
| `GigaMeter_SchoolMasterData` | Master data pushed from GigaMaps |
| `GigaMeter_SchoolStatic` | Static school attributes |
| `GigaMeter_ConnectivityPingChecks` | Raw ping results |
| `ConnectivityPingChecksDailyAggr` | Daily aggregates |

`allow_migrate` keeps the schemas apart: `giga_meter` migrates only to `gigameter_database`,
everything else only to `default`.

> **Practical consequences**
> - **No cross-database `JOIN`.** A queryset spanning `giga_meter` and `schools` fails. The code
>   works around this by fetching ids into Python — `fetch_all_school_map(giga_ids)`
>   ([proco/giga_meter/tasks.py:547](../../proco/giga_meter/tasks.py#L547)) is the canonical example.
> - **No enforced foreign keys** across the boundary.
> - **Migrations must be run for both aliases.** [web-worker.sh](../../web-worker.sh) runs a plain
>   `manage.py migrate`, which targets `default` only. The Giga Meter schema is migrated separately.
> - Local `docker-compose.local.yml` waits for a database literally named `gigameter` before
>   starting the backend — if it hangs on "DB not ready yet", that database does not exist.

## 2. School master push — 20:30 UTC

`handle_giga_meter_school_master_data_sync`
([proco/giga_meter/tasks.py:415](../../proco/giga_meter/tasks.py#L415)), 10-hour limit. Pushes
school master data **from** GigaMaps **into** the Giga Meter database so measurement clients know
which schools exist.

Backed by `giga_meter_handle_published_school_master_data_row` and
`giga_meter_handle_deleted_school_master_data_row` (each `10 * 55 * 60` ≈ 9 h 10 m).

```bash
pipenv run python manage.py sync_school_master_data_with_giga_meter_db \
  -country_iso3_format='BRA' --force --schedule
```

`giga_meter_update_static_data` (6 h) and `scheduler_for_populate_school_geopoint_field` (4 h) exist
but are **not scheduled** — manual tools only.

## 3. Ping aggregation — 09:15, 15:15, 21:15, 23:15 UTC

`fetch_and_aggregate_ping_data`
([proco/giga_meter/tasks.py:806](../../proco/giga_meter/tasks.py#L806)), 4-hour limit. Pulls
aggregated ping data from the Giga Meter API into `SchoolDailyStatus`.

### The only real retry policy in the codebase

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

When the upstream reports aggregation in progress, the task reschedules itself 15 minutes later
carrying the attempt counter. Other exceptions are **re-raised**, so they reach Sentry — unlike the
swallow-and-warn pattern used elsewhere. A `finally` block always calls `task_on_complete`, so this
task never leaves stale `running` rows.

> The re-queued call passes `force_tasks` and `retry_attempt` but **not `date_str`**. A retry of a
> run targeting a specific historical date silently re-targets *today*. Only relevant when invoking
> manually with `date_str`.

### Gated by a feature flag

```python
if not settings.GIGA_METER_ENABLE_AUTO_SYNC:
    logger.warning('Giga Meter - Ping data sync is disabled from config. ...')
    return
```

It returns **before** creating a `BackgroundTask`, so a disabled sync leaves no row — only a log
line. "No ping data and no task row" means the flag, not a failure.

### Pipeline

`fetch_aggregated_ping_data_from_api` → `fetch_all_school_map` → `stream_school_daily_status` →
`aggregate_ping_rows` → `process_batch` → `bulk_upsert_school_status`, orchestrated by
`run_ping_aggregation`.

```bash
pipenv run python manage.py fetch_giga_meter_ping_data --date='2026-09-17' --schedule
```

## 4. Delta Lake backup — 21:30 UTC

`scheduler_for_backup_giga_meter_connectivity_ping_data`
([proco/giga_meter/tasks.py:896](../../proco/giga_meter/tasks.py#L896)), 4-hour limit. Archives raw
ping data to Azure Delta Lake, then prunes locally.

`AZURE_DELTALAKE_CONFIG['GIGA_METER_PING_BACKUP']`
([config/settings/base.py:592](../../config/settings/base.py#L592)):

| Setting | Env var | Default |
|---|---|---|
| SAS token | `GIGA_METER_PING_BACKUP_AZURE_BLOB_SAS_TOKEN` | `None` |
| Storage account | `GIGA_METER_PING_BACKUP_AZURE_STORAGE_ACCOUNT_NAME` | `saunigiga` |
| Container | `GIGA_METER_PING_BACKUP_AZURE_BLOB_CONTAINER_NAME` | **`giga-dataops-dev`** |
| Lakehouse path | `GIGA_METER_PING_BACKUP_AZURE_LAKEHOUSE_PATH` | `giga_meter/ping` |
| Retention days | `GIGA_METER_PING_BACKUP_RAW_DATA_RETENTION_DAYS` | — |
| Hard-delete grace | `GIGA_METER_PING_BACKUP_HARD_DELETE_GRACE_PERIOD_DAYS` | — |

> **None of these six variables are in `.env_example`, and the container default is a dev
> container.** An unconfigured production environment writes ping backups into `giga-dataops-dev`.
>
> This job also **deletes local data after archiving**. A failed upload followed by a successful
> prune loses data. Verify credentials and container before running it manually.

When `start_date`/`end_date` are omitted, `till_date` is derived from the retention window:

```python
retention_days = settings.AZURE_DELTALAKE_CONFIG['GIGA_METER_PING_BACKUP']['DATA_RETENTION_DAYS']
n_days_old = datetime.now() - timedelta(days=retention_days)
```

```bash
pipenv run python manage.py backup_giga_meter_connectivity_ping_data \
  -start_date='2026-09-01' -end_date='2026-09-15' --schedule
```

## Entity path

`update_entity_live_data_from_giga_meter` at `10:30, 16:30, 22:30` UTC, gated by
`HEALTH_GIGA_METER_ENABLE_AUTO_SYNC` and `ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC` — **both default to
`False`**.

```bash
# Full backfill — fetches ALL data, not a delta
pipenv run python manage.py entity_giga_meter_sync -entity_type='health'
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| No ping data, no `BackgroundTask` row | `GIGA_METER_ENABLE_AUTO_SYNC` false — returns before registering |
| Ping task reschedules repeatedly | Upstream aggregation ongoing; 15-min countdown, bounded by `MAX_RETRIES` |
| Cross-database query errors | A queryset joining `giga_meter` models to core models |
| Backend hangs on startup locally | The `gigameter` database does not exist |
| Backup "succeeded", nothing in Azure | Container/SAS wrong; local prune already ran |
| Entity Giga Meter data absent | Both entity flags default to `False` |
| Giga Meter tables missing after deploy | `migrate` was run for `default` only |
