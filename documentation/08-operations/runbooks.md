# Runbooks

Worked procedures for the failures that actually happen. Each starts with a **diagnosis** step that
changes nothing.

Command reference: [management-commands.md](management-commands.md).

---

## Before you start

Three facts that shape every recovery here:

1. **Raw QoS rows survive at most one day.** `clean_old_live_data` runs at 05:10 UTC and keeps
   **a single row per country** in `QoSData` — the one with the highest `version` — deleting
   everything else, including rows with `version = NULL`
   ([proco/data_sources/tasks.py:1124](../../proco/data_sources/tasks.py#L1124)). If you need to
   re-derive from raw QoS, you have until the next 05:10.
2. **Raw PCDC and real-time rows survive 30 days.** `DailyCheckAppMeasurementData` and
   `RealTimeConnectivity` are pruned on a 30-day window.
3. **Aggregated data is durable.** `SchoolDailyStatus`, `SchoolWeeklyStatus` and their entity
   equivalents are not pruned by the retention jobs, so re-aggregation is usually possible long
   after the raw data has gone — but only if the raw data was ingested in the first place.

Corollary: **if an ingestion outage is longer than a day, recovering QoS means re-pulling from the
Delta Share, not re-aggregating locally.**

---

## 1. A country's data has stopped updating

### Diagnose

```bash
# Is there a recent master-data row for the country?
pipenv run python manage.py shell -c "
from proco.data_sources.models import SchoolMasterData
from proco.locations.models import Country
c = Country.objects.get(iso3_format='BRA')
qs = SchoolMasterData.objects.filter(country=c)
print('total', qs.count())
print('latest pulled_at', qs.order_by('-pulled_at').values_list('pulled_at', flat=True).first())
print('pending publish', qs.filter(status='PUBLISHED', is_read=False).count())
print('by status', dict(qs.values_list('status').annotate(__import__('django.db.models', fromlist=['Count']).Count('id'))))
"
```

Then check worker logs for a swallowed per-country exception:

```bash
grep 'Exception caught for "BRA"' /code/logs/celeryd-*.log
```

This is the single most common cause. `load_data_from_school_master_apis` catches per-country
exceptions, logs them at **warning**, and reports overall success
([proco/data_sources/tasks.py:174](../../proco/data_sources/tasks.py#L174)).

Also check the exclusion lists:

```bash
grep -E 'COUNTRY_(EXCLUSION|INCLUSION)_LIST' .env
```

### Fix

```bash
# Re-pull one country
pipenv run python manage.py shell -c "
from proco.data_sources.tasks import update_static_data
update_static_data.delay(country_iso3_format='BRA')"
```

If rows are stuck at `PUBLISHED, is_read=False`, the apply job is not running:

```bash
pipenv run python manage.py data_cleanup --handle_published_school_master_data_row -country_id=5
```

---

## 2. A scheduled job will not start

**Symptom**: the job's schedule passes, nothing happens, no errors.

### Diagnose

Almost always the `BackgroundTask` guard. `task_on_start` returns `None` — and the task silently
no-ops — when a row with the same `name` exists
([proco/background/utils.py:7](../../proco/background/utils.py#L7)). A worker killed mid-task never
runs `task_on_complete`, so a `running` row is left behind.

```bash
pipenv run python manage.py shell -c "
from proco.background.models import BackgroundTask
for t in BackgroundTask.objects.filter(status='running').order_by('created_at'):
    print(t.created_at, t.name, t.description)
"
```

Anything `running` for longer than its task's time limit is stale.

### Fix

Soft-delete the stale row — via the admin console (`/admin/background-task`, needs
`can_delete_background_task`) or:

```bash
pipenv run python manage.py shell -c "
from proco.background.models import BackgroundTask
from proco.core.utils import get_current_datetime_object
BackgroundTask.objects.filter(
    status='running', name__startswith='update_static_data_status_'
).update(deleted=get_current_datetime_object())
"
```

Then re-dispatch the task.

### If beat itself is not firing

RedBeat stores the schedule in Redis under `gigamaps:`. A removed or renamed entry can persist
across a deploy:

```bash
redis-cli --scan --pattern 'gigamaps:*'
# remove a stale entry
redis-cli DEL 'gigamaps:<entry-name>'
```

Then restart beat.

---

## 3. Missing PCDC / Daily Check App data

### Diagnose

```bash
pipenv run python manage.py data_loss_recovery_for_pcdc \
  -start_date='2026-09-01' -end_date='2026-09-15' --check_missing_dates
```

Reports gaps without changing anything.

### Fix — by date

```bash
pipenv run python manage.py data_loss_recovery_for_pcdc \
  -start_date='2026-09-01' -end_date='2026-09-15' --pull_data
```

Or a single date:

```bash
pipenv run python manage.py data_loss_recovery_for_pcdc -pull_data_date='2026-09-07' --pull_data
```

### Fix — by ISO week

```bash
pipenv run python manage.py data_loss_recovery_for_pcdc_weekly \
  -start_week_no=36 -end_week_no=38 -year=2026 --pull_data
```

The weekly form dispatches `data_loss_recovery_for_pcdc_weekly_task`, a 10-hour Celery task.

### Then re-aggregate

Pulling raw data does **not** rebuild the rollups:

```bash
pipenv run python manage.py redo_aggregations \
  -country_id=5 -year=2026 -week_no=37 --update_school_weekly --update_country_daily
```

---

## 4. Missing QoS data

Two commands exist — one keyed by Delta Share **version**, one by **date**. They behave differently
enough that picking the wrong one, or the wrong flag combination, can make things worse.

### Diagnose — not with the commands' own checks

Because of constraint 1, both built-in checks are unreliable for anything older than the last 05:10
cleanup:

- `data_loss_recovery_for_qos_dates --check_missing_dates` lists the dates present in `QoSData`.
  After cleanup almost every date looks missing, even when the daily tables are fine.
- `data_loss_recovery_for_qos --check_missing_versions` starts from the lowest version still in
  `QoSData`. With one row left per country it never reports older gaps.

Check the durable tables instead — this is read-only:

```sql
SELECT date, live_data_source, COUNT(*)
FROM connection_statistics_schooldailystatus
WHERE date BETWEEN '2026-09-01' AND '2026-09-20'
GROUP BY 1, 2 ORDER BY 1, 2;
```

A date with no or unusually few `QOS` rows for a QoS country is a gap.

### Fix — by date (the usual case)

```bash
pipenv run python manage.py data_loss_recovery_for_qos_dates \
  -country_code='BRA' -start_date='01-09-2026' -end_date='03-09-2026' \
  --pull_data --aggregate --schedule
```

- **Dates are `DD-MM-YYYY`** (`DATE_FORMAT`, [config/settings/base.py:118](../../config/settings/base.py#L118)).
  `YYYY-MM-DD` parses to `None` and the command fails.
- **Always combine `--pull_data` with `--aggregate`.** `--pull_data` first **deletes** that date's
  QoS rows from `QoSData` and `RealTimeConnectivity`
  ([data_loss_recovery_for_qos_dates.py:401](../../proco/data_sources/management/commands/data_loss_recovery_for_qos_dates.py#L401)),
  so the aggregation starts from a clean date. `--aggregate` on its own inserts one averaged row per
  school and day into `RealTimeConnectivity` **on top of** whatever is already there; if the date
  already had per-measurement rows, or you run it twice, the daily average is skewed.
- **If the pull fails after the delete** (e.g. another `504`), the error is only logged
  ([line 151](../../proco/data_sources/management/commands/data_loss_recovery_for_qos_dates.py#L151))
  and that date is left with no raw QoS rows. Daily and weekly tables are untouched, so the map does
  not lose data, but the pull must be repeated.
- **`--schedule`** runs it as `scheduler_for_data_loss_recovery_for_qos_dates`, a Celery task with a
  **2-hour limit**, so the SSH session can close. Without it, it runs inline. Keep ranges small (one
  country, a few days): the loader upserts row by row, with two queries per row
  ([proco/core/utils.py:270](../../proco/core/utils.py#L270)), and even three days have timed out.
- Without `-country_code` it iterates **every** QoS country — avoid on production.

> **Unverified.** The date filter is passed to Delta Sharing as `jsonPredicateHints`, which the
> server may ignore. If the QoS server does not apply it, each date in the loop downloads the whole
> table, which would also explain the timeouts.

### Fix — by version (narrow case)

Use only when a **specific version** is known to be missing and you recover it **the same day**,
before the next 05:10 cleanup:

```bash
pipenv run python manage.py data_loss_recovery_for_qos \
  -country_code='BRA' -pull_start_version=100 -pull_end_version=102 --pull_data \
  --aggregate -aggregate_start_version=100 -aggregate_end_version=102
```

- Country is mandatory. There is **no `--schedule`**: it runs inline, so keep the SSH session alive.
- `--pull_data` does not delete anything; it skips versions already present and rows for which a
  higher version exists.
- `--aggregate` reads the versions from `QoSData`, so after a cleanup it finds nothing to aggregate
  unless you pulled them first. It has the same stacking problem as the date command: it adds
  averaged rows to `RealTimeConnectivity` without clearing existing ones.

### What neither command covers

- **Countries without QoS.** Both only process QoS countries. If a QoS outage broke the live-data
  chain (see [../05-background-jobs/ingestion-live-data.md](../05-background-jobs/ingestion-live-data.md)),
  Daily Check App / Giga Meter aggregation for **non-QoS countries** also has to be re-run, for
  example with `finalize_previous_day_data` per country and date or with `redo_aggregations` (§6).
  Their raw rows in `RealTimeConnectivity` are only kept for 30 days.
- **Side effect worth knowing:** re-aggregating a QoS country recomputes the daily status for
  **all** sources of that country and date, so its Giga Meter daily rows are rebuilt too.

---

## 5. A bad School Master version was published

### Diagnose

```bash
pipenv run python manage.py data_loss_recovery_for_school_master_version \
  -country_code='BRA' --check_latest_version
```

### Fix

```bash
pipenv run python manage.py data_loss_recovery_for_school_master_version \
  -country_code='BRA' -pull_version=42 --pull_data --schedule
```

`--schedule` runs it as a Celery task rather than inline — use it on production.

> This re-pulls a version into the **staging** table as `DRAFT`. It does not roll back already-applied
> changes to `School`. Reverting applied data means re-publishing the correct version through the
> normal review flow, which is a human step.

---

## 6. Aggregations look wrong

Symptoms: pie-chart counts that do not match the school list, `schools_connected` inconsistent with
per-school statuses.

### Re-aggregate a specific week

```bash
pipenv run python manage.py redo_aggregations \
  -country_id=5 -year=2026 -week_no=37 --update_school_weekly --update_country_daily
```

Entity equivalent:

```bash
pipenv run python manage.py redo_entity_aggregations \
  -country_id=5 -entity_type_code='health' -year=2026 -week_no=37 --update_entity_weekly
```

Both dispatch 10-hour Celery tasks.

### Rebuild denormalised school fields

`School.connectivity_status`, `coverage_status` and `coverage_type` are denormalised copies,
normally refreshed by `update_school_records` at 01:00:

```bash
pipenv run python manage.py populate_school_new_fields -country_id=5
```

### Duplicate rows

```bash
# ALWAYS snapshot the database first — there is no dry-run flag
pipenv run python manage.py data_cleanup --clean_duplicate_school_weekly -country_id=5
pipenv run python manage.py data_cleanup --clean_duplicate_country_daily -country_id=5
```

---

## 7. Search returns nothing or stale results

### Diagnose

```bash
grep -E 'SEARCH_ENDPOINT|SEARCH_API_KEY|INDEX_NAME' .env
```

> Watch for the **hyphen/underscore mismatch**: code defaults are `giga_schools` / `giga_entities`
> / `giga_countries`; `.env_example` supplies `giga-schools` / `giga-countries`. A mismatch yields
> an empty search with no error.

Then check whether the nightly rebuild ran:

```bash
pipenv run python manage.py shell -c "
from proco.background.models import BackgroundTask
print(list(BackgroundTask.objects.filter(
    name__startswith='rebuild_unified_index'
).order_by('-created_at').values_list('created_at','status')[:5]))"
```

### Fix

```bash
# Full rebuild
pipenv run python manage.py build_unified_index \
  --delete_index --create_index --clean_index --update_index

# One country
pipenv run python manage.py build_unified_index --update_index -country_id=144
```

---

## 8. Everything is slow after a deploy

Usually cache invalidation. Warming happens at 04:45 UTC, so the first users after a deploy pay
full rebuild costs.

### Check

```bash
redis-cli --scan --pattern 'SOFT_CACHE_*' | head
redis-cli DBSIZE
```

### Fix

Trigger the warmer rather than waiting:

```bash
pipenv run python manage.py shell -c "
from proco.utils.tasks import update_all_entity_cached_values
update_all_entity_cached_values.delay(clean_cache=False)"
```

Pass `clean_cache=False` — you want to *fill* the cache, not clear it again.

> Do not run a hard invalidation during peak hours. `cache_manager.invalidate(hard=True)` issues a
> Redis `KEYS` scan and then deletes, and every subsequent request is a cold miss until the warmer
> completes.

See [caching-and-performance.md](caching-and-performance.md).

---

## 9. The map is empty but the API works

Tile SQL connects to `connections[settings.READ_ONLY_DB_KEY]` **directly**
([proco/schools/api.py:182](../../proco/schools/api.py#L182)). A `DatabaseError` there returns
`204 No Content`, which Mapbox renders as an empty tile.

### Check

```bash
pipenv run python manage.py shell -c "
from django.db import connections
from django.conf import settings
with connections[settings.READ_ONLY_DB_KEY].cursor() as c:
    c.execute('SELECT 1'); print('replica OK', c.fetchone())"
```

If that fails, or is very slow, the replica is the problem — not the application. Check replication
lag and the `statement_timeout` in `READ_ONLY_DATABASE_URL`.

There is no fallback to the primary. Restoring the replica is the only fix short of a code change.

---

## 10. Leftover Delta Share credential files

`load_data_from_school_master_apis` writes a `.share` profile containing a **live bearer token** to
`BASE_DIR` and removes it in a `try/except OSError: pass`. A worker killed in between leaves the
file behind.

```bash
ls -la /code/*_profile_*.share 2>/dev/null
# if any exist:
rm -f /code/*_profile_*.share
```

Worth adding to any post-incident checklist after a worker crash.

---

## 11. Reviewers report published rows still in their queue

Expected, not a bug. `apply_queryset_filters` excludes rows that are finished **and** `is_read=True`
([proco/data_sources/api.py:144](../../proco/data_sources/api.py#L144)). A published row keeps
`is_read=False` until `handle_published_school_master_data_row` runs, which is every 4 hours at
`:27`.

If it has been more than four hours, the apply job is stuck — go to runbook 2.

```bash
pipenv run python manage.py shell -c "
from proco.data_sources.models import SchoolMasterData
print('awaiting apply:', SchoolMasterData.objects.filter(status='PUBLISHED', is_read=False).count())"
```

---

## Escalation checklist

When the cause is not obvious, in order:

1. `BackgroundTask` rows with `status='running'` older than their time limit
2. Worker logs for `Exception caught for "<ISO3>"` and `Found running Job with`
3. Replica health and `statement_timeout`
4. Redis: `DBSIZE`, memory, and whether `gigamaps:*` beat keys are present
5. Delta Share token expiry — `SCHOOL_MASTER_EXPIRATION_TIME`, `QOS_EXPIRATION_TIME`
6. Feature flags: `GIGA_METER_ENABLE_AUTO_SYNC`, `HEALTH_GIGA_METER_ENABLE_AUTO_SYNC`,
   `ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC`, `ENABLED_*_EMAILS`
7. Country exclusion/inclusion lists
