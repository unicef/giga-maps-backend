# Cleanup and retention

What gets deleted, when, and how permanently. **Read this before planning any recovery** — it
determines what is still recoverable.

| Schedule (UTC) | Task | Target |
|---|---|---|
| `01:40, 15:40` | `cleanup_school_master_rows` | Staging duplicates |
| `01:45, 15:45` | `cleanup_health_entity_master_rows` | Staging duplicates |
| `05:10` | `clean_old_live_data` | Raw measurements |
| `Sat & Sun 05:20` | `clean_historic_data` | `django-simple-history` rows |

---

## Retention summary

| Data | Window | Deletion type |
|---|---|---|
| `RealTimeConnectivity` | 30 days | Hard |
| `DailyCheckAppMeasurementData` | 30 days | Hard |
| `QoSData` | **Latest version per country only** | Hard |
| `EntityRealTimeConnectivity` | **Latest version per country only** | Hard |
| `SchoolMasterData` duplicates | Newest per `school_id_giga` per status group | **Hard, raw SQL** |
| `HealthEntityMasterIntermediateData` duplicates | Same | **Hard, raw SQL** |
| Historical (`django-simple-history`) rows | Pruned weekly | Hard |
| `SchoolDailyStatus` / `SchoolWeeklyStatus` | **Never pruned** | — |
| `EntityDailyStatus` / `EntityWeeklyStatus` | **Never pruned** | — |
| `CountryDailyStatus` / `CountryWeeklyStatus` | **Never pruned** | — |
| `School` / `Entity` | Soft delete only | Soft |

**The practical rule:** aggregated data is permanent, raw data is not. QoS raw data effectively
survives less than a day.

## `clean_old_live_data` — 05:10 daily

[proco/data_sources/tasks.py:1099](../../proco/data_sources/tasks.py#L1099).

```python
older_then_date = current_datetime - timedelta(days=30)

RealTimeConnectivity.objects.filter(created__lt=older_then_date).delete()
DailyCheckAppMeasurementData.objects.filter(created_at__lt=older_then_date).delete()

qos_latest_ids = list(QoSData.objects.filter(version__isnull=False)
    .order_by('country_id', '-version').distinct('country_id').values_list('id', flat=True))
QoSData.objects.exclude(id__in=qos_latest_ids).delete()
```

Note the field names differ — `created` on one model, `created_at` on the other. Both are correct
for their model.

> **The `QoSData` rule is not a 30-day window**, despite the task name and the surrounding code.
> It keeps exactly **one row per country** — the highest `version` — and deletes everything else
> unconditionally, including rows ingested minutes earlier with a lower version.
>
> Consequences:
> - QoS history must be read from `SchoolDailyStatus`, never `QoSData`.
> - Any recovery that needs raw QoS rows has **until the next 05:10**, then must re-pull from the
>   Delta Share.
> - `.distinct('country_id')` requires PostgreSQL — this is not portable, which is fine here.
>
> `EntityRealTimeConnectivity` follows the same max-version-per-country rule, grouped by
> `entity__country_id`.

## Master-data de-duplication — 01:40 / 15:40

[proco/data_sources/tasks.py:822](../../proco/data_sources/tasks.py#L822). Three raw-SQL passes
per country, each keeping only the newest row per `school_id_giga`:

| Pass | Ordered by | Applies to statuses |
|---|---|---|
| 1 | `created DESC` | `DRAFT`, `UPDATED_IN_DRAFT`, `DRAFT_LOCKED`, `UPDATED_IN_DRAFT_LOCKED`, `DELETED`, `DELETED_PUBLISHED`, `DISCARDED` |
| 2 | `published_at DESC` | `PUBLISHED` |
| 3 | `created DESC` | `is_read=True AND status != 'PUBLISHED'` |

```sql
DELETE FROM data_sources_schoolmasterdata
WHERE country_id = %s AND id IN (
    SELECT id FROM (
        SELECT id, ROW_NUMBER() OVER (PARTITION BY school_id_giga ORDER BY created DESC) AS rn
        FROM data_sources_schoolmasterdata
        WHERE country_id = %s AND status IN (...)
    ) t WHERE t.rn > 1
)
```

> These are **hard `DELETE`s executed outside the ORM**. They bypass:
> - the soft-delete convention (`deleted` timestamp)
> - `django-simple-history` (no historical row is written for the deletion)
> - the `BaseManager` and any model-level `delete()` override
>
> Superseded review rows are gone permanently. If you need to know what a reviewer saw before
> publishing, the `historical*` table is the only record — and `clean_historic_data` prunes that too.
>
> On the positive side, the country id **is** parameterised (`%s`), so this is not an injection
> surface.

## Publish-time deletion

Separately from the scheduled cleanup, **publishing a row hard-deletes its siblings**
([proco/data_sources/serializers.py:404](../../proco/data_sources/serializers.py#L404)):

```python
def delete_all_related_rows(self, instance):
    SchoolMasterData.objects.filter(
        school_id_giga=instance.school_id_giga,
    ).exclude(pk=instance.id).delete()
```

Filtered on `school_id_giga` alone — **not scoped by country**. See
[../04-admin-flows/school-master-review-publish.md](../04-admin-flows/school-master-review-publish.md).

## Historical rows — weekends 05:20

[proco/data_sources/tasks.py:1255](../../proco/data_sources/tasks.py#L1255):

```python
call_command('data_source_additional_steps', '--clean_school_master_historical_rows')
call_command('data_source_additional_steps', '--clean_health_entity_master_historical_rows')
```

`day_of_week='0,6'` is Sunday and Saturday.

`django-simple-history` writes a row on every change to master-data models. Without pruning, the
`historical*` tables outgrow the tables they shadow. This job is what keeps that in check — and it
is also what removes the last copy of data the raw-SQL cleanup already deleted.

> This task uses a **second-granularity** task key (`%d%m%Y_%H%M%S`), so the `BackgroundTask` guard
> is effectively disabled for it. Two runs a second apart would both proceed.

## Manual cleanup

```bash
# Run the scheduled cleanups now
pipenv run python manage.py data_cleanup --cleanup_school_master_rows
pipenv run python manage.py data_cleanup --cleanup_health_entity_master_rows
pipenv run python manage.py data_cleanup --cleanup_qos_data_rows

# Duplicate statistics — snapshot the database first, there is no dry run
pipenv run python manage.py data_cleanup --clean_duplicate_school_weekly -country_id=5
pipenv run python manage.py data_cleanup --clean_duplicate_schools -country_id=5

# Historical rows
pipenv run python manage.py data_source_additional_steps --clean_school_master_historical_rows
```

## Before you delete anything

1. **Snapshot the database.** No cleanup command has a dry-run mode.
2. **Scope by country.** Every relevant command takes `-country_id`. Global runs are rarely needed.
3. **Check what recovery depends on it.** If you are mid-incident, the raw data you are about to
   prune may be the only route back.
4. **Remember what is already gone.** QoS raw rows older than the latest version were deleted at the
   last 05:10.

## Table growth to watch

| Table | Why it grows |
|---|---|
| `historical*` | One row per change; pruned only at weekends |
| `SchoolDailyStatus` / `EntityDailyStatus` | Never pruned — one row per entity per day |
| `SchoolWeeklyStatus` / `EntityWeeklyStatus` | Never pruned |
| `BackgroundTask` | One row per job run, soft-deleted at most |
| `data_sources_schoolmasterdata` | Bounded by the twice-daily cleanup — if it grows, that job is failing |
