# Entity jobs

The entity pipeline runs in parallel with the school pipeline. Read
[../02-domain-model/entities.md](../02-domain-model/entities.md) first — it explains why there are
two of everything.

**10 scheduled jobs.**

| Schedule (UTC) | Task | School counterpart | Offset |
|---|---|---|---|
| `*/4h :22` | `utils.handle_deleted_entity_master_data_row` | `:17` | +5 min |
| `*/4h :32` | `data_sources.handle_published_entity_master_data_row` | `:27` | +5 min |
| `*/4h :52` | `data_sources.update_entity_static_data` | `:47` | +5 min |
| `01:30` | `utils.update_entity_records` | `01:00` | +30 min |
| `02:00` | `utils.rebuild_unified_index` | replaces `rebuild_school_index` | — |
| `01:45, 15:45` | `data_sources.cleanup_health_entity_master_rows` | `:40` | +5 min |
| `02:55, 08:55, 14:55, 20:55` | `utils.populate_entity_registration_data` | `:50` | +5 min |
| `04:45` | `utils.update_all_entity_cached_values` | replaces `update_all_cached_values` | — |
| `05:00` | `data_sources.update_entity_qos_data` | `04:00` | +1 h |
| `10:30, 16:30, 22:30` | `data_sources.update_entity_live_data_from_giga_meter` | `02:10…` | — |

The five-minute offsets are deliberate — see
[README.md](README.md#the-offset-pattern). They reduce database contention; they do **not**
guarantee the school job has finished, since several run far longer than five minutes.

---

## Feature flags

Two of the three entity flags default to **off**:

| Flag | Default | Gates |
|---|---|---|
| `GIGA_METER_ENABLE_AUTO_SYNC` | `True` | School-side Giga Meter sync |
| `HEALTH_GIGA_METER_ENABLE_AUTO_SYNC` | **`False`** | Health-entity Giga Meter sync |
| `ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC` | **`False`** | Entity live-data sync generally |

**On a default configuration the entity pipeline ingests master data but not live connectivity.**
Check these before investigating missing entity live data.

## Master data — `update_entity_static_data`

[proco/data_sources/tasks.py:1548](../../proco/data_sources/tasks.py#L1548). Same Delta Share
mechanism as School Master, reading the `health-master` schema.

The difference that matters: Health Master has an **inclusion list** as well as an exclusion list.

| Setting | Semantics |
|---|---|
| `HEALTH_MASTER_COUNTRY_INCLUSION_LIST` | Allow-list — only these countries |
| `HEALTH_MASTER_COUNTRY_EXCLUSION_LIST` | Deny-list |
| `SCHOOL_MASTER_COUNTRY_EXCLUSION_LIST` | Deny-list only |

Health ingestion is **opt-in per country**; school ingestion is opt-out. An empty inclusion list is
the most common reason no health data appears. Neither `HEALTH_MASTER_*` variable is in
`.env_example`.

Rows land in `HealthEntityMasterIntermediateData`, which inherits the same
`MasterDataSourceModelMixin` eight-state machine — reviewed and published through the **same admin
screens and the same permission slugs** as schools. There is no separate health permission.

## Publish and delete

`handle_published_entity_master_data_row`
([proco/data_sources/tasks.py:1572](../../proco/data_sources/tasks.py#L1572)) applies published
rows to `Entity` + the type's detail model (`HealthEntity`).

`handle_deleted_entity_master_data_row`
([proco/utils/tasks.py:799](../../proco/utils/tasks.py#L799), ~50-minute limit) soft-deletes
entities removed upstream. Note it lives in `utils.tasks`, not `data_sources.tasks`, unlike its
school counterpart — an inconsistency worth knowing when grepping.

## `update_entity_records` — 01:30, 10-hour limit

[proco/utils/tasks.py:600](../../proco/utils/tasks.py#L600). The entity analogue of
`update_school_records`: recomputes denormalised fields on `Entity`.

It takes optional `start_time` / `end_time` for an incremental window, and a helper
`get_entity_static_connectivity_status(master_row, field_names)`
([proco/utils/tasks.py:564](../../proco/utils/tasks.py#L564)) derives connectivity status from
static master-data fields, with an inner `normalized_value(field_name)` handling the string/boolean
mismatch between master data (`'yes'`/`'no'`) and `Entity`'s real booleans.

**Ten hours is the longest limit in the codebase**, shared with `redo_aggregations_task`. At
`--concurrency=3` a full run occupies a third of the worker pool for its duration.

## `populate_entity_registration_data` — 4× daily

[proco/utils/tasks.py:505](../../proco/utils/tasks.py#L505). Finds entities that have started
reporting live data but have no `EntityRealTimeRegistration` row, using raw SQL:

```sql
SELECT DISTINCT sds.entity_id
FROM connection_statistics_entitydailystatus AS sds
INNER JOIN entities_entity s ON s.id = sds.entity_id
LEFT JOIN connection_statistics_entityrealtimeregistration AS srt
       ON sds.entity_id = srt.entity_id AND srt.deleted IS NULL
WHERE s.deleted IS NULL AND sds.deleted IS NULL
  AND sds.connectivity_speed IS NOT NULL
  AND srt.entity_id IS NULL
```

Then, **per entity**, it shells out to a management command and issues two more queries:

```python
for missing_entity_id in entity_ids_missing_in_rt_table:
    call_command('populate_entity_registration_data', '--reset', f'-entity_id={...}')
    entity = Entity.objects.get(id=...)
    latest_daily = EntityDailyStatus.objects.filter(...).order_by('-date').first()
    ...
    entity.save(update_fields=['connectivity_status'])
```

> `call_command` inside a per-row loop re-enters Django's command machinery for every entity. On a
> large first import this is the dominant cost and a clear candidate for batching. The log message
> also claims "in last 6 hours" while the job runs every 6 hours on a 4×-daily schedule — the query
> has no time bound at all, so it processes *every* unregistered entity each run.

## `rebuild_unified_index` — 02:00, 4-hour limit

[proco/utils/tasks.py:481](../../proco/utils/tasks.py#L481). A thin wrapper:

```python
call_command('build_unified_index', '--delete_index', '--create_index', '--clean_index', '--update_index')
```

A **full destructive rebuild** every night: the index is deleted and recreated, not incrementally
updated. See [search-index.md](search-index.md).

## `update_all_entity_cached_values` — 04:45, 2-hour limit

See [cache-warming.md](cache-warming.md). Note it still selects countries by
`School.objects...distinct('country_id')` — a country with health entities but no schools would not
be warmed.

## `cleanup_health_entity_master_rows` — 01:45, 15:45

[proco/data_sources/tasks.py:886](../../proco/data_sources/tasks.py#L886). Same three raw-SQL
de-duplication passes as the school version, against
`data_sources_healthentitymasterintermediatedata`. **Hard deletes**, outside the ORM and outside
the soft-delete convention.

## Manual operations

```bash
# Seed the type registry (idempotent)
pipenv run python manage.py seed_entity_types

# Full Giga Meter backfill for health entities
pipenv run python manage.py entity_giga_meter_sync -entity_type='health'

# QoS sync for a date range
pipenv run python manage.py entity_qos_sync \
  -entity_type='health' -start_date='2026-09-01' -end_date='2026-09-15'

# Re-aggregate a week
pipenv run python manage.py redo_entity_aggregations \
  -country_id=5 -entity_type_code='health' -year=2026 -week_no=37 --update_entity_weekly
```

## Failure modes

| Symptom | Cause |
|---|---|
| No health data at all | `HEALTH_MASTER_COUNTRY_INCLUSION_LIST` empty, or `HEALTH_MASTER_*` unset |
| Master data flows, live data does not | `HEALTH_GIGA_METER_ENABLE_AUTO_SYNC` / `ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC` default to `False` |
| Entities missing from search | `ENTITIES_INDEX_NAME` unset, or `rebuild_unified_index` failed mid-rebuild leaving no index |
| Entities invisible in every API | `entity_type` is `NULL` on those rows |
| `update_entity_records` never completes | 10 h limit; run it with a `start_time`/`end_time` window |
| Country totals look wrong | `CountryWeeklyStatus` is shared between both pipelines |
