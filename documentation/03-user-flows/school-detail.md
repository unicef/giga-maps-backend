# School detail

`/map/schools` and the school panel on the map — a single school, its attributes and its
connectivity history.

**Backend**: `SchoolsViewSet` ([proco/schools/api.py:57](../../proco/schools/api.py#L57)),
`SchoolDailyStatsListAPIView`

---

## What loads

| Call | Purpose |
|---|---|
| `GET /api/locations/schools/school/{id}/` | School attributes |
| `GET /api/statistics/school/{school_id}/daily-stat/` | The connectivity timeline |
| `GET /api/statistics/schoolconnectivity/` | Connectivity summary |
| `GET /api/statistics/schoolcoverage/` | Coverage summary |
| `GET /api/statistics/schoolweeklystatus/` | Weekly rows |

## What a school record contains

[proco/schools/models.py:18](../../proco/schools/models.py#L18) — grouped as the UI presents it:

| Section | Fields |
|---|---|
| Identity | `name`, `giga_id_school`, `external_id` (government id) |
| Location | `country`, `admin1`, `admin2`, `geopoint`, `address`, `postal_code`, `timezone` |
| Education | `education_level`, `education_level_govt`, `education_level_regional`, `school_type`, `environment` |
| Status | `connectivity_status`, `coverage_status`, `coverage_type` |
| Provenance | `is_verified_school`, `establishment_year`, `gps_confidence` |

Richer attributes — student and teacher counts, computers, electricity, water, infrastructure
distances — live on **`SchoolWeeklyStatus`**, not on `School`. They are point-in-time values that
change between master-data releases, so they are versioned by week.

`School.last_weekly_status` is the denormalised pointer to the most recent one.

## Status fields are denormalised and can drift

`connectivity_status`, `coverage_status` and `coverage_type` are copies, refreshed by
`update_school_records` at **01:00 UTC**. Between a statistics update and that job, the map colour
and the detail panel can disagree.

All three are `CharField(max_length=10, blank=True, default='unknown')` with **no `choices`
constraint**, so a recovery script writing an unexpected value will persist it silently.

Repair:

```bash
pipenv run python manage.py populate_school_new_fields -country_id=5
```

## The connectivity timeline

`GET /api/statistics/school/{school_id}/daily-stat/` reads `SchoolDailyStatus`
([proco/connection_statistics/models.py:334](../../proco/connection_statistics/models.py#L334)),
carrying the full measurement set: download and upload speed (bps), latency, jitter, packet loss,
uptime, round-trip time, plus `live_data_source`.

`live_data_source` tells you where a reading came from:

| Value | Meaning |
|---|---|
| `DAILY_CHECK_APP_MLAB` | Daily Check App / MLab |
| `QOS` | QoS partner feed |
| `DAILY_CHECK_APP_MLAB_QOS` | Both |
| `UNKNOWN` | Unattributed |

> **Retention asymmetry matters here.** `SchoolDailyStatus` is **never pruned**, so the timeline goes
> back indefinitely. The *raw* measurements behind it do not: `DailyCheckAppMeasurementData` is kept
> 30 days, and `QoSData` keeps only the latest version per country. A gap in the timeline usually
> cannot be repaired by re-aggregating — it needs a re-pull. See
> [../05-background-jobs/cleanup-and-retention.md](../05-background-jobs/cleanup-and-retention.md).

## Real-time registration

`SchoolRealTimeRegistration` records that a school is *expected* to report live data, and from which
source. A school with daily statuses but no registration row is picked up by
`populate_school_registration_data`, which runs four times a day.

If a school shows live data but is not flagged as real-time in the UI, that job is the place to
look.

## Random schools

`GET /api/locations/schools/random/` (`RandomSchoolsListAPIView`,
[proco/schools/api.py:117](../../proco/schools/api.py#L117)) returns a sample for the landing
experience, sized by `RANDOM_SCHOOLS_DEFAULT_AMOUNT` — which is **not in `.env_example`**.

It uses `CachedListMixin`, so the same sample is served until the cache turns over.

## Duplicate schools

Duplicates arise when the same physical school appears under two Giga IDs. `School` enforces
uniqueness on `(country, giga_id_school)` among live rows, which does not prevent this.

```bash
pipenv run python manage.py merge_schools \
  -old_id=123 -new_id=456 -year=2026 --delete_old --redo_aggregations
```

`--redo_aggregations` re-runs the rollups so counts stay consistent after the merge. Without it,
country totals will be wrong until the next aggregation.

## Failure modes

| Symptom | Cause |
|---|---|
| Detail panel status ≠ map colour | Denormalised fields stale until 01:00 UTC |
| Timeline has gaps | Ingestion gap; raw data may already be pruned |
| Attributes missing | They live on `SchoolWeeklyStatus`; the school may have no weekly row yet |
| School not on the map but reachable by URL | Tile `LIMIT` truncation at that zoom level |
| Same school twice | Two Giga IDs — use `merge_schools` |
| Live data present, not flagged real-time | `populate_school_registration_data` has not run |

## Related

- [map-exploration.md](map-exploration.md)
- [../02-domain-model/data-model.md](../02-domain-model/data-model.md)
- [../05-background-jobs/aggregation.md](../05-background-jobs/aggregation.md)
