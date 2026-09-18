# Glossary

Domain vocabulary, with the code that defines each term.

---

### Admin0 / Admin1 / Admin2
Administrative boundary levels. Admin0 is the country, Admin1 the first subdivision (state,
province, region), Admin2 the second (district, municipality). Stored in `CountryAdminMetadata`
([proco/locations/models.py:152](../../proco/locations/models.py#L152)) and denormalised onto
`School.admin1` / `School.admin2` as nullable FKs. Boundaries are loaded from Mapbox metadata CSVs
via `load_country_admin_data`.

### Advanced Filter
An admin-configured, per-country filter that the public map exposes to users — for example
"schools with download speed above X". Modelled by `AdvanceFilter`
([proco/accounts/models.py:570](../../proco/accounts/models.py#L570)) and
`AdvanceFilterCountryRelationship`. Has its own draft → published lifecycle. See
[04-admin-flows/advanced-filters.md](../04-admin-flows/advanced-filters.md).

### API Key
A credential for the public API, scoped to one `API`, optionally to a set of countries and API
categories, with an approval workflow and an expiry-extension workflow.
[proco/accounts/models.py:92](../../proco/accounts/models.py#L92).

### Benchmark
A threshold used to classify connectivity as good / moderate / no connectivity. Can be **global**
(a single worldwide value) or **country-specific**. Surfaces as the `benchmark=global` query
parameter on data-layer endpoints.

### Column Configuration
Metadata describing a queryable column — its name, type, and permitted values — used to drive both
data layers and advanced filters without hard-coding field lists.
`ColumnConfiguration` ([proco/accounts/models.py:492](../../proco/accounts/models.py#L492)).

### Connectivity status
A coarse classification stored denormalised on `School.connectivity_status`, defaulting to
`'unknown'`. Recomputed by the `update_school_records` job. Distinct from `coverage_status`
(mobile/infrastructure coverage) and `coverage_type`.

### Coverage
Availability of mobile/fixed network infrastructure *near* a school, as opposed to whether the
school itself is connected. Tracked with its own set of counters
(`schools_coverage_good/moderate/no/unknown`) and its own availability ladder
(`COVERAGE_TYPES_AVAILABILITY`).

### Daily Check App (DCA) / PCDC
The **P**roject **C**onnect **D**aily **C**heck app — the measurement client whose results reach
the backend through the Daily Check App API. Its measurements land in
`DailyCheckAppMeasurementData` and are attributed to the `DAILY_CHECK_APP_MLAB` live data source.
"PCDC" appears throughout task and command names (`data_loss_recovery_for_pcdc`).

### Data Layer
An admin-defined visualisation layer on the map — a named, published, country-scoped view over a
metric, backed by one or more data sources. `DataLayer`
([proco/accounts/models.py:339](../../proco/accounts/models.py#L339)) with
`DataLayerDataSourceRelationship` and `DataLayerCountryRelationship`. Types include `LIVE`;
categories include `CONNECTIVITY`. See [04-admin-flows/data-layers.md](../04-admin-flows/data-layers.md).

### Delta Share
Databricks' open protocol for sharing tables across organisations. Giga's data platform publishes
`school-master`, `health-master` and `qos` schemas from a share (default name `gold`); the backend
reads them with the `delta-sharing` client. Configured under `DATA_SOURCE_CONFIG` in
[config/settings/base.py:426](../../config/settings/base.py#L426).

### Entity
The generic replacement for `School` — any mappable point of interest.
`Entity` ([proco/entities/models.py:196](../../proco/entities/models.py#L196)) carries the shared
geography and connectivity fields; type-specific fields live in a OneToOne detail model such as
`HealthEntity`. See [02-domain-model/entities.md](../02-domain-model/entities.md).

### EntityType
A **database row**, not a Python enum, describing a kind of entity: its code (`school`, `health`),
its detail model, its master-data model, and whether it is `is_legacy` (i.e. backed by the old
`School` table). [proco/entities/models.py:22](../../proco/entities/models.py#L22).

### Giga ID
Giga's stable global identifier for a school (`School.giga_id_school`), as distinct from the
government identifier (`School.external_id`, sourced from `school_id_govt`). Uniqueness is enforced
per country, with a partial index excluding soft-deleted rows.

### Giga Meter
The measurement product (desktop app + Chrome extension) that produces connectivity readings. It
has **its own PostgreSQL database**, exposed to this backend as the `gigameter_database` alias and
read through `GigaMeterDBRouter`. The `proco/giga_meter/` app models those tables.

### Health Entity
The first non-school entity type: health facilities. Master data arrives from the `health-master`
Delta Share schema into `HealthEntityMasterIntermediateData`, then becomes `Entity` +
`HealthEntity` rows.

### Integration status
A country's position on the onboarding ladder, stored on `CountryWeeklyStatus.integration_status`
([proco/connection_statistics/models.py:64](../../proco/connection_statistics/models.py#L64)):

| Value | Constant | Meaning |
|---:|---|---|
| 4 | `COUNTRY_CREATED` | Default — country exists, nothing mapped |
| 5 | `SCHOOL_OSM_MAPPED` | School locations from OpenStreetMap |
| 0 | `JOINED` | Country joined Project Connect |
| 1 | `SCHOOL_MAPPED` | School locations mapped |
| 2 | `STATIC_MAPPED` | Static connectivity mapped |
| 3 | `REALTIME_MAPPED` | Real-time connectivity mapped |

The numeric values are **not** in ladder order — do not sort by the raw integer.

### Live data source
Which measurement system a reading came from
([proco/connection_statistics/config.py:8](../../proco/connection_statistics/config.py#L8)):
`DAILY_CHECK_APP_MLAB`, `QOS`, `DAILY_CHECK_APP_MLAB_QOS`, `UNKNOWN`.

### Master data
Authoritative reference data about schools/entities, arriving from the data platform and passing
through a human review workflow before it is applied. `SchoolMasterData` and
`HealthEntityMasterIntermediateData`, both built on `MasterDataSourceModelMixin`
([proco/core/models.py:154](../../proco/core/models.py#L154)).

### QoS
**Q**uality **o**f **S**ervice — richer per-measurement telemetry (latency, jitter, packet loss,
uptime) from ISP and platform integrations, ingested from the `qos` Delta Share schema into
`QoSData`.

### Real-time registration
A record that a given school/entity is *expected* to report live data, and from which source.
`SchoolRealTimeRegistration` / `EntityRealTimeRegistration`. Populated by the
`populate_school_registration_data` and `populate_entity_registration_data` jobs.

### Row status
The review/publish state of a master-data row. Eight states — see
[04-admin-flows/school-master-review-publish.md](../04-admin-flows/school-master-review-publish.md)
for the full state machine.

### Soft delete
The project-wide deletion convention: rows carry a nullable `deleted` timestamp rather than being
removed. Unique constraints are declared **twice** — once including `deleted`, and once as a
partial constraint with `condition=Q(deleted=None)` — so that a name can be reused after deletion.
`School.delete()` also cascades the soft delete to daily and weekly statuses
([proco/schools/models.py:110](../../proco/schools/models.py#L110)).

### Soft cache
The stale-while-revalidate cache layer. A stale entry is returned to the caller *and* a Celery
refresh is queued. [proco/utils/cache.py:11](../../proco/utils/cache.py#L11).

### Time player
The map's time-travel control, which replays connectivity over past weeks/months. Served by
`/api/statistics/time-players/` and the v2 variant `/api/accounts/time-players/v2/`.

### Weekly / Daily status
The aggregation tables. `CountryWeeklyStatus`, `SchoolWeeklyStatus`, `CountryDailyStatus`,
`SchoolDailyStatus` (plus `EntityWeeklyStatus` / `EntityDailyStatus`). Weekly rows are keyed by ISO
`(year, week)`; daily rows by date. `School.last_weekly_status` is a denormalised pointer to the
most recent weekly row.
