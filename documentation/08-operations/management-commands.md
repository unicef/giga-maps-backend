# Management commands

**40 commands.** Many are the de-facto runbooks for one-off data operations — the commented-out
block in [web-worker.sh](../../web-worker.sh) is effectively an undocumented operations history.

All run as `pipenv run python manage.py <command>`. Flags use a **single dash** for value arguments
(`-country_id=5`) and a double dash for booleans (`--reset`), which is unusual for Django and easy
to get wrong.

---

## Setup and seeding

| Command | Arguments | Purpose |
|---|---|---|
| `load_iso3_format_code_for_countries` | `--country-file` | Load countries with ISO3 codes |
| `load_country_admin_data` | `-af/--admin-file`, `-at/--admin-type` | Load admin0/1/2 boundaries from Mapbox CSVs |
| `load_api_data` | `-af/--api-file` | Load the public API catalogue from a TSV |
| `load_system_data_layers` | `--delete_data_sources`, `--update_data_sources`, `--delete_data_layers`, `--update_data_layers`, `--update_data_layers_code` | Seed data sources and layers |
| `load_column_configurations` | — | Seed queryable columns |
| `load_about_us_content` | `--load_about_us_content` | Seed About Us CMS content |
| `update_system_role_permissions` | — | Create/refresh the `Admin` and `Read Only` roles |
| `non_prod_setup_roles_and_users` | — | Roles **and** representative users, non-production only |
| `create_admin_user` | `-email`, `-first_name`, `-last_name` | Create a superuser |
| `populate_active_data_layer_for_countries` | `--reset` | Link layers to countries |
| `populate_active_filters_for_countries` | — | Link filters to countries |
| `populate_admin_ui_labels` | `-at` | Admin1/Admin2 display labels |

## Entity subsystem

| Command | Arguments | Purpose |
|---|---|---|
| `seed_entity_types` | — | Seed the `school` and `health` entity types. Idempotent |
| `load_health_entity_column_configurations` | — | Health column configs |
| `load_health_entity_data_layers` | — | Health data sources and layers |
| `load_health_entity_advance_filters` | — | Health filters + country relationships |
| `populate_entity_active_filters_for_countries` | — | Link entity filters to countries |
| `populate_entity_registration_data` | — | Populate `EntityRealTimeRegistration` |
| `entity_giga_meter_sync` | `-entity_type` | **Full backfill** of Giga Meter data for entities |
| `entity_qos_sync` | `-entity_type`, `-start_date`, `-end_date` | QoS Delta Share sync for entities |

> `entity_giga_meter_sync` fetches **all** data, not a delta. Its own help text says so. Do not run
> it as a routine refresh.

## Search index

| Command | Arguments |
|---|---|
| `index_rebuild_schools` | `--delete_index`, `--create_index`, `--clean_index`, `--update_index`, `-country_id`, `-school_id` |
| `build_unified_index` | same, plus `-entity_id` |

```bash
# Full rebuild of the unified index
pipenv run python manage.py build_unified_index \
  --delete_index --create_index --clean_index --update_index

# One country only
pipenv run python manage.py build_unified_index --update_index -country_id=144
```

Flag order in the source examples is delete → create → clean → update. `build_unified_index` is the
current one; `index_rebuild_schools` targets the legacy `giga_schools` index.

## Data population and backfill

| Command | Arguments | Purpose |
|---|---|---|
| `populate_school_new_fields` | `-start_school_id`, `-end_school_id`, `-country_id`, `-school_id`, `-school_ids` | Backfill `coverage_type`, `coverage_status`, `connectivity_status` |
| `populate_school_registration_data` | `--reset`, `-country_id` | Populate `SchoolRealTimeRegistration` |
| `populate_admin_id_fields_to_schools` | `-start_school_id`, `-end_school_id`, `-at` | Backfill `admin1`/`admin2` FKs |
| `populate_school_geopoint_field` | `-country_iso3_format`, `--schedule` | Backfill geopoints |

`--schedule` dispatches the work as a Celery task instead of running inline — useful for anything
that would exceed a shell session, and essential on a production host where you cannot hold a
terminal open for hours.

## Recovery

Covered with worked examples in [runbooks.md](runbooks.md).

| Command | Arguments |
|---|---|
| `data_loss_recovery_for_pcdc` | `-start_date`, `-end_date`, `-pull_data_date`, `--check_missing_dates`, `--pull_data` |
| `data_loss_recovery_for_pcdc_weekly` | `-start_week_no`, `-end_week_no`, `-year`, `--pull_data` |
| `data_loss_recovery_for_qos` | `-country_code`, `-pull_start_version`, `-pull_end_version`, `-aggregate_start_version`, `-aggregate_end_version`, `--check_missing_versions`, `--pull_data`, `--aggregate` |
| `data_loss_recovery_for_qos_dates` | `-country_code`, `-start_date`, `-end_date`, `--check_missing_dates` |
| `data_loss_recovery_for_school_master_version` | `-country_code`, `-pull_version`, `--check_latest_version`, `--pull_data`, `--schedule` |
| `redo_aggregations` | `-country_id`, `-year`, `-week_no`, `--update_school_weekly`, `--update_country_daily` |
| `redo_entity_aggregations` | `-country_id`, `-entity_type_code`, `-year`, `-week_no`, `--update_entity_weekly` |

Every recovery command has a **dry-run style flag** (`--check_missing_dates`,
`--check_missing_versions`, `--check_latest_version`) that reports gaps without changing anything.
Always run that first.

## Cleanup and repair

### `data_cleanup` — the swiss army knife

The largest command in the project, with 20+ mutually independent operations:

| Flag | Effect |
|---|---|
| `--clean_duplicate_schools` | De-duplicate school rows |
| `--clean_duplicate_school_gigs_ids` | De-duplicate by Giga ID (note the typo, `gigs`) |
| `--clean_duplicate_school_external_ids` | De-duplicate by government ID |
| `--clean_duplicate_school_weekly` / `_daily` | De-duplicate statistics |
| `--clean_duplicate_country_weekly` / `_daily` | Same, country level |
| `--cleanup_school_master_rows` | Manual run of the scheduled cleanup |
| `--cleanup_health_entity_master_rows` | Health equivalent |
| `--cleanup_qos_data_rows` | Prune `QoSData` |
| `--cleanup_active_download_layer` | Requires `-layer_id` |
| `--update_school_giga_ids` | Repair Giga IDs |
| `--populate_school_lowercase_fields` | Rebuild `*_lower` denormalised columns |
| `--populate_school_education_level_govt_lower` | Same, one field |
| `--handle_published_school_master_data_row` | Manual publish apply |
| `--handle_deleted_school_master_data_row` | Manual delete apply |
| `*_with_scheduler` variants | Dispatch via Celery instead of inline |

Scoping arguments: `-country_id`, `-exclude_country_ids`, `-start_school_id`, `-end_school_id`,
`-year`, `-week_no`, `-start_week_no`, `-end_week_no`, `-layer_id`.

> **`data_cleanup` performs destructive de-duplication with no dry-run flag.** Take a database
> snapshot before running any `--clean_duplicate_*` operation, and scope it with `-country_id`
> rather than running globally.

### Others

| Command | Arguments | Purpose |
|---|---|---|
| `data_source_additional_steps` | `--clean_school_master_historical_rows`, `--clean_health_entity_master_historical_rows`, `-country_id`, `-start_school_id`, `-end_school_id` | Prune `django-simple-history` rows. Called by `clean_historic_data` at weekends |
| `data_alteration_through_sql` | `--update_brasil_live_data_source_name`, `--update_non_brasil_live_data_source_name`, `-start_school_id`, `-end_school_id` | Raw-SQL fixups for live data source naming. Brazil is special-cased because of the nic.br integration |
| `merge_schools` | `-old_id`, `-new_id`, `-year`, `--delete_old`, `--redo_aggregations` | Merge duplicate schools, optionally re-aggregating |
| `clearcsv` | — | Strip erroneous lines from a CSV before import |

## Giga Meter

| Command | Arguments | Purpose |
|---|---|---|
| `sync_school_master_data_with_giga_meter_db` | `-country_iso3_format`, `--force`, `--schedule` | Push school master data into the Giga Meter database |
| `fetch_giga_meter_ping_data` | `--date`, `--schedule` | Fetch ping data and aggregate into `SchoolDailyStatus` |
| `backup_giga_meter_connectivity_ping_data` | `-start_date`, `-end_date`, `-till_date`, `--schedule` | Back up ping data to Azure Delta Lake |

## API keys

```bash
pipenv run python manage.py create_api_key_with_write_access \
  -user='service@example.org' \
  -api_code='DAILY_CHECK_APP' \
  -reason='Post/Delete control over DailyCheckApp documentation' \
  -valid_till='31-12-2099' \
  --force_user -first_name='Service' -last_name='Account'
```

| Argument | Purpose |
|---|---|
| `-user` | Email |
| `-api_code` | Which API |
| `-reason` | Stored in `write_access_reason` |
| `-valid_till` | `DD-MM-YYYY` |
| `--force_user` | Create the user if absent — then `-first_name`/`-last_name` are required |
| `--inactive_email` | Suppress notification |

> Keys created this way have `has_write_access=True` and are therefore **invisible in the admin
> console**, which filters on `has_write_access=False`. They cannot be listed, audited or revoked
> through the UI. Keep an out-of-band record of every key issued with this command. See
> [../04-admin-flows/api-key-approval.md](../04-admin-flows/api-key-approval.md).

## Conventions and cautions

**Argument style.** Single dash for values, double dash for booleans. `--country_id=5` will not
work; it is `-country_id=5`.

**`--schedule`.** Where available, prefer it over running inline on a production host.

**ID-range batching.** Several commands take `-start_school_id` / `-end_school_id`. The historical
usage in `web-worker.sh` is `1` to `1000000` in a single pass. Smaller batches are safer on a live
database.

**No transaction wrapper.** Most of these commands are not wrapped in `transaction.atomic()`. An
interrupted run leaves partial results. Design your batches so that re-running is safe.

## Related

- [runbooks.md](runbooks.md) — worked recovery procedures
- [../05-background-jobs/manual-and-recovery-tasks.md](../05-background-jobs/manual-and-recovery-tasks.md)
