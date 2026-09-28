# Daily Check App (PCDC)

The Daily Check App — "PCDC", Project Connect Daily Check — is the measurement client whose results
reach the backend over a REST API. It is the primary source of live connectivity data alongside QoS.

## Configuration

`DATA_SOURCE_CONFIG['DAILY_CHECK_APP']`
([config/settings/base.py:460](../../config/settings/base.py#L460)):

| Setting | Env var | Default |
|---|---|---|
| Base URL | `DAILY_CHECK_APP_BASE_URL` | `None` |
| API code | `DAILY_CHECK_APP_API_CODE` | `DAILY_CHECK_APP` |
| Measurement path | `ENTITY_GIGA_METER_MEASUREMENT_PATH` | **`/measurements/v2/sandbox`** |
| Page size | `ENTITY_GIGA_METER_PAGE_SIZE` | `50` |

> **The default measurement path is a sandbox endpoint.** Any environment that has not set
> `ENTITY_GIGA_METER_MEASUREMENT_PATH` is reading test data — and that variable is **not in
> `.env_example`**. Check this before investigating "wrong measurements in production".
>
> A page size of 50 is small for a bulk pull; pagination overhead dominates on high-volume days.
> Both variables are named `ENTITY_GIGA_METER_*` despite configuring the Daily Check App, which
> makes them easy to miss when grepping.

`.env_example` points `DAILY_CHECK_APP_BASE_URL` at
`https://uni-ooi-giga-daily-check-service-api-dev.azurewebsites.net/api/v1` — a dev service.

## Authentication

The backend authenticates with an **API key issued through its own API-key machinery**, created with
`has_write_access=True`. The commented examples in [web-worker.sh](../../web-worker.sh) show real
ones:

```bash
pipenv run python manage.py create_api_key_with_write_access \
  -user='pcdc_user_with_write_api_key@nagarro.com' \
  -api_code='DAILY_CHECK_APP' \
  -reason='For Post/Delete API Control over DailyCheckApp documentation' \
  -valid_till='31-12-2099' --force_user -first_name='PCDC' -last_name='User'
```

> These keys are invisible in the admin console, which filters `has_write_access=False`, and
> `valid_till='31-12-2099'` makes them effectively permanent. Keep an out-of-band record. See
> [../04-admin-flows/api-key-approval.md](../04-admin-flows/api-key-approval.md).

## Ingestion

`load_data_from_daily_check_app_api`
([proco/data_sources/tasks.py:799](../../proco/data_sources/tasks.py#L799)), 1-hour limit. It is the
**first link** in the live-data chain:

```python
chain(
    load_data_from_daily_check_app_api.s(),
    load_data_from_qos_apis.s(),
    chord(group([finalize_previous_day_data.s(cid, date) for cid in country_ids]),
          finalize_task.si()),
).delay()
```

Measurements land in `DailyCheckAppMeasurementData`
([proco/data_sources/models.py:212](../../proco/data_sources/models.py#L212)) and are attributed to
the `DAILY_CHECK_APP_MLAB` live data source. MLab is the measurement platform behind the readings.

> **A failure here stops the entire chain**, including QoS loading and every country's
> finalisation. The DCA pull being down means no live data at all, not just no DCA data.

## Schedule

| Schedule (UTC) | Task | `today` |
|---|---|---|
| `02:10, 08:10, 14:10, 20:10` | `update_live_data` | `True` |
| `00:30` | `update_live_data` | `False` — closes out yesterday |

## Retention

`DailyCheckAppMeasurementData` is pruned at **30 days** by `clean_old_live_data` (05:10 UTC). Raw
measurements older than that are gone; the aggregated `SchoolDailyStatus` rows are permanent.

## Manual invocation and recovery

```bash
curl "https://<host>/api/sources/load/daily_check_app/"
```

```bash
# Diagnose — changes nothing
pipenv run python manage.py data_loss_recovery_for_pcdc \
  -start_date='2026-09-01' -end_date='2026-09-15' --check_missing_dates

# Pull a date range
pipenv run python manage.py data_loss_recovery_for_pcdc \
  -start_date='2026-09-01' -end_date='2026-09-15' --pull_data

# Pull by ISO week
pipenv run python manage.py data_loss_recovery_for_pcdc_weekly \
  -start_week_no=36 -end_week_no=38 -year=2026 --pull_data
```

Pulling raw data does not rebuild the rollups — follow with `redo_aggregations`. Full procedure in
[../08-operations/runbooks.md](../08-operations/runbooks.md).

## Related

- [../05-background-jobs/ingestion-live-data.md](../05-background-jobs/ingestion-live-data.md)
- [giga-meter.md](giga-meter.md) — the other live-measurement path
