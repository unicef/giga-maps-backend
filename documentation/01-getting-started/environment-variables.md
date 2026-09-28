# Environment variables

Settings are read with `django-environ` across four modules: `base.py` (the bulk), and the
`dev.py` / `dev_test.py` / `prod.py` overlays. **120 distinct variables** are referenced in total.

> ### `.env_example` is incomplete
> 45 of the 120 variables are not present in [.env_example](../../.env_example) — including the
> entire `HEALTH_MASTER_*` group, `ENTITIES_INDEX_NAME`, `REDIS_URL`, the ping-backup group and the
> Slack webhook. Most have code defaults so nothing crashes, but features silently do not work.
> The full list of omissions is in [the gap table](#variables-missing-from-envexample) below.

---

## Core

| Variable | Default | Purpose |
|---|---|---|
| `DJANGO_SETTINGS_MODULE` | — | `config.settings.dev` / `.dev_test` / `.prod` |
| `DJANGO_SECRET_KEY` | `None` | Django secret. Read as `SECRET_KEY` in some overlays |
| `PROJECT_FULL_NAME` / `PROJECT_SHORT_NAME` | `gigamaps` | Branding in emails and logs |
| `ALLOWED_HOSTS` | — | Set in the overlays, not `base.py` |
| `APP_ENVIRONMENT` | — | `DEV` / `STG` / `PROD`, used in notifications |
| `GIGAMAPS_LOG_LEVEL` | `INFO` | Level for the `gigamaps.*` logger tree |

## Databases

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Primary PostGIS database |
| `READ_ONLY_DATABASE_URL` | Read replica. Append `?options=-c statement_timeout=…` to bound query time |
| `GIGA_METER_DATABASE_URL` | The separate Giga Meter Postgres |

## Cache and queue

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Django cache backend |
| `CELERY_BROKER_URL` | — | Broker (conventionally db `1`) |
| `CELERY_RESULT_BACKEND_URL` | — | Results (conventionally db `2`) |
| `CELERY_ENABLED` | — | Master switch for async dispatch |
| `NO_EXPIRY_CACHE_PREFIX` | `NO_EXPIRY_CACHE` | Key prefix for the never-expiring cache partition |
| `INVALIDATE_CACHE_HARD` | `'false'` | **String, not bool.** `'true'` makes the nightly warmer `DEL` keys instead of marking them stale |
| `CACHE_CONTROL_MAX_AGE_FOR_FE` | — | `max-age` sent to browsers |
| `LIVE_LAYER_CACHE_FOR_COUNTRY_IDS` | **`['144']`** | Restrict live-layer warming to specific countries. The default is **one country**, not all |
| `LIVE_LAYER_CACHE_FOR_WEEKS` | **`5`** | How many weeks of live-layer data to warm |

## Authentication — Azure AD B2C

| Variable | Purpose |
|---|---|
| `AD_B2C_TENANT_ID`, `AD_B2C_CLIENT_ID` | Tenant and app registration |
| `AD_B2C_BASE_URL`, `AD_B2C_DOMAIN` | B2C endpoints |
| `AD_B2C_SIGNUP_SIGNIN_POLICY` | e.g. `B2C_1_UNICEF_SOCIAL_signup_signin` |
| `AD_B2C_FORGOT_PASSWORD_POLICY` | e.g. `B2C_1_PasswordResetPolicy` |
| `AD_B2C_EDIT_PROFILE_POLICY` | e.g. `B2C_1_ProfileEditingPolicy` |

## Azure Cognitive Search

| Variable | Default | Purpose |
|---|---|---|
| `SEARCH_ENDPOINT`, `SEARCH_API_KEY` | `None` | Service credentials |
| `COUNTRY_INDEX_NAME` | `giga_countries` | |
| `SCHOOL_INDEX_NAME` | `giga_schools` | Built by the now-disabled `rebuild_school_index` |
| `ENTITIES_INDEX_NAME` | `giga_entities` | Built by `rebuild_unified_index` |

> The code defaults use **underscores** (`giga_schools`) while `.env_example` supplies **hyphens**
> (`giga-schools`). Whichever the real service uses, the two must agree — a mismatch produces an
> empty search with no error.

## Data sources

`DATA_SOURCE_CONFIG` at [config/settings/base.py:426](../../config/settings/base.py#L426) groups
these into four blocks.

### School Master (Delta Share)

| Variable | Default |
|---|---|
| `SCHOOL_MASTER_SHARE_NAME` | `gold` |
| `SCHOOL_MASTER_SCHEMA_NAME` | `school-master` |
| `SCHOOL_MASTER_ENDPOINT` | `None` |
| `SCHOOL_MASTER_BEARER_TOKEN` | `None` |
| `SCHOOL_MASTER_EXPIRATION_TIME` | `None` |
| `SCHOOL_MASTER_SHARE_CREDENTIALS_VERSION` | `1` |
| `SCHOOL_MASTER_REVIEW_GRACE_PERIOD_IN_HRS` | `48` — how long a row waits for human review before auto-publish |
| `SCHOOL_MASTER_DASHBOARD_URL` | `None` — deep link used in reminder emails |
| `SCHOOL_MASTER_COUNTRY_EXCLUSION_LIST` | `''` — comma-separated ISO codes to skip |

### Health Master (Delta Share) — *absent from `.env_example`*

Same shape as School Master, prefixed `HEALTH_MASTER_`, with `SCHEMA_NAME` defaulting to
`health-master`. Adds `HEALTH_MASTER_COUNTRY_INCLUSION_LIST` — an **allow**-list, which School
Master does not have. Health ingestion is opt-in per country.

### QoS (Delta Share)

| Variable | Default |
|---|---|
| `QOS_SHARE_NAME` | `gold` |
| `QOS_SCHEMA_NAME` | `qos` |
| `QOS_ENTITY_SCHEMA_NAME` | `health-master` |
| `QOS_ENDPOINT`, `QOS_BEARER_TOKEN`, `QOS_EXPIRATION_TIME` | `None` |
| `QOS_COUNTRY_EXCLUSION_LIST` | `''` |

### Daily Check App (REST)

| Variable | Default |
|---|---|
| `DAILY_CHECK_APP_BASE_URL` | `None` |
| `DAILY_CHECK_APP_API_CODE` | `DAILY_CHECK_APP` |
| `ENTITY_GIGA_METER_MEASUREMENT_PATH` | `/measurements/v2/sandbox` |
| `ENTITY_GIGA_METER_PAGE_SIZE` | `50` |

## Giga Meter

| Variable | Default | Purpose |
|---|---|---|
| `GIGA_METER_ENABLE_AUTO_SYNC` | `True` | Master switch for school-side Giga Meter sync |
| `HEALTH_GIGA_METER_ENABLE_AUTO_SYNC` | `False` | Health-entity equivalent — **off by default** |
| `ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC` | `False` | Entity live-data sync — **off by default** |

### Ping backup to Azure Delta Lake

| Variable | Default |
|---|---|
| `GIGA_METER_PING_BACKUP_AZURE_BLOB_SAS_TOKEN` | `None` |
| `GIGA_METER_PING_BACKUP_AZURE_STORAGE_ACCOUNT_NAME` | `saunigiga` |
| `GIGA_METER_PING_BACKUP_AZURE_BLOB_CONTAINER_NAME` | `giga-dataops-dev` |
| `GIGA_METER_PING_BACKUP_AZURE_LAKEHOUSE_PATH` | `giga_meter/ping` |
| `GIGA_METER_PING_BACKUP_RAW_DATA_RETENTION_DAYS` | — |
| `GIGA_METER_PING_BACKUP_HARD_DELETE_GRACE_PERIOD_DAYS` | — |

> The container default `giga-dataops-dev` is a **dev** container. Production must override it.

## Email

| Variable | Purpose |
|---|---|
| `MAILJET_API_URL`, `MAILJET_API_KEY`, `MAILJET_SECRET_KEY` | Anymail/Mailjet backend |
| `DEFAULT_FROM_EMAIL`, `SERVER_EMAIL`, `SERVER_EMAIL_SIGNATURE` | Envelope and signature |
| `SUPPORT_EMAIL_ID`, `SUPPORT_PHONE_NUMBER`, `CONTACT_EMAIL` | Shown to users |
| `CONTACT_MANAGERS`, `ADMINS` | Recipients for contact-form and error mail |
| `NO_REPLY_EMAIL_ID_OPTIONS` | Alternate no-reply senders |
| `MAILING_USE_CELERY` | Send via the `send_email` task instead of inline |
| `ENABLED_API_KEY_EMAILS` | `True` — API key lifecycle notifications |
| `ENABLED_DATA_LAYER_EMAILS` | Data layer publish notifications |
| `ENABLED_DATA_SOURCES_EMAILS` | Master-data review reminders |

Each `ENABLED_*_EMAILS` flag is a kill switch. Turn them off in any environment that shares a
mail provider with production, or reviewers get paged by your test data.

## Performance tuning

| Variable | Default | Purpose |
|---|---|---|
| `COUNTRY_MAP_API_SAMPLING_LIMIT` | `None` | Cap on points returned for a country map view |
| `ADMIN_MAP_API_SAMPLING_LIMIT` | `None` | Same, for admin-level views |
| `RANDOM_SCHOOLS_DEFAULT_AMOUNT` | — | Sample size for `/schools/random/` |

Both sampling limits default to `None`, i.e. **unbounded**. `.env_example` suggests 15 000 and
20 000. Leaving them unset on a large country is a reliable way to produce a multi-second response.

## Translation

| Variable | Default |
|---|---|
| `AI_TRANSLATION_ENDPOINT`, `AI_TRANSLATION_KEY`, `AI_TRANSLATION_REGION` | `None` |
| `AI_TRANSLATION_SUPPORTED_TARGETS` | `[]` — list |
| `AI_TRANSLATION_CACHE_KEY_LIMIT` | `2000` |

## Observability

| Variable | Purpose |
|---|---|
| `SENTRY_DSN`, `CELERY_SENTRY_DSN` | Separate DSNs for web and worker |
| `ENABLED_BACKEND_PROMETHEUS_METRICS` | Mounts `django_prometheus` URLs at `/` |
| `ENABLED_DB_METRICS` | Database instrumentation |
| `ENABLED_FLOWER_METRICS` | Starts Flower in the worker container |
| `SCHOOL_MASTER_DATA_CHANGES_SLACK_WEBHOOK_URL` | Slack alerts on master-data change |

## Storage and static

| Variable | Purpose |
|---|---|
| `AZURE_ACCOUNT_NAME`, `AZURE_ACCOUNT_KEY`, `AZURE_CONTAINER` | Blob storage for media |
| `USE_CLOUDFRONT`, `USE_COMPRESSOR`, `USE_HTTPS`, `LETSENCRYPT_DIR` | Overlay-only toggles |
| `MAPBOX_KEY` | Mapbox token |

## Miscellaneous

| Variable | Purpose |
|---|---|
| `CORS_ALLOW_ORIGINS` | Comma-separated origins for `CustomCorsMiddleware` |
| `NOCODB_API_URL`, `NOCODB_API_TOKEN`, `NOCODB_TABLE_ID`, `NOCODB_TABLE_ID_PRODUCTION` | NocoDB integration |
| `DATA_LAYER_DASHBOARD_URL`, `API_KEY_ADMIN_DASHBOARD_URL` | Admin deep links in emails |
| `GIGA_LOGO_URL`, `GIGA_WEBSITE_URL`, `GIGA_LINKEDIN_URL`, `GIGA_X_URL`, `GIGA_INSTAGRAM_URL`, `GIGA_YOUTUBE_URL`, `GIGA_NEWS_LETTER_URL` | Email footer links |
| `DEV_ADMIN_EMAIL` | Dev-only admin account |

---

## Variables missing from `.env_example`

For anyone regenerating the example file. Grouped by consequence of leaving them unset.

**Feature silently disabled**
`HEALTH_MASTER_*` (9 vars) · `ENTITIES_INDEX_NAME` · `QOS_ENTITY_SCHEMA_NAME` ·
`HEALTH_GIGA_METER_ENABLE_AUTO_SYNC` · `ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC` ·
`ENTITY_GIGA_METER_MEASUREMENT_PATH` · `ENTITY_GIGA_METER_PAGE_SIZE` ·
`GIGA_METER_PING_BACKUP_*` (6 vars) · `SCHOOL_MASTER_DATA_CHANGES_SLACK_WEBHOOK_URL`

**Falls back to a default that is wrong outside dev**
`REDIS_URL` · `GIGA_METER_PING_BACKUP_AZURE_BLOB_CONTAINER_NAME` ·
`GIGA_METER_PING_BACKUP_AZURE_STORAGE_ACCOUNT_NAME`

**Operational toggles with no example value**
`CELERY_ENABLED` · `ENABLED_API_KEY_EMAILS` · `ENABLED_DATA_LAYER_EMAILS` ·
`ENABLED_DATA_SOURCES_EMAILS` · `ENABLED_BACKEND_PROMETHEUS_METRICS` · `ENABLED_DB_METRICS` ·
`RANDOM_SCHOOLS_DEFAULT_AMOUNT` · `CONTACT_MANAGERS` · `NO_REPLY_EMAIL_ID_OPTIONS`

**Overlay-only (`prod.py` / `dev.py`), so arguably fine to omit**
`ALLOWED_HOSTS` · `ADMINS` · `SECRET_KEY` · `AZURE_ACCOUNT_*` · `AZURE_CONTAINER` ·
`USE_CLOUDFRONT` · `USE_COMPRESSOR` · `USE_HTTPS` · `LETSENCRYPT_DIR` · `MAPBOX_KEY` ·
`DEV_ADMIN_EMAIL`
