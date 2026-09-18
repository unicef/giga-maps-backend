# Repository map

## Top level

| Path | What it is |
|---|---|
| `config/` | Django settings, URLs, WSGI, gunicorn config, DB backends |
| `proco/` | The application — all 14 Django apps plus shared utilities |
| `docker/` | Dockerfiles and compose files for local, dev, test and observability |
| `deploy/` | Deployment assets |
| `scripts/` | `runtests.sh` and helpers |
| `manage.py` | Django entry point |
| `hello.py` | Flask sidecar used as a health endpoint in worker containers |
| `web-worker.sh` | Web container entrypoint — migrate, collectstatic, gunicorn |
| `celery.sh` / `celery-dev.sh` | Worker container entrypoints |
| `celerybeat.sh` | Beat container entrypoint (RedBeat) |
| `azure-pipeline-dev.yml` | CI/CD |
| `data_sources.json` | Seed data for data-source records |

`web-worker.sh` also contains **~80 lines of commented-out management commands**. These are not
dead weight — they are the de-facto runbook for one-off data operations. They are catalogued
properly in [08-operations/management-commands.md](../08-operations/management-commands.md).

## `config/`

| Path | Purpose |
|---|---|
| `config/settings/base.py` | The single large settings module (~600 lines) |
| `config/settings/dev.py`, `prod.py`, … | Environment overlays |
| `config/urls.py` | Root URLconf |
| `config/db/backends/` | Custom database backend(s) |
| `config/gunicorn.py` | gunicorn tuning |
| `config/prometheus_multiproc.py` | Multiprocess Prometheus wrapper |
| `config/tests/` | Settings-level tests |

## `proco/` — the apps

Ordered roughly by how central they are.

| App | LOC¹ | Responsibility |
|---|---:|---|
| `accounts` | 17.7k | API keys, API catalogue, **data layers**, **advanced filters**, column configurations, notifications, app config. The largest and most complex app. |
| `data_sources` | 8.7k | Ingestion from School Master, Health Master, QoS and Daily Check App; the review/publish workflow for master data |
| `core` | 7.6k | Shared base models, permissions (47 slugs), viewsets, filters, managers, and 40 management commands |
| `connection_statistics` | 7.1k | The statistics tables — weekly/daily/real-time, for both countries and schools/entities |
| `schools` | 3.9k | The `School` model, tile endpoints, CSV import, downloads |
| `entities` | 3.6k | The generic `EntityType`/`Entity`/`HealthEntity` system and its `v2` API |
| `locations` | 2.9k | `Country`, `CountryAdminMetadata`, boundaries, country search |
| `utils` | 2.3k | Cache manager, DB routers, Celery tasks for cache/index, Slack notifications |
| `giga_meter` | 2.2k | Models and jobs against the separate Giga Meter database |
| `custom_auth` | 1.9k | `ApplicationUser`, `Role`, `RolePermission`, JWT authentication |
| `about_us` | 575 | CMS content for the About page |
| `background` | 424 | `BackgroundTask` registry — how long jobs report progress |
| `contact` | 303 | Contact form submissions |
| `mailing` | 21 | A single `send_email` Celery task |
| `proco_data_migrations` | — | Empty package, retained for historical migrations |

¹ Python lines excluding migrations.

### Conventional file layout inside an app

```
proco/<app>/
├── models.py          Django models
├── api.py             DRF views / viewsets   ← not views.py
├── api_urls.py        URLconf                 ← not urls.py
├── serializers.py     DRF serializers
├── tasks.py           Celery tasks (6 apps have one)
├── filters.py         django-filter FilterSets
├── config.py          App-local constants
├── constants.py       Choice values, enums
├── utils.py           Helpers
├── management/commands/
├── migrations/
└── tests/
```

The `api.py` / `api_urls.py` naming is consistent across every app — if you are looking for a
view, it is in `api.py`.

### Shared building blocks in `proco/core/`

| File | Contains |
|---|---|
| `models.py` | `BaseModelMixin`, `BaseModel`, `DataSourceModelMixin`, `MasterDataSourceModelMixin` (the review/publish state machine), `CustomDateTimeField` |
| `permissions.py` | ~40 DRF permission classes, all deriving from `ProcoBasePermission` |
| `viewsets.py` | `BaseModelViewSet` and friends |
| `filters.py` | Reusable filter backends |
| `managers.py` | Soft-delete aware managers |
| `db_utils.py` | Raw-SQL helpers |
| `management/commands/` | The bulk of the 40 management commands |
| `resources/` | CSV/TSV seed files (country ISO codes, admin boundaries, API catalogue) |

### Cross-cutting in `proco/utils/`

| File | Contains |
|---|---|
| `cache.py` | `SoftCacheManager` — the stale-while-revalidate cache |
| `db_routers.py` | `CustomRequestDBRouterMiddleware`, `ReadOnlyDBRouter` |
| `tasks.py` | Cache warming, search index rebuilds, entity record updates, aggregation redo |
| `slack_notification_service.py` | Data-change alerts |
| `middleware.py` | `CustomCorsMiddleware` |
| `dates.py` | ISO week helpers |
| `geometry.py` | PostGIS helpers |

## Frontend repo, for cross-reference

| Path | Purpose |
|---|---|
| `src/core/routes.ts` | **Every route in the app**, in one file |
| `src/api/` | HTTP layer — `project-connect.ts` holds most endpoint calls |
| `src/@/map/` | Map view |
| `src/@/country/` | Country detail view |
| `src/@/admin/` | Admin console (models, effects, ui, types) |
| `src/@/api-docs/` | Public API documentation and key request |
| `src/@/about-giga-map/` | Static content pages |
| `src/@/product-tour/` | Guided tour |
| `src/@/sidebar/`, `src/@/common/`, `src/@/scroll/` | Shared UI |
