# Tech stack

Versions are taken from [Pipfile](../../Pipfile) and [Dockerfile](../../Dockerfile). Exact resolved
versions live in `Pipfile.lock`.

## Runtime

| Layer | Choice | Version | Notes |
|---|---|---|---|
| Language | Python | **3.8** | Pinned in `[requires]` and the Azure pipeline |
| Framework | Django | **>=2.2,<3** | Django 2.2 LTS — end of extended support Apr 2022 |
| API | Django REST Framework | >=3.10 | |
| WSGI server | gunicorn | * | 8 workers, 300s timeout |
| Database | PostgreSQL + PostGIS | see `docker/Dockerfile-db*` | GeoDjango via `django.contrib.gis` |
| DB driver | psycopg2 | 2.8.6 | |
| Cache / broker | Redis | 5.2.1 client, `django-redis` 5.2.0 | |
| Task queue | Celery | <6.0 | |
| Scheduler | celery-redbeat | 2.2.0 | Schedule stored in Redis, not the DB |
| Task UI | flower | 2.0.1 | Optional |

> **Django 2.2 is past end-of-life.** Any upgrade work needs to account for
> `djangorestframework-jwt` (1.11.0, unmaintained), `jsonfield` 2.0.2, and `django-fakery` 3.0.0,
> all of which are version-sensitive to the Django 2.x series.

## Notable libraries and why they're here

| Package | Role |
|---|---|
| `djangorestframework-gis` | Serializes PostGIS geometry to GeoJSON for map endpoints |
| `django-mapbox-location-field` | Map point widget in the admin |
| `unicef-restlib` | UNICEF's shared DRF helpers; supplies `DynamicPageNumberPagination` |
| `drf-flex-fields` | Powers the `?expand=` query parameter used across list endpoints |
| `django-simple-history` (3.0.0) | Audit trail on master-data models via `HistoricalRecords` |
| `django-dynamic-db-router` | Third router in the chain; allows explicit `in_database()` blocks |
| `azure-search-documents` (11.3.0) | Azure Cognitive Search client |
| `django-storages[azure]` + `azure-storage-blob` (<12) | Media storage on Azure Blob |
| `delta-sharing` (1.0.5) | Reads Databricks Delta Shares — **declared in `[dev-packages]`** |
| `django-constance` | Runtime-editable settings (see `CONSTANCE_CONFIG`) |
| `django-anymail` (7.2) | Transactional email — **also in `[dev-packages]`** |
| `django-templated-email` | Templated notification emails |
| `scikit-learn`, `scipy`, `numpy` | Statistical helpers in aggregation code |
| `isoweek` | ISO week arithmetic — the weekly statistics tables are ISO-week keyed |
| `geopy` | Distance calculations |
| `django-prometheus` (2.2.0) | `/metrics` endpoint |
| `sentry-sdk` (1.39.1) | Error reporting |
| `newrelic` | APM agent |
| `flask` | Tiny sidecar in worker containers ([hello.py](../../hello.py)) |

> `delta-sharing` and `django-anymail` sit in `[dev-packages]` while being imported by production
> code paths (data-source ingestion and mailing respectively). The Docker build installs dev
> packages, so this works — but it is fragile, and a `pipenv install --deploy` without `--dev`
> would break ingestion. Worth fixing.

## Frontend (for cross-reference)

`giga-maps-frontend` is a React SPA using **effector** for state management and a custom
`createRouter` abstraction over `history`. Routes are declared centrally in
`src/core/routes.ts`, which makes it the single best file to read when mapping a URL to a flow —
see [03-user-flows/README.md](../03-user-flows/README.md).

Map rendering is Mapbox GL, fed by the backend's vector-tile endpoints under
`/api/locations/schools/tiles/`.

## Tooling

| Concern | Tool | Config |
|---|---|---|
| Linting | flake8 + ~15 plugins | [.flake8](../../.flake8) |
| Import order | isort | [.isort.cfg](../../.isort.cfg) |
| Coverage | coverage 7.3.0 | [.coveragerc](../../.coveragerc) |
| Static analysis | SonarQube | Step in [azure-pipeline-dev.yml](../../azure-pipeline-dev.yml) |
| Security lint | flake8-bandit, dlint | via flake8 |
| CI/CD | Azure Pipelines | [azure-pipeline-dev.yml](../../azure-pipeline-dev.yml) |
| Containers | Docker + compose | [docker/](../../docker/) |
