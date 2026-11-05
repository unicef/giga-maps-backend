# Local setup

Two supported paths: **Docker Compose** (recommended, matches CI) and **native pipenv**.

---

## Path A — Docker Compose

### Prerequisites

- Docker and Docker Compose
- A `.env` file at the repository root — copy [.env_example](../../.env_example) and fill it in.
  Read [environment-variables.md](environment-variables.md) first: `.env_example` is **incomplete**
  and omits several variables that live code reads.

### Bring it up

```bash
docker compose -f docker/docker-compose.local.yml up --build
```

This starts three services, defined in
[docker/docker-compose.local.yml](../../docker/docker-compose.local.yml):

| Service | Container | Host port | Notes |
|---|---|---|---|
| `db` | `proco_dev_db` | `7432` → 5432 | PostGIS. User/pass/db = `test`/`test`/`proco` |
| `redis` | — | not published | Exposed only inside the compose network |
| `backend` | `proco_dev_backend` | `8000` | `manage.py runserver`, repo bind-mounted at `/code` |

The backend container blocks on a readiness loop before starting:

```bash
until PGPASSWORD=test psql -h db -U test -d gigameter -c 'SELECT 1'; do sleep 5; done
```

> **This waits for a database named `gigameter`, not `proco`.** The Giga Meter database must exist
> in the same Postgres instance or the backend will loop forever. `docker/Dockerfile-db-local` and
> `docker/initdb-local.sh` are responsible for creating it — if the backend hangs on
> "DB not ready yet", check there first.

### Celery is commented out by default

The `celery_beat` and `celery_worker` services are present but commented out in the compose file.
Uncomment them if you need scheduled or async work. Note that the committed block contains a
**hard-coded `FLOWER_BASIC_AUTH` credential pair** — do not reuse it, and do not uncomment it into
anything reachable from outside your machine.

To run a worker without Docker changes, from inside the backend container:

```bash
pipenv run celery --app=proco.taskapp worker --loglevel=DEBUG --concurrency=2
```

And beat:

```bash
pipenv run celery --app=proco.taskapp beat --scheduler=redbeat.RedBeatScheduler --loglevel=DEBUG
```

### Database URLs used by compose

```
DATABASE_URL           postgis://test:test@db/proco
READ_ONLY_DATABASE_URL postgis://test:test@db/proco?options=-c statement_timeout=300000
REDIS_URL              redis://redis:6379/0
CELERY_BROKER_URL      redis://redis:6379/1
CELERY_RESULT_BACKEND_URL redis://redis:6379/2
```

Locally the "read replica" points at the same database, with a 300-second statement timeout. That
timeout is the only thing distinguishing it — which is worth remembering when a query that works
locally times out against the real replica.

---

## Path B — Native

### System dependencies

GeoDjango needs system libraries that pip cannot provide:

```bash
# macOS
brew install postgis gdal geos proj libpq

# Debian/Ubuntu — mirrors what CI does
sudo apt install postgis postgresql-14-postgis-3 binutils libproj-dev gdal-bin
```

### Python

The project pins **Python 3.8**.

```bash
pip install pipenv
pipenv install --dev
```

`--dev` is not optional: `delta-sharing` and `django-anymail` are declared in `[dev-packages]`
but imported by production code paths. See [../00-overview/tech-stack.md](../00-overview/tech-stack.md).

### Database

```bash
createdb proco
psql proco -c "CREATE EXTENSION postgis;"
createdb gigameter
psql gigameter -c "CREATE EXTENSION postgis;"
```

### Run

```bash
pipenv run python manage.py migrate
pipenv run python manage.py runserver 0.0.0.0:8000
```

---

## Seeding a usable dataset

A fresh database has no countries, no roles, no API catalogue and no data layers — most endpoints
will return empty results and the admin console will be unusable. Seed in this order:

```bash
# 1. Countries with ISO3 codes
pipenv run python manage.py load_iso3_format_code_for_countries \
  --country-file ./proco/core/resources/locations_country_processed.csv

# 2. Administrative boundaries (admin0 → admin1 → admin2)
pipenv run python manage.py load_country_admin_data -af ./proco/core/resources/mapbox_boundaries_metadata_admin_0.csv -at admin0
pipenv run python manage.py load_country_admin_data -af ./proco/core/resources/mapbox_boundaries_metadata_admin_1.csv -at admin1
pipenv run python manage.py load_country_admin_data -af ./proco/core/resources/mapbox_boundaries_metadata_admin_2.csv -at admin2

# 3. Roles and their permissions
pipenv run python manage.py update_system_role_permissions

# 4. The public API catalogue
pipenv run python manage.py load_api_data --api-file ./proco/core/resources/all_apis-dev.tsv

# 5. Data sources and system data layers
pipenv run python manage.py load_system_data_layers --delete_data_sources --update_data_sources --update_data_layers

# 6. Column configurations and filters
pipenv run python manage.py load_column_configurations
pipenv run python manage.py populate_active_data_layer_for_countries --reset
pipenv run python manage.py populate_active_filters_for_countries

# 7. An admin user for yourself
pipenv run python manage.py create_admin_user -email='you@example.com' -first_name='You' -last_name='Dev'
```

For a non-production environment there is a shortcut that creates roles *and* representative users
in one step:

```bash
pipenv run python manage.py non_prod_setup_roles_and_users
```

### Entity subsystem

If you are working on entities, additionally:

```bash
pipenv run python manage.py seed_entity_types
pipenv run python manage.py load_health_entity_column_configurations
pipenv run python manage.py load_health_entity_data_layers
pipenv run python manage.py load_health_entity_advance_filters
pipenv run python manage.py populate_entity_active_filters_for_countries
```

Every command above is described in
[../08-operations/management-commands.md](../08-operations/management-commands.md).

---

## Verify the install

```bash
curl -s localhost:8000/health/          # → "OK"
curl -s localhost:8000/api/locations/   # → paginated country list
```

Then run the checks CI runs:

```bash
./scripts/runtests.sh
```

That script sets `DJANGO_SETTINGS_MODULE=config.settings.dev_test` and runs `manage.py check`,
a `makemigrations --dry-run --check` (so an un-generated migration fails the build), then the test
suite under coverage. Note that **the flake8 and isort steps inside it are commented out** — they
run in name only. Run them yourself:

```bash
pipenv run flake8 .
pipenv run isort . --check-only --rr
```

---

## Common problems

| Symptom | Cause | Fix |
|---|---|---|
| Backend loops on "DB not ready yet" | The `gigameter` database does not exist | Create it in the same Postgres instance |
| `django.contrib.gis` import errors | GDAL/GEOS not found | Install system geo libs; on macOS you may need `GDAL_LIBRARY_PATH` / `GEOS_LIBRARY_PATH` |
| `ImportError: delta_sharing` | Installed without `--dev` | `pipenv install --dev` |
| Empty map, no countries | Seed data not loaded | Run the seeding sequence above |
| Beat starts but nothing runs | RedBeat stores the schedule in Redis under `gigamaps:` — a stale key can shadow code changes | Flush the `gigamaps:*` keys and restart beat |
| `tuple concurrently updated` on startup | Two processes creating the PostGIS extension at once | Already handled — [proco/utils/db_routers.py:17](../../proco/utils/db_routers.py#L17) patches `prepare_database` to swallow it |
