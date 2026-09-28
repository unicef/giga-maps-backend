# Testing

## Running tests

```bash
./scripts/runtests.sh
```

That script ([scripts/runtests.sh](../../scripts/runtests.sh)) does four things:

```bash
export DJANGO_SETTINGS_MODULE=config.settings.dev_test
pipenv run python -W ignore manage.py check
pipenv run python -W ignore manage.py makemigrations --dry-run --check
pipenv run coverage run manage.py test -v 2 --noinput
pipenv run coverage xml
```

`makemigrations --dry-run --check` fails if a model change has no migration — a genuinely useful
gate that catches the most common review mistake.

> **The flake8 and isort steps inside the script are commented out.** They print their banners and
> do nothing. Run them yourself:
>
> ```bash
> pipenv run flake8 .
> pipenv run isort . --check-only --rr
> ```

### Individual tests

```bash
export DJANGO_SETTINGS_MODULE=config.settings.dev_test
pipenv run python manage.py test proco.schools
pipenv run python manage.py test proco.schools.tests.test_api
pipenv run python manage.py test proco.schools.tests.test_api.SchoolAPITestCase.test_list
```

## Test settings

[config/settings/dev_test.py](../../config/settings/dev_test.py) layers over `base.py`:

| Setting | Value |
|---|---|
| `DEBUG` | `False`, including template debug — for speed |
| `SECRET_KEY` | `'test_key'` default |
| `ALLOWED_HOSTS` | `['*']` |
| Databases | **All three** — `default`, `read_only_database`, `gigameter_database` |

> Tests require all three database connections. `READ_ONLY_DATABASE_URL` and
> `GIGA_METER_DATABASE_URL` are read with `env.db(var=...)` and **have no default**, so they must be
> set or the settings module raises at import. This is the usual reason `manage.py test` fails
> before running a single test.

`base.py` also exposes `UNDER_TEST`, computed as `sys.argv[1] == 'test'`, for code that needs to
behave differently under test.

## Coverage

[.coveragerc](../../.coveragerc):

```ini
[run]
source = proco
omit = *migrations*,*tests*,*proco_data_migrations*,*/proco/*/admin.py,
       *dailycheckapp_contact*,*realtime_dailycheckapp*,*realtime_unicef*,*giga_meter*

[report]
fail_under = 70
```

> **`*giga_meter*` is omitted from coverage measurement**, so the entire Giga Meter app — including
> `fetch_and_aggregate_ping_data`, the cross-database sync, and the Delta Lake backup that deletes
> local data — does not count toward the 70 % threshold. It does have tests (213 lines), they just
> are not measured.
>
> Three other omitted packages (`dailycheckapp_contact`, `realtime_dailycheckapp`,
> `realtime_unicef`) **no longer exist** in the tree. Same stale list as the Sonar exclusions.

## Current test layout

| App | Test files | LOC |
|---|---:|---:|
| `connection_statistics` | 6 | 2 638 |
| `accounts` | 2 | 2 442 |
| `data_sources` | 4 | 1 024 |
| `schools` | 3 | 612 |
| `custom_auth` | 2 | 519 |
| `locations` | 2 | 409 |
| `core` | 3 | 350 |
| `about_us` | 1 | 257 |
| `giga_meter` | 3 | 213 |
| `background` | 2 | 140 |
| `contact` | 1 | 132 |
| **`entities`** | **0** | **0** |
| `utils` | `tests.py` (module, not package) | — |

> **The `entities` app has no tests.** It has a `factories.py` but no `tests/` package — 3 629 lines
> of the newer, generic entity subsystem are untested, including the `v2` API, the type registry,
> and the `entity_name` property that is interpolated into raw SQL. This is the largest coverage gap
> in the repository and the most valuable place to add tests.

## Fixtures and factories

`factory-boy` and `django-fakery` 3.0.0. Factories live in `tests/factories.py` per app — except
`entities`, where `factories.py` sits at the app root
([proco/entities/factories.py](../../proco/entities/factories.py)).

Django fixtures load from `proco/fixtures/` (`FIXTURE_DIRS`).

## Writing tests

### Entity-aware tests need seeded types

`EntityType` rows are created by a management command, not migrations. A test touching entities must
seed them:

```python
from django.core.management import call_command
call_command('seed_entity_types')
```

### Permission tests must cover every method

Because each permission class guards one HTTP method and abstains on the rest
([code-structure.md](code-structure.md#permission-classes-guard-one-method-each)), a test that only
exercises `GET` proves nothing about `DELETE`. Test each method a view exposes, with and without the
relevant slug.

### Task tests

Call the task function directly rather than through `.delay()`. `current_task.request.id` is `None`
outside a worker and the code falls back to `uuid.uuid4()`, so the `BackgroundTask` guard still
applies — clear conflicting rows in `setUp`, or the task will no-op and the test will pass
vacuously.

### Cross-database tests

`giga_meter` models resolve to `gigameter_database`. Django's test runner creates test databases for
every alias in `DATABASES`; a `TestCase` that touches multiple aliases needs `databases = '__all__'`
(or the relevant set) or Django blocks the query.

## CI

[azure-pipeline-dev.yml](../../azure-pipeline-dev.yml) runs `runtests.sh` — but:

> - **only on `develop`** (`condition: eq(variables['Build.SourceBranch'], 'refs/heads/develop')`)
> - **with `continueOnError: true`**
>
> So a failing test suite does not fail the build, and pushes to `staging` do not run tests at all.
> Treat green CI as "the image built", not "the tests passed", and run the suite locally before
> merging.

Coverage is uploaded to SonarQube from `coverage.xml`.

## Related

- [code-structure.md](code-structure.md)
- [../08-operations/deployment.md](../08-operations/deployment.md)
