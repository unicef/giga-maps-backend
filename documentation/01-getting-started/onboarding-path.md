# Onboarding path

A reading and doing order for someone new to this codebase. Roughly **day 1 → week 1**.

The single most important thing to internalise early: **there are two parallel systems for the same
domain concept** — `School` and `Entity`. Almost every confusing thing in this repository traces
back to that. Read [../02-domain-model/entities.md](../02-domain-model/entities.md) before you touch
anything.

---

## Day 1 — Get oriented and get it running

**Read (about 45 minutes)**

1. [../00-overview/architecture.md](../00-overview/architecture.md) — the shape of the system
2. [../00-overview/glossary.md](../00-overview/glossary.md) — skim; come back to it constantly
3. [../00-overview/repo-map.md](../00-overview/repo-map.md) — note that views live in `api.py`, not
   `views.py`

**Do**

4. [local-setup.md](local-setup.md) — get it running. Budget time for the geo libraries.
5. Seed data, in the documented order. A fresh database serves empty responses everywhere, which
   looks like a broken install.
6. Verify:
   ```bash
   curl -s localhost:8000/health/
   curl -s localhost:8000/api/locations/
   ```

**Checkpoint** — you can list countries and the admin console loads.

### If setup fights you

| Symptom | Cause |
|---|---|
| Backend loops on "DB not ready yet" | The compose readiness check waits for a database called `gigameter` |
| `django.contrib.gis` import errors | GDAL/GEOS missing; on macOS you may need `GDAL_LIBRARY_PATH` |
| `ImportError: delta_sharing` | Installed without `--dev` |
| Empty map, no data | Seeding not done |

---

## Day 2 — Follow one request end to end

**Read**

1. [../03-user-flows/README.md](../03-user-flows/README.md)
2. [../03-user-flows/map-exploration.md](../03-user-flows/map-exploration.md)
3. [../07-api-reference/conventions.md](../07-api-reference/conventions.md)

**Do**

4. Open the frontend against your backend, load the map, and watch the network tab.
5. Trace `GET /api/statistics/global-stat/` from `config/urls.py` → `api_urls.py` → `api.py` →
   the serializer.
6. Do the same for a tile request and read
   [proco/schools/api.py:134](../../proco/schools/api.py#L134) — `BaseTileGenerator` is where the
   map actually comes from.

**Checkpoint** — you can explain what happens between a user opening `/map` and seeing dots, including
where the cache and the read replica come in.

---

## Day 3 — The data model

**Read**

1. [../02-domain-model/data-model.md](../02-domain-model/data-model.md)
2. [../02-domain-model/entities.md](../02-domain-model/entities.md) — the important one
3. [../02-domain-model/databases-and-routing.md](../02-domain-model/databases-and-routing.md)

**Do**

4. Open a shell and look at real rows:
   ```bash
   pipenv run python manage.py shell -c "
   from proco.schools.models import School
   from proco.connection_statistics.models import SchoolWeeklyStatus
   s = School.objects.first()
   print(s.name, s.connectivity_status, s.last_weekly_status_id)
   print(SchoolWeeklyStatus.objects.filter(school=s).values('year','week','connectivity_speed')[:5])
   "
   ```
5. Note which attributes live on `School` and which on `SchoolWeeklyStatus`. This trips everyone up.

**Checkpoint** — you can say why `School.connectivity_status` exists when the same information is in
`SchoolWeeklyStatus`, and what refreshes it.

---

## Day 4 — Ingestion and background jobs

**Read**

1. [../05-background-jobs/README.md](../05-background-jobs/README.md) — the schedule
2. [../05-background-jobs/celery-architecture.md](../05-background-jobs/celery-architecture.md)
3. [../05-background-jobs/ingestion-master-data.md](../05-background-jobs/ingestion-master-data.md)
4. [../04-admin-flows/school-master-review-publish.md](../04-admin-flows/school-master-review-publish.md)

**Do**

5. Read [proco/taskapp/__init__.py](../../proco/taskapp/__init__.py) top to bottom — all 25 jobs in
   one file.
6. Look at the `BackgroundTask` table and understand the guard pattern.

**Checkpoint** — you can explain why a published master-data row can take up to four hours to appear
on the map, and why a crashed worker can block a job for an hour.

---

## Day 5 — Auth and the admin console

**Read**

1. [../04-admin-flows/auth-and-rbac.md](../04-admin-flows/auth-and-rbac.md)
2. [../04-admin-flows/README.md](../04-admin-flows/README.md)
3. [../04-admin-flows/data-layers.md](../04-admin-flows/data-layers.md)

**Do**

4. Create a custom role with a narrow permission set, assign it, and observe what disappears.
5. Read `ProcoBasePermission.has_permission`
   ([proco/core/permissions.py:10](../../proco/core/permissions.py#L10)) until the
   one-method-per-class pattern is obvious.

**Checkpoint** — you can explain why a viewset lists four permission classes, and what happens if you
add a method without adding one.

---

## Week 1, day 2 — Operations

**Read**

1. [../08-operations/caching-and-performance.md](../08-operations/caching-and-performance.md)
2. [../08-operations/runbooks.md](../08-operations/runbooks.md) — skim now, return under pressure
3. [../08-operations/management-commands.md](../08-operations/management-commands.md)

**Checkpoint** — you know what data is recoverable and what is not. Specifically: raw QoS data
survives **less than a day**.

---

## Week 1, day 3 — Contribute

**Read**

1. [../09-contributing/code-structure.md](../09-contributing/code-structure.md)
2. [../09-contributing/testing.md](../09-contributing/testing.md)

**Do**

3. Run the suite:
   ```bash
   ./scripts/runtests.sh
   pipenv run flake8 .      # runtests.sh has this step commented out
   pipenv run isort . --check-only --rr
   ```
4. Pick up something small. **The `entities` app has no tests at all** — 3 629 lines uncovered. That
   is the highest-value place to start and a good way to learn the newer subsystem.

---

## Things that will confuse you, listed up front

| Surprise | Explanation |
|---|---|
| Views are in `api.py` | Project convention. Also `api_urls.py`, not `urls.py` |
| Two of everything | The School/Entity split — [../02-domain-model/entities.md](../02-domain-model/entities.md) |
| `DELETE` does not delete | Soft delete via a `deleted` timestamp |
| Unique constraints declared twice | One partial, one full — required by soft delete |
| Permission classes that return `True` | Each guards one HTTP method and abstains on the rest |
| Stale data served on purpose | Soft cache returns stale and refreshes in the background |
| A "completed" background task that did nothing | `update_live_data` queues a chain and returns |
| A job that silently does not run | A `BackgroundTask` row for the same time bucket exists |
| Empty API responses on a fresh install | Seed data not loaded |
| `manage.py test` fails before running | `READ_ONLY_DATABASE_URL` / `GIGA_METER_DATABASE_URL` unset |
| Green CI with failing tests | CI runs tests only on `develop`, with `continueOnError: true` |
| A country stops updating with no error | Per-country exceptions are logged as warnings |

## Where to ask

The code is the source of truth, and these docs cite it with clickable
`file.py:line` references. When a doc and the code disagree, **the code is right** — please fix the
doc.
