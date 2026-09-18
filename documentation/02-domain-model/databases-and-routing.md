# Databases and routing

Three database connections, three routers, and two different routing strategies. Getting this wrong
is a common source of confusing bugs.

---

## The connections

| Alias | Env var | Contents |
|---|---|---|
| `default` | `DATABASE_URL` | Primary PostgreSQL + PostGIS. All writes |
| `read_only_database` | `READ_ONLY_DATABASE_URL` | Read replica for heavy GETs |
| `gigameter_database` | `GIGA_METER_DATABASE_URL` | The separate Giga Meter database |

## The router chain

[config/settings/base.py:473](../../config/settings/base.py#L473):

```python
DATABASE_ROUTERS = [
    'proco.utils.db_routers.ReadOnlyDBRouter',
    'proco.giga_meter.db_router.GigaMeterDBRouter',
    'dynamic_db_router.DynamicDbRouter',
]
```

Django consults routers **in order** and takes the first non-`None` answer.

| Router | Strategy | Scope |
|---|---|---|
| `ReadOnlyDBRouter` | **Per request**, via thread-local | GETs on a URL-name whitelist |
| `GigaMeterDBRouter` | **Per model**, via app label | Everything in `proco.giga_meter` |
| `DynamicDbRouter` | **Explicit**, via `in_database()` context manager | Where code asks |

Two different mental models coexist. `ReadOnlyDBRouter` answers "which database for *this
request*"; `GigaMeterDBRouter` answers "which database for *this model*". A `giga_meter` model
inside a whitelisted request still goes to `gigameter_database`, because `ReadOnlyDBRouter` returns
`None` unless the thread-local is set — and the thread-local is set for the request, not the model.

---

## Read-replica routing

### The middleware

`CustomRequestDBRouterMiddleware` ([proco/utils/db_routers.py:36](../../proco/utils/db_routers.py#L36))
is **last** in `MIDDLEWARE`, so it runs after authentication:

```python
def process_view(self, request, view_func, args, kwargs):
    if request.method == 'GET':
        url_name = resolve(request.path_info).url_name
        if url_name in settings.READ_ONLY_DATABASE_ALLOWED_REQUESTS:
            THREAD_LOCAL.OVERRIDE_DB_FOR_READ = settings.READ_ONLY_DB_KEY
            logger.info('Using Read-Only DB for request: ...')
        else:
            logger.info('CHECK:: Using Write DB for GET request: ...')

def process_response(self, request, response):
    if hasattr(THREAD_LOCAL, 'OVERRIDE_DB_FOR_READ'):
        del THREAD_LOCAL.OVERRIDE_DB_FOR_READ
    return response
```

Both branches log at **INFO on every GET**, which is useful for diagnosis and noisy in production:

```bash
grep 'Using Read-Only DB for request' /code/logs/*.log
grep 'CHECK:: Using Write DB for GET request' /code/logs/*.log
```

### Caveats of the thread-local approach

> - **Only `GET`.** `POST`/`PUT`/`DELETE` always use `default`, correctly.
> - **Only whitelisted URL names**, compared against the **un-namespaced** `resolve().url_name`. Two
>   apps using the same name (e.g. `global-search-filter` in both `locations` and `entities`) would
>   both match a single whitelist entry.
> - **Not inherited by threads.** Any code that spawns a thread or hands work to an executor inside
>   the request loses the flag and silently reads the primary.
> - **Celery tasks never get it.** `process_view` only runs for HTTP requests, so all background
>   work reads and writes `default` — with the exception of tile SQL, which connects explicitly.
> - `process_response` clears the flag; there is no `process_exception`, but Django's contract still
>   runs `process_response` after an exception is converted to a response.

### The whitelist

[config/settings/base.py:545](../../config/settings/base.py#L545) — **11 active, 4 commented out**:

```python
READ_ONLY_DATABASE_ALLOWED_REQUESTS = [
    'global-stat',
    'get-time-player-data',
    # 'tiles-connectivity-view',
    # 'tiles-school-connectivity-status-view',
    'get-latest-week-and-month',
    'list-published-advance-filters',
    'list-published-data-layers',
    'info-data-layer',
    # 'map-data-layer',
    'download-schools',
    'download-countries',
    'search-countries-admin-schools',
    'search-countries-admin-entities',
    # 'get-time-player-data-v2',
    'tiles-view',
]
```

| Commented out | Effect |
|---|---|
| `tiles-connectivity-view` | None — tile SQL connects to the replica directly |
| `tiles-school-connectivity-status-view` | None — same |
| `map-data-layer` | **Runs on the primary** |
| `get-time-player-data-v2` | **Runs on the primary** |

> `map-data-layer` (`/api/accounts/layers/{id}/map/`) and the v2 time player are map-facing read
> endpoints running against the write database. Adding them back is a one-line change — but find out
> why they were removed first. The most likely reason is replica lag making a freshly published
> layer appear missing, which is a correctness trade-off, not an oversight.

`READ_ONLY_DATABASE_ALLOWED_MODELS` exists and is **empty** — model-level replica routing is
available but unused.

### Tiles bypass the router entirely

```python
with connections[settings.READ_ONLY_DB_KEY].cursor() as cur:
    cur.execute(sql)
```

[proco/schools/api.py:182](../../proco/schools/api.py#L182).

> Tile rendering **fails when the replica is down, even though the primary is healthy**, with no
> fallback. A `DatabaseError` returns `204 No Content`, which Mapbox renders as an empty tile — so
> replica trouble presents as "the map lost all its schools", not as an error. See
> [../08-operations/runbooks.md](../08-operations/runbooks.md#9-the-map-is-empty-but-the-api-works).

### `statement_timeout`

Configured through the connection URL, not settings:

```
READ_ONLY_DATABASE_URL=postgis://…/proco?options=-c statement_timeout=300000
```

Local compose uses 300 s; the commented-out Celery services use 1 800 s. This is the main protection
against one expensive map query pinning a replica connection.

---

## Giga Meter routing

`GigaMeterDBRouter` ([proco/giga_meter/db_router.py](../../proco/giga_meter/db_router.py)) routes by
app label for both reads and writes, and constrains migrations:

```python
def allow_migrate(self, db, app_label, model_name=None, **hints):
    if app_label == app_config.app_name:
        return db == settings.GIGA_METER_DB_KEY
    return db == 'default'
```

> - **Cross-database joins are impossible.** Fetch ids into Python and query each side —
>   `fetch_all_school_map(giga_ids)` is the pattern.
> - **`migrate` must be run per alias.** [web-worker.sh](../../web-worker.sh) runs a plain
>   `manage.py migrate`, targeting `default`. Giga Meter migrations are applied separately.

Full detail: [../06-integrations/giga-meter.md](../06-integrations/giga-meter.md).

---

## Dynamic routing

`django-dynamic-db-router` 0.3.0 is third in the chain, enabling explicit blocks:

```python
from dynamic_db_router import in_database

with in_database('read_only_database'):
    ...
```

Being last, it only applies where the earlier two returned `None`. It is the right tool for
forcing a **Celery task** onto the replica, since the middleware's thread-local never reaches one.

---

## PostGIS extension race

A defensive patch at import time
([proco/utils/db_routers.py:17](../../proco/utils/db_routers.py#L17)) wraps
`PostGISDatabaseWrapper.prepare_database`:

```python
def safe_prepare_database(self):
    try:
        orig_prepare_database(self)
    except InternalError as e:
        if 'tuple concurrently updated' in str(e):
            logger.warning('Concurrent PostGIS extension creation detected; safely proceeding.')
        else:
            raise
```

Two processes running `CREATE EXTENSION postgis` simultaneously — as happens when web and worker
containers start together — produce `tuple concurrently updated`. This swallows exactly that error
and re-raises anything else. The whole block is wrapped in a bare `except Exception: pass`, so on a
non-PostGIS backend it is a no-op.

---

## Choosing a database when writing code

| Situation | Use |
|---|---|
| Any write | `default` — automatic |
| A GET on a new heavy read endpoint | Add its URL name to `READ_ONLY_DATABASE_ALLOWED_REQUESTS` |
| Raw SQL for tiles or similar | `connections[settings.READ_ONLY_DB_KEY]`, and handle the replica being down |
| A Celery task that only reads | `with in_database('read_only_database'):` — the middleware will not help |
| Anything touching `proco.giga_meter` models | Automatic; do not join across |

Before adding a URL name to the whitelist, confirm the endpoint tolerates **replica lag**. Anything
a user reads immediately after writing — a just-published layer, a just-approved key — should stay
on the primary.
