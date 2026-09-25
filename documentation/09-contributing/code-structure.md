# Code structure and conventions

## Where things go

Every app follows the same layout. The naming is **not** Django's default — learn it once:

| File | Django default | Here |
|---|---|---|
| Views | `views.py` | **`api.py`** |
| URLs | `urls.py` | **`api_urls.py`** |
| Serializers | — | `serializers.py` |
| Celery tasks | — | `tasks.py` |
| Constants | — | `config.py` (an `AppConfig` class) and `constants.py` |
| Filters | — | `filters.py` |
| Exceptions | — | `exceptions.py` |

If you are looking for a view, it is in `api.py`.

## Base classes to inherit

| Need | Use | From |
|---|---|---|
| A model with timestamps + soft delete | `BaseModelMixin` | `proco.core.models` |
| …plus `created_by` / `last_modified_by` | `BaseModel` | same |
| A model fed by an external source | `DataSourceModelMixin` | same |
| …with the review/publish workflow | `MasterDataSourceModelMixin` | same |
| A DRF viewset | `BaseModelViewSet` | `proco.core.viewsets` |
| A permission class | `ProcoBasePermission` | `proco.core.permissions` |
| Big integers (bps) | `PositiveBigIntegerField` | `proco.core.models` |
| Project datetimes | `CustomDateTimeField` | same |

## Conventions that are load-bearing

### Soft delete

Never hard-delete. `DELETE` sets `deleted = now()`; `BaseManager` filters `deleted__isnull=True`.

Declare unique constraints **twice** — once including `deleted`, once partial:

```python
constraints = [
    UniqueConstraint(fields=['code', 'deleted'],
                     name='unique_with_deleted_for_<thing>'),
    UniqueConstraint(fields=['code'], condition=Q(deleted=None),
                     name='unique_without_deleted_for_<thing>'),
]
```

Use `DO_NOTHING` for FKs to users, consistent with users being soft-deleted.

### Permission classes guard one method each

```python
def has_permission(self, request, view):
    if request.method != self.method:
        return True     # abstain
```

So views stack them:

```python
permission_classes = (
    core_permissions.IsUserAuthenticated,
    core_permissions.CanViewThing,     # GET
    core_permissions.CanUpdateThing,   # PUT
    core_permissions.CanDeleteThing,   # DELETE
)
```

> **Adding a method to an existing viewset without adding its permission class leaves that method
> unguarded** beyond authentication. Nothing fails loudly. This is the easiest authorization mistake
> to make in this codebase.

### Celery tasks register themselves

```python
@app.task(soft_time_limit=..., time_limit=...)
def my_task(...):
    task_key = 'my_task_status_{current_time}'.format(
        current_time=format_date(get_current_datetime_object(), frmt='%d%m%Y_%H'))
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Description')
    if task_instance:
        ...
        task_instance.info('progress message')
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.info('Found running Job with "{0}" name so skipping current iteration'.format(task_key))
```

Always pass explicit time limits — the CLI default is 60 s soft.

Wrap the body in `try/finally` so `task_on_complete` runs on failure too.
`fetch_and_aggregate_ping_data` ([proco/giga_meter/tasks.py:806](../../proco/giga_meter/tasks.py#L806))
is the model to copy: it has a `finally`, a real retry policy, and re-raises unexpected errors.

### Logging

```python
logger = logging.getLogger('gigamaps.' + __name__)
```

Use `%`-style or `.format()` — `flake8-logging-format` is enabled. Do **not** swallow an exception
into a `warning` unless the caller can genuinely continue; the existing ingestion code does this and
it is why silent data gaps happen.

### Both sides of the school/entity split

Before changing a school-side model, job or endpoint, check whether an entity counterpart needs the
same change. The mapping table is in
[../02-domain-model/entities.md](../02-domain-model/entities.md#duplicated-infrastructure). The two
implementations are independent code, not generated from a shared source, and have already diverged.

### Raw SQL

Used deliberately for tile generation, bulk cleanup and some aggregation. When you write it:

- **Parameterise.** Use `%s` placeholders and pass a params list — as the master-data cleanup does.
  Do not f-string request values into SQL; the tile generators do this today and it is a known
  defect, not a pattern to follow.
- Prefer `connections[settings.READ_ONLY_DB_KEY]` for heavy reads, and handle it being unavailable.
- Remember raw SQL bypasses soft delete, `django-simple-history`, and model `save()` overrides.

### Denormalised columns

`School.name_lower`, `education_level_lower`, `school_type_lower` and the status fields are
maintained in `save()`. **`bulk_update()` and `.update()` bypass `save()`** — set them explicitly or
schedule the matching `populate_*` repair command.

## Style

| Tool | Config | Notes |
|---|---|---|
| flake8 + ~15 plugins | [.flake8](../../.flake8) | Includes bandit (security) and dlint |
| isort | [.isort.cfg](../../.isort.cfg) | |
| EditorConfig | [.editorconfig](../../.editorconfig) | |
| Quotes | `flake8-quotes` | Single quotes |
| Commas | `flake8-commas` | Trailing commas required |
| Commented code | `flake8-eradicate` | Flags commented-out code |

Run them yourself — `scripts/runtests.sh` has both steps **commented out**:

```bash
pipenv run flake8 .
pipenv run isort . --check-only --rr
```

> `flake8-eradicate` would flag a great deal of existing commented-out code
> ([web-worker.sh](../../web-worker.sh), the beat schedule, the replica whitelist). That is probably
> why the step is disabled. Do not add more.

## Adding things

### A new endpoint

1. View in `api.py`, inheriting `BaseModelViewSet`.
2. Route in `api_urls.py` with an explicit `name=` — the name is used by `reverse()`, the cache
   warmer, and the replica whitelist.
3. Permission classes for **every** method the view exposes.
4. `permit_list_expands` for any nested serializers.
5. If it is a heavy GET, consider `READ_ONLY_DATABASE_ALLOWED_REQUESTS` — but only if it tolerates
   replica lag.
6. If the frontend will call it on a hot path, add it to the cache warmer **with the exact query
   string the frontend sends**.
7. Add it to [../07-api-reference/endpoint-index.md](../07-api-reference/endpoint-index.md).

### A new scheduled job

1. Task in the owning app's `tasks.py`, with explicit time limits and the `BackgroundTask` guard.
2. Entry in [proco/taskapp/__init__.py](../../proco/taskapp/__init__.py).
3. Choose a crontab minute that does not collide with the existing ones — the schedule is already
   dense between 00:00 and 05:30 UTC.
4. Restart beat; remember RedBeat keeps removed entries in Redis.
5. Document it in [../05-background-jobs/README.md](../05-background-jobs/README.md).

### A new model field

1. Migration.
2. Serializer(s).
3. Entity counterpart, if applicable.
4. `ColumnConfiguration` seed data if it should be filterable — it **cannot** be added through the
   API.
5. Search index mapping if it should be searchable.

## Related

- [testing.md](testing.md)
- [../00-overview/repo-map.md](../00-overview/repo-map.md)
