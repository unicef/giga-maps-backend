# Azure Cognitive Search

Powers country, school and entity search. Client: `azure-search-documents` 11.3.0.

## Configuration

[config/settings/base.py:415](../../config/settings/base.py#L415):

```python
ENABLE_AZURE_COGNITIVE_SEARCH = True     # hard-coded, not an env var

if ENABLE_AZURE_COGNITIVE_SEARCH:
    AZURE_CONFIG['COGNITIVE_SEARCH'] = {
        'SEARCH_ENDPOINT': env('SEARCH_ENDPOINT', default=None),
        'SEARCH_API_KEY': env('SEARCH_API_KEY', default=None),
        'COUNTRY_INDEX_NAME': env('COUNTRY_INDEX_NAME', default='giga_countries'),
        'SCHOOL_INDEX_NAME': env('SCHOOL_INDEX_NAME', default='giga_schools'),
        'ENTITIES_INDEX_NAME': env('ENTITIES_INDEX_NAME', default='giga_entities'),
    }
```

`ENABLE_AZURE_COGNITIVE_SEARCH` is a module constant, so search cannot be disabled by
configuration — only by leaving the endpoint and key unset.

| Index | Env var | Code default | `.env_example` | Built by |
|---|---|---|---|---|
| Countries | `COUNTRY_INDEX_NAME` | `giga_countries` | `giga-countries` | — |
| Schools | `SCHOOL_INDEX_NAME` | `giga_schools` | `giga-schools` | `rebuild_school_index` *(disabled)* |
| Entities | `ENTITIES_INDEX_NAME` | `giga_entities` | **absent** | `rebuild_unified_index` |

> **Two problems in that table.**
>
> 1. Code defaults use underscores; `.env_example` uses hyphens. Whichever the real service uses,
>    the two must agree. A mismatch gives an **empty search with no error and no log line** —
>    the client queries an index that does not exist.
> 2. `ENTITIES_INDEX_NAME` is not in `.env_example` at all, so entity search falls back to
>    `giga_entities` regardless of what the service is actually named.
>
> This is the first thing to check for any "search returns nothing" report.

## Consumers

| Endpoint | View |
|---|---|
| `GET /api/locations/gsearch/` | `AggregateSearchViewSet` ([proco/locations/api.py:735](../../proco/locations/api.py#L735)) |
| `GET /api/locations/search-countries/` | `CountrySearchStatListAPIView` ([line 318](../../proco/locations/api.py#L318)) |
| `GET /api/v2/entities/gentity-search/` | `AggregateSearchEntityViewSet` ([proco/entities/api.py](../../proco/entities/api.py)) |

Shared logic is in `BaseSearchMixin` ([proco/locations/api.py:530](../../proco/locations/api.py#L530)).

`search-countries-admin-schools` and `search-countries-admin-entities` are on the read-replica
whitelist and are warmed nightly by the cache warmer.

## Index maintenance

Nightly at **02:00 UTC**, `rebuild_unified_index` performs a **full destructive rebuild**:

```python
call_command('build_unified_index', '--delete_index', '--create_index', '--clean_index', '--update_index')
```

> **Search is unavailable for the duration**, and if the task fails partway the index stays deleted
> or partial until the next night. There is no retry and no alert. Details and the verification
> query are in [../05-background-jobs/search-index.md](../05-background-jobs/search-index.md).

Incremental updates do not cause an outage:

```bash
pipenv run python manage.py build_unified_index --update_index
pipenv run python manage.py build_unified_index --update_index -country_id=144
```

**Prefer `--update_index` alone for recovery.** The full sequence is only needed when the index
schema changes.

## Troubleshooting

| Symptom | Check |
|---|---|
| Search empty, everything else fine | Index name mismatch (hyphen vs underscore); `SEARCH_ENDPOINT`/`SEARCH_API_KEY` set |
| Worked yesterday, empty today | Rebuild failed after `--delete_index` — check `BackgroundTask` for `rebuild_unified_index_status_*` |
| Empty for a window each night | Expected — the 02:00 rebuild |
| New schools not searchable | Index refreshes nightly; run `--update_index` scoped to the country |
| Entities missing, schools fine | `ENTITIES_INDEX_NAME` |

```bash
pipenv run python manage.py shell -c "
from proco.background.models import BackgroundTask
print(list(BackgroundTask.objects.filter(
    name__startswith='rebuild_unified_index'
).order_by('-created_at').values_list('created_at','completed_at','status')[:5]))"
```
