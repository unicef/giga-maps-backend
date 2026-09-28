# Search

Country and school/entity search, backed by Azure Cognitive Search.

| Endpoint | URL name | View |
|---|---|---|
| `GET /api/locations/gsearch/` | `global-search-filter` | `AggregateSearchViewSet` |
| `GET /api/locations/search-countries/` | `search-countries-admin-schools` 🔵 | `CountrySearchStatListAPIView` |
| `GET /api/v2/entities/gentity-search/` | `global-search-filter` | `AggregateSearchEntityViewSet` |

🔵 On the read-replica whitelist and warmed nightly.

---

## Implementation

`BaseSearchMixin` ([proco/locations/api.py:530](../../proco/locations/api.py#L530)) holds the shared
logic; `AggregateSearchViewSet` ([line 735](../../proco/locations/api.py#L735)) composes results
across countries, admin areas and schools. The entity variant lives in
[proco/entities/api.py](../../proco/entities/api.py).

`search-countries` is different in kind: it returns countries **with their statistics attached**,
which is why it is expensive enough to warrant both replica routing and nightly warming.

## The indexes

| Index | Env var | Code default |
|---|---|---|
| Countries | `COUNTRY_INDEX_NAME` | `giga_countries` |
| Schools | `SCHOOL_INDEX_NAME` | `giga_schools` |
| Entities | `ENTITIES_INDEX_NAME` | `giga_entities` |

> **The most common search bug is a name mismatch.** Code defaults use underscores; `.env_example`
> supplies hyphens (`giga-schools`, `giga-countries`), and `ENTITIES_INDEX_NAME` is absent from
> `.env_example` entirely. Querying a non-existent index returns **no results and no error**.
>
> Check this before anything else.

## Freshness

The unified index is rebuilt **once a night at 02:00 UTC**, destructively —
`--delete_index --create_index --clean_index --update_index`.

Two consequences for users:

1. **A school added today is not searchable until tomorrow.** Nothing indexes incrementally on
   write.
2. **Search is unavailable during the rebuild**, and if the rebuild fails partway the index stays
   deleted until the following night.

To make a country searchable immediately without an outage:

```bash
pipenv run python manage.py build_unified_index --update_index -country_id=144
```

`--update_index` alone does not delete the index.

## Lower-cased shadow columns

Database-side search (as opposed to Cognitive Search) uses the `*_lower` columns on `School` —
`name_lower`, `education_level_lower`, `education_level_govt_lower`, `school_type_lower` — each
`db_index=True` and maintained in `save()`.

> `bulk_update()` and `.update()` bypass `save()`, so these can drift. `data_cleanup
> --populate_school_lowercase_fields` exists to repair it, which suggests it has happened. If
> case-insensitive search misses a school that clearly exists, check its `name_lower`.

## Failure modes

| Symptom | Cause |
|---|---|
| No results at all | Index name mismatch, or `SEARCH_ENDPOINT`/`SEARCH_API_KEY` unset |
| Worked yesterday, empty today | Nightly rebuild failed after `--delete_index` |
| Empty for a window each night | Expected — the 02:00 rebuild |
| New schools not found | Index refreshes nightly |
| Entities missing, schools fine | `ENTITIES_INDEX_NAME` |
| A specific school not found by lowercase name | `name_lower` drift |
| `search-countries` slow | Cold cache — it is warmed nightly with one exact query string |

## Related

- [../05-background-jobs/search-index.md](../05-background-jobs/search-index.md)
- [../06-integrations/azure-cognitive-search.md](../06-integrations/azure-cognitive-search.md)
