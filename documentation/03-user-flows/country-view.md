# Country view

`/map/country/:code` — the detail page for a single country.

**Backend**: `CountryViewSet` ([proco/locations/api.py:57](../../proco/locations/api.py#L57))

---

## What loads

| Call | Purpose |
|---|---|
| `GET /api/locations/countries/{code}/` | Country detail |
| `GET /api/statistics/global-stat/?country_id={id}` | Country statistics |
| `GET /api/accounts/adv_filters/PUBLISHED/{country_id}/?expand=column_configuration&ordering=name` | Available filters |
| `GET /api/accounts/layers/PUBLISHED/` | Available data layers |
| `GET /api/locations/country-admin-metadata/` | Admin1/Admin2 boundaries |
| `GET /api/locations/schools/tiles/connectivity/?…&country_id={id}` | Vector tiles, scoped |

All but the tiles are **pre-warmed nightly** with exactly these query strings
([proco/utils/tasks.py:83](../../proco/utils/tasks.py#L83)). Calling them with a different `expand`
or `ordering` misses the warm cache entirely.

## Lookup is by code, case-insensitively

```python
def get_object(self):
    return get_object_or_404(
        self.queryset.annotate(code_lower=Lower('code')),
        code_lower=self.kwargs.get('pk').lower(),
    )
```

So `/map/country/BR`, `/br` and `/Br` all resolve. The cache warmer uses `country.code.lower()` for
the same reason.

> `Country.code` has **no unique constraint** ([../02-domain-model/data-model.md](../02-domain-model/data-model.md#country)).
> `get_object_or_404` on a non-unique field raises `MultipleObjectsReturned` → `500`, not `404`, if
> two countries ever share a code.
>
> The `Lower('code')` annotation also means this lookup **cannot use a plain index on `code`**. A
> functional index on `LOWER(code)` would help; there is none.

## Query-time optimisations

```python
queryset = Country.objects.all().select_related('last_weekly_status')

def get_queryset(self):
    return super().get_queryset().defer('geometry')
```

`defer('geometry')` is important — country geometries are large PostGIS polygons, and the list view
would otherwise serialise all of them. Boundaries are fetched separately from
`/api/locations/countries-boundary/` when actually needed.

`pagination_class = None` — the country list is returned whole, which is reasonable for ~200 rows.

## Caching

`CachedListMixin` and `CachedRetrieveMixin` with distinct prefixes:

| Prefix | Endpoint |
|---|---|
| `COUNTRIES_LIST` | list |
| `COUNTRY_INFO` | detail |

Both go through the soft cache, so a stale entry is served while a refresh is queued.

## Filters on the country list

`filter_queryset` supports three boolean query parameters, each of which runs a **subquery over a
large table**:

| Parameter | Effect |
|---|---|
| `has_schools=true` / `false` | Countries with / without at least one school |
| `has_school_master_records=true` | Countries with pending (unpublished, undiscarded) master rows |
| `has_api_requests=true` | Countries referenced by at least one API key |

```python
queryset.filter(id__in=list(
    School.objects.all().values_list('country_id', flat=True)
        .order_by('country_id').distinct('country_id')))
```

> Each of these materialises the full distinct list **into Python** (`list(...)`) and passes it as an
> `IN` clause, rather than using a subquery or `EXISTS`. On the schools table that is a distinct scan
> over millions of rows on every uncached request.
>
> `has_school_master_records` is the admin console's country picker for the review queue, so it is
> not on the public hot path — but `has_schools` is used by the map. Both are candidates for
> `Exists()` subqueries.

Note the commented-out `is_read=True` in the master-records filter, with the explanation
*"There are records where status manually updated but is_read = false"* — a deliberate relaxation to
catch rows edited outside the normal flow.

## `mark-as-joined`

`POST /api/locations/mark-as-joined/` (`MarkAsJoinedViewSet`,
[proco/locations/api.py:804](../../proco/locations/api.py#L804)) records that a country has joined
Project Connect, which corresponds to the `JOINED` value of
`CountryWeeklyStatus.integration_status`.

Remember the ladder's numeric values are **not** in ladder order — see
[../00-overview/glossary.md](../00-overview/glossary.md#integration-status).

## Admin boundaries

`CountryAdminMetadata` is a self-referencing tree with `adm0`/`adm1`/`adm2` levels. The UI label for
each level comes from `description_ui_label` (default `'Admins'`), so a country can call its Admin1
level "States", "Provinces" or "Regions".

```bash
pipenv run python manage.py populate_admin_ui_labels -at=admin1
```

If labels show as "Admins", that command has not been run for the country.

## Failure modes

| Symptom | Cause |
|---|---|
| `404` on a valid code | `code` mismatch, or the country is soft-deleted |
| `500` on a country page | Duplicate `code` values → `MultipleObjectsReturned` |
| Country page slow, map fast | Uncached `has_schools` filter, or the detail cache is cold |
| Filters missing | Not published for this country, or the warm cache key does not match the request |
| Boundaries missing | `CountryAdminMetadata` not loaded — run `load_country_admin_data` |
| Admin level labelled "Admins" | `populate_admin_ui_labels` not run |

## Related

- [map-exploration.md](map-exploration.md)
- [filters-and-layers.md](filters-and-layers.md)
- [../05-background-jobs/cache-warming.md](../05-background-jobs/cache-warming.md)
