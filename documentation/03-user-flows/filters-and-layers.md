# Filters and layers

What the map draws (**data layers**) and how users narrow it (**advanced filters**). Both are
admin-configured; this page covers the consumption side.

Admin side: [../04-admin-flows/data-layers.md](../04-admin-flows/data-layers.md),
[../04-admin-flows/advanced-filters.md](../04-admin-flows/advanced-filters.md).

---

## Data layers

### Fetching

```
GET /api/accounts/layers/PUBLISHED/
GET /api/accounts/layers/{id}/info/?country_id=&start_date=&end_date=&is_weekly=true&benchmark=global
GET /api/accounts/layers/{id}/map/
GET /api/accounts/layers/{id}/metadata/
```

| Endpoint | Purpose | Replica |
|---|---|---|
| `layers/{status}/` | Published layers | ✓ (`list-published-data-layers`) |
| `layers/{id}/info/` | The data the map draws | ✓ (`info-data-layer`) |
| `layers/{id}/map/` | Map-shaped payload | ✗ — **commented out** of the whitelist |
| `layers/{id}/metadata/` | Legend, benchmarks | — |

> `map-data-layer` runs against the **primary** database. If it is on a hot path, that is worth
> revisiting — but check why it was removed from the whitelist first; replica lag making a freshly
> published layer appear missing is the likely reason.

### Types and categories

| Dimension | Values |
|---|---|
| `type` | `LIVE`, `STATIC` |
| `category` | `CONNECTIVITY`, `COVERAGE` |
| `is_reverse` | Inverts the legend — lower is better, e.g. latency |

### Benchmarks

`benchmark=global` uses `DataLayer.global_benchmark`; a country-specific benchmark comes from
`Country.benchmark_metadata`. The benchmark is what splits schools into good / moderate / no
connectivity, and it appears directly in the tile SQL:

```sql
WHEN c.connectivity_speed >  {benchmark} THEN 'good'
WHEN c.connectivity_speed <= {benchmark} THEN 'moderate'
```

So switching benchmark changes the colours without changing the underlying data.

### Which layer loads by default

`DataLayerCountryRelationship.is_default` decides the layer pre-selected for a country. The same
flag drives cache warming:

```python
Q(is_default=True) | Q(is_default=False,
                       data_layer__category=LAYER_CATEGORY_CONNECTIVITY,
                       data_layer__created_by__isnull=True)
```

> Warmed layers are the country default **plus system-created connectivity layers**. An
> **admin-created layer that is not the country default is never warmed** and will be consistently
> slower for users, with nothing in the UI to explain why.

## Advanced filters

### Fetching

```
GET /api/accounts/adv_filters/PUBLISHED/{country_id}/?expand=column_configuration&ordering=name
GET /api/accounts/adv_filters/{country_id}/all/
GET /api/accounts/column_configurations/{id}/choices/
```

> The first of these is **warmed nightly with exactly that query string**
> ([proco/utils/tasks.py:88](../../proco/utils/tasks.py#L88)). A request with a different `expand`,
> a different `ordering`, or any extra parameter is a **complete cache miss**. If the frontend's
> query string ever changes, the warmer must change with it.

### Widget types

| `type` | UI | Intended `query_param_filter` |
|---|---|---|
| `DROPDOWN` | Single select | `exact` / `iexact` |
| `DROPDOWN_MULTISELECT` | Multi select | `in` |
| `RANGE` | Min/max | `range` |
| `INPUT` | Free text | `contains` / `icontains` |
| `BOOLEAN` | Yes/No | `on` |

> The widget type and the lookup are **independent fields with no cross-validation**. A `RANGE`
> widget configured with `iexact` saves cleanly and fails at query time. If a filter returns
> nonsense, check the pairing before suspecting the data.

### Dropdown choices

`GET /api/accounts/column_configurations/{id}/choices/` returns live values from the database rather
than a static list, so a dropdown reflects what actually exists.

## How filters reach the tiles

Filter parameters are passed through to the tile endpoints and converted to SQL by
`core_utilities.get_filter_sql(request, 'schools', 'schools_school')`, whose output is concatenated
into the tile query ([proco/schools/api.py:455](../../proco/schools/api.py#L455)):

```python
school_filters = core_utilities.get_filter_sql(request, 'schools', 'schools_school')
if len(school_filters) > 0:
    tbl['school_condition'] += ' AND ' + school_filters
```

A second call handles `school_static` filters against `connection_statistics_schoolweeklystatus`,
adding a `LEFT OUTER JOIN` only when such a filter is present.

> This is part of the **unparameterised SQL** construction described in
> [map-exploration.md](map-exploration.md#unparameterised-sql-on-an-unauthenticated-endpoint).
> `get_filter_sql` generates SQL from request parameters on an endpoint requiring no authentication.

## Tile cache keys include filters

`ConnectivityTileRequestHandler.get_cache_key` builds the key from **every** query parameter except
`cache`, sorted. So:

- Each distinct filter combination is a separate cache entry — correct, but it means a user applying
  an unusual filter set always pays a cold miss.
- Parameter ordering does not matter, since they are sorted.
- An extra parameter — a cache-buster, a tracking value — creates a new entry.

## Entity equivalents

| Schools | Entities |
|---|---|
| `/api/accounts/layers/…` | `/api/v2/entities/layers/…` |
| `/api/accounts/adv_filters/…` | `/api/v2/entities/filters/…` |
| `/api/accounts/column_configurations/…` | `/api/v2/entities/column_configurations/…` |
| `/api/locations/schools/tiles/…` | `/api/v2/entities/tiles/…` |

`DataLayer`, `AdvanceFilter` and `ColumnConfiguration` each carry a nullable `entity_type` FK, where
`NULL` means legacy school-only.

## Failure modes

| Symptom | Cause |
|---|---|
| A filter does not appear | Not published, or not activated for that country |
| A filter returns nonsense | Widget type and `query_param_filter` mismatch |
| A dropdown is empty | `options` not populated, or no live values in `choices/` |
| One layer consistently slow | Admin-created and not the country default — not warmed |
| Filters slow to load | Query string does not match the warmer's exact key |
| Colours change unexpectedly | Benchmark switched between global and country |

## Related

- [../04-admin-flows/data-layers.md](../04-admin-flows/data-layers.md)
- [../04-admin-flows/advanced-filters.md](../04-admin-flows/advanced-filters.md)
- [../05-background-jobs/cache-warming.md](../05-background-jobs/cache-warming.md)
