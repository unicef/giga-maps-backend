# User flows

End-user journeys through the public application, traced from the frontend route to the backend
code that serves it.

Routes are taken from
[src/core/routes.ts](https://github.com/unicef/giga-maps-frontend/blob/staging/src/core/routes.ts) on the frontend `staging`
branch; endpoints from the effector effects under `src/@/*/effects/` and `src/api/`.

---

## Route map

### Map section

| Route | Flow | Key endpoints |
|---|---|---|
| `/` | Redirects to `/map` | — |
| `/map` | [Global map](map-exploration.md) | `statistics/global-stat/`, `locations/countries/`, `accounts/layers/PUBLISHED/` |
| `/map/countries` | [Country list](map-exploration.md) | `locations/countries/`, `locations/search-countries/` |
| `/map/country/:code` | [Country view](country-view.md) | `locations/countries/{code}/`, `statistics/global-stat/?country_id=`, `accounts/adv_filters/PUBLISHED/{country_id}/` |
| `/map/schools` | [School browsing](school-detail.md) | `locations/schools/school/`, `locations/schools/tiles/*` |
| `/map/tour` | Product tour | — (client-side) |

### Content section

| Route | Flow | Key endpoints |
|---|---|---|
| `/about` | About page | `about_us/about_us/`, `about_us/slide_image/` |
| `/media`, `/country-progress`, `/join-us`, `/privacy`, `/daily-check-app` | Static content | mostly client-side |

### API documentation section

| Route | Flow | Key endpoints |
|---|---|---|
| `/docs` | [API docs home](api-documentation.md) | `accounts/apis/` |
| `/docs/explore-api` | API explorer | `accounts/apis/`, `accounts/api_categories/` |
| `/docs/api-keys` | [API key request](api-key-request.md) | `accounts/api_keys/`, `accounts/api_keys/{id}/request_extension/` |
| `/docs/api/:apiKey` | Per-API reference | `accounts/apis/` |

---

## Flows

| Document | Covers |
|---|---|
| [map-exploration.md](map-exploration.md) | Landing on the map, global statistics, vector tiles, zoom behaviour |
| [country-view.md](country-view.md) | Drilling into a country, admin boundaries, per-country statistics |
| [school-detail.md](school-detail.md) | A single school, its history, its connectivity timeline |
| [search.md](search.md) | Country and school search, Azure Cognitive Search, the unified index |
| [filters-and-layers.md](filters-and-layers.md) | Applying advanced filters and switching data layers |
| [time-player.md](time-player.md) | Replaying connectivity over time |
| [downloads.md](downloads.md) | CSV export of countries and schools |
| [api-documentation.md](api-documentation.md) | Browsing the public API catalogue |
| [api-key-request.md](api-key-request.md) | Requesting, using and extending an API key |
| [contact.md](contact.md) | The contact form |

---

## Shared mechanics

### Authentication is optional for most of the map

The map, country views, school details, search, tiles and downloads are all reachable
anonymously. Authentication (Azure AD B2C) is needed only for API key management and the admin
console.

### Everything public is cached

Public read endpoints pass through `SoftCacheManager`. A user hitting a stale entry gets the stale
value immediately while a Celery task refreshes it in the background — so the first user after an
expiry does not pay for the rebuild. See
[../08-operations/caching-and-performance.md](../08-operations/caching-and-performance.md).

### Heavy reads go to the replica

A whitelist of URL names is routed to `read_only_database` by request-scoped middleware
([config/settings/base.py:545](../../config/settings/base.py#L545)):

```
global-stat · get-time-player-data · get-latest-week-and-month
list-published-advance-filters · list-published-data-layers · info-data-layer
download-schools · download-countries · search-countries-admin-schools
search-countries-admin-entities · tiles-view
```

Four names are **commented out** of that list — `tiles-connectivity-view`,
`tiles-school-connectivity-status-view`, `map-data-layer`, `get-time-player-data-v2`. The two tile
endpoints do not need it: tile SQL connects to `connections[settings.READ_ONLY_DB_KEY]` directly
([proco/schools/api.py:182](../../proco/schools/api.py#L182)), bypassing the router entirely.
`map-data-layer` and the v2 time player, however, genuinely run against the **primary** database.

### `?expand=` everywhere

`drf-flex-fields` lets the client choose which nested objects to inline. The frontend always sends
an explicit `expand` list. Fields not in a viewset's `permit_list_expands` are silently ignored.

### Pagination

`unicef_restlib.pagination.DynamicPageNumberPagination`, default page size **10**. The frontend
overrides it per call with `page_size`. There is no hard maximum in the pagination class itself —
per-endpoint sampling limits (`COUNTRY_MAP_API_SAMPLING_LIMIT`, `ADMIN_MAP_API_SAMPLING_LIMIT`) are
the real bound on large responses, and both default to `None`.
