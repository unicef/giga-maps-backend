# API conventions

Cross-cutting behaviour shared by every endpoint. Route list:
[endpoint-index.md](endpoint-index.md).

---

## Base URL and namespacing

Everything lives under `/api/`, grouped by app
([config/urls.py:32](../../config/urls.py#L32)). There is **no global version prefix** — `v2`
appears only for the entity subsystem (`/api/v2/entities/`). The rest is implicitly v1.

## Authentication

Two mechanisms, for two audiences.

### JWT (frontend and admin console)

```
Authorization: Bearer <token>
```

Issued by Azure AD B2C, `RS256`, verified against a public key, 1-day expiry
([config/settings/base.py:388](../../config/settings/base.py#L388)).

A second, independent check applies: `IsUserAuthenticated` rejects any user whose `last_login` is
more than **5 days** old ([proco/core/permissions.py:65](../../proco/core/permissions.py#L65)). Both
failures return a generic `AuthenticationFailed`, so a valid token can still yield `401`.

### API key (public API consumers)

Keys are issued through the request/approval flow in
[../03-user-flows/api-key-request.md](../03-user-flows/api-key-request.md) and validated at
`PUT /api/accounts/validate_api_key/` with `{"api_id": …, "api_key": …}`.

Validation requires: the API is not deleted, id and key match, `status == APPROVED`, and
`valid_to >= today`. Failure returns `404` with `{"detail": "Please enter valid api key."}` — the
same response for wrong, expired and declined keys.

Note that this endpoint itself requires `IsUserAuthenticated`, so it validates a key *for a
logged-in user* rather than acting as anonymous API authentication.

### Anonymous

Most public map endpoints — countries, schools, tiles, statistics, downloads, search, published
layers and filters — require no authentication.

## Authorization

A custom role/permission system with **47 slugs**, not Django's `auth.Permission`. Full detail in
[../04-admin-flows/auth-and-rbac.md](../04-admin-flows/auth-and-rbac.md).

The mechanic that surprises people: **each permission class guards exactly one HTTP method and
returns `True` for all others**, so views stack several classes. A method with no matching class is
unguarded beyond authentication.

## Pagination

`unicef_restlib.pagination.DynamicPageNumberPagination`, default page size **10**
([config/settings/base.py:132](../../config/settings/base.py#L132)).

| Parameter | Effect |
|---|---|
| `page` | Page number |
| `page_size` | Override the page size |

```json
{
  "count": 1234,
  "next": "https://…?page=3",
  "previous": "https://…?page=1",
  "results": [ … ]
}
```

The pagination class imposes no hard maximum on `page_size`. The real bounds on large responses are
`COUNTRY_MAP_API_SAMPLING_LIMIT` and `ADMIN_MAP_API_SAMPLING_LIMIT`, both of which **default to
`None`**.

Viewsets that support it set `apply_query_pagination = True`.

## `expand` — selective nesting

`drf-flex-fields` powers a comma-separated `expand` parameter:

```
GET /api/accounts/layers/?expand=created_by,last_modified_by,published_by
GET /api/accounts/api_keys/?expand=user,api
GET /api/sources/school_master/?expand=school,modified_by,published_by,country
```

Each viewset declares `permit_list_expands`. **A field not on that list is silently ignored** — no
error, just an un-expanded response. Check the viewset before assuming an expansion is broken.

## Filtering, ordering, search

Three backends, per viewset:

| Backend | Parameter |
|---|---|
| `DjangoFilterBackend` | Declared in `filterset_fields` |
| `SearchFilter` | `search=`, across `search_fields` |
| `NullsAlwaysLastOrderingFilter` | `ordering=`, across `ordering_fields` |

`filterset_fields` is a dict of field → lookups:

```python
filterset_fields = {
    'school_name': ['iexact', 'in', 'exact'],
    'status': ['iexact', 'exact', 'in'],
    'country_id': ['exact', 'in'],
}
```

Used as `?status__in=DRAFT,PUBLISHED` or `?school_name__iexact=…`.

`ordering` accepts a comma-separated list with `-` for descending:
`?ordering=-last_modified_at,name`. `NullsAlwaysLastOrderingFilter` keeps `NULL`s at the end
regardless of direction — useful for `valid_to` and similar nullable columns.

## Date and datetime formats

Set globally from `DATE_FORMAT`, `DATETIME_FORMAT`, `DATE_INPUT_FORMATS` and
`DATETIME_INPUT_FORMATS` in [config/settings/base.py:118](../../config/settings/base.py#L118).

`DATE_FORMAT` is **`%d-%m-%Y`** — day-first, which is what API responses render dates as. All
datetimes are UTC (`TIME_ZONE = 'UTC'`, `USE_TZ = True`).

> Management commands are inconsistent with this: the `data_loss_recovery_*` commands take
> `YYYY-MM-DD`, while `create_api_key_with_write_access` takes `DD-MM-YYYY` matching `DATE_FORMAT`.
> Check the specific command before passing a date.

## Caching

Public read endpoints go through `SoftCacheManager`. A stale entry is **returned to the caller**
while a refresh is queued — so a `200` may carry data that is up to a day old.

| Parameter | Effect |
|---|---|
| `cache=off` / `cache=false` | Bypass the cache for this request |

The cache key is built from the **full query string**, so any extra or differently-ordered parameter
is a separate entry. Endpoints warmed nightly are warmed with one exact query string — see
[../05-background-jobs/cache-warming.md](../05-background-jobs/cache-warming.md).

`Cache-Control` headers are applied by `custom_cache_control` with `max_age` from
`CACHE_CONTROL_MAX_AGE_FOR_FE`, and only for listed status codes.

## Soft delete

`DELETE` sets `deleted = now()` rather than removing the row. Listings filter
`deleted__isnull=True` via `BaseManager`.

Unique constraints are therefore declared twice — once including `deleted`, once partial with
`condition=Q(deleted=None)` — so a code or name can be reused after deletion.

Two exceptions worth knowing: the master-data cleanup jobs and `delete_all_related_rows` on publish
both issue **hard `DELETE`s**. See
[../05-background-jobs/cleanup-and-retention.md](../05-background-jobs/cleanup-and-retention.md).

## Errors

Standard DRF shapes:

| Status | Meaning |
|---|---|
| `400` | Validation error, or a malformed bulk-update body |
| `401` | Not authenticated, expired JWT, **or `last_login` older than 5 days** |
| `403` | Authenticated but lacking the permission slug |
| `404` | Not found — also returned for an invalid API key |
| `500` | Unhandled — reported to Sentry |
| `204` | Also returned by tile endpoints on a **database error**, which renders as an empty tile |

Domain-specific errors carry a `{old, new}` context, e.g.
`InvalidSchoolMasterDataRowStatusAtUpdateError` when a status transition is not permitted.

## Bulk operations

Several viewsets accept a list body on `PUT`/`DELETE` to the collection URL:

```json
PUT /api/sources/school_master/
[{"id": 1, "status": "DRAFT_LOCKED"}, {"id": 2, "status": "DRAFT_LOCKED"}]
```

`validate_ids` ([proco/data_sources/api.py:247](../../proco/data_sources/api.py#L247)) rejects
duplicate ids and ids absent from the database, naming the missing ones.

## CORS

`proco.utils.middleware.CustomCorsMiddleware`, driven by `CORS_ALLOW_ORIGINS` (comma-separated).
`django-cors-headers` is installed but its middleware is **commented out**
([config/settings/base.py:90](../../config/settings/base.py#L90)).

Tile responses set `Access-Control-Allow-Origin: *` directly on the response
([proco/schools/api.py:209](../../proco/schools/api.py#L209)), bypassing the middleware — tiles are
readable from any origin by design.

## Compression

`django.middleware.gzip.GZipMiddleware` is **first** in the middleware chain, so every response is
gzipped where the client allows it — including vector tiles, which are already compact binary.

## Rate limiting

**There is none.** No DRF throttle classes are configured, and no throttling middleware is present.
Any rate limiting is at the ingress layer, outside this repository.
