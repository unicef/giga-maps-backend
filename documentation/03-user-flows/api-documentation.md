# API documentation pages

The public-facing documentation for Giga's data APIs, served by the frontend at `/docs`.

| Route | Purpose |
|---|---|
| `/docs` | Landing |
| `/docs/explore-api` | Browse the catalogue |
| `/docs/api-keys` | Request and manage keys |
| `/docs/api/:apiKey` | Per-API reference |

---

## Where the content comes from

The catalogue is **data, not code**. `API` records
([proco/accounts/models.py:13](../../proco/accounts/models.py#L13)) drive every page:

| Field | Shown as |
|---|---|
| `name`, `description` | Title and summary |
| `code` | Stable identifier used in URLs and by `create_api_key_with_write_access` |
| `category` | `public` or `private` — **default `private`** |
| `documentation_url` | Link to full documentation |
| `download_url` | The export endpoint a key for this API unlocks |
| `report_title` | CSV filename template |
| `default_filters` | JSON — filters applied to every request with a key for this API |

Endpoints:

```
GET /api/accounts/apis/            # the catalogue
GET /api/accounts/api_categories/  # grouping
```

`APICategory` records group APIs, with an `is_default` flag used as the fallback when a key has no
explicit category
([proco/accounts/api.py:376](../../proco/accounts/api.py#L376)).

## Seeding

```bash
pipenv run python manage.py load_api_data --api-file ./proco/core/resources/all_apis.tsv
# or, for non-production
pipenv run python manage.py load_api_data --api-file ./proco/core/resources/all_apis-dev.tsv
```

> **There is no admin UI for the API catalogue.** Adding or changing an API is a TSV edit plus a
> management command run. If a documentation page shows stale information, the TSV is the source of
> truth, not the database.
>
> Note there are separate production and dev TSVs; they can and do diverge.

## `download_url` is load-bearing

`API.download_url` is not just a link. The download mixin resolves it and compares the resulting
view name against the view being called
([proco/core/mixins.py:76](../../proco/core/mixins.py#L76)):

```python
download_url = urlsplit(str(valid_api_key.api.download_url)).path
if not is_valid_path(download_url) or \
   resolve(download_url).view_name != request.resolver_match.view_name:
    raise core_exceptions.InvalidAPIKeyError()
```

> So an API whose `download_url` is wrong, unset, or points at a non-existent path makes **every
> download with a key for that API fail** with `InvalidAPIKeyError` — an error message that does not
> hint at the real cause. When a user reports a key that "does not work", check the API's
> `download_url` resolves.

## Public vs private

`category` defaults to `private`. Public APIs get a generated CSV filename when `report_title` is
blank:

```python
report_file_name = str('_'.join([api.name, api.category, '{dt}']))
```

Private APIs with a blank `report_title` do not get that fallback.

## Related

- [api-key-request.md](api-key-request.md)
- [downloads.md](downloads.md)
- [../07-api-reference/endpoint-index.md](../07-api-reference/endpoint-index.md)
