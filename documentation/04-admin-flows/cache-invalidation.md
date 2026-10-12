# Cache invalidation

Forcing the public cache to refresh — after publishing a layer, activating a filter, or correcting
data that users are still seeing stale.

| Endpoint | Method | Scope |
|---|---|---|
| `/api/accounts/invalidate-cache/` | `GET` | Everything |
| `/api/accounts/invalidate-cache-patterns/` | `DELETE` | Everything, or targeted patterns |
| `/api/v2/entities/invalidate-cache-patterns/` | `DELETE` | Entity keys |

Permission: `CanCleanCache` ([proco/core/permissions.py:268](../../proco/core/permissions.py#L268)).

---

## Soft vs hard

Both endpoints take `?hard=true|false`, defaulting to `settings.INVALIDATE_CACHE_HARD`:

```python
if request.query_params.get('hard', settings.INVALIDATE_CACHE_HARD).lower() == 'true':
    cache_manager.invalidate(hard=True)
    message = 'Cache cleared. Map is updated in real time.'
else:
    cache_manager.invalidate()
    message = 'Cache invalidation started. Maps will be updated in a few minutes.'
```

| Mode | Behaviour | User impact |
|---|---|---|
| **Soft** (default) | Mark entries `invalidated=True` | Users keep getting **stale** data until refreshes land — no latency spike |
| **Hard** | `DELETE` the keys | Next request is a **cold miss** and blocks |

The response messages describe the trade-off accurately: hard is "updated in real time", soft is
"updated in a few minutes".

> **Prefer soft during working hours.** A hard invalidation of everything makes every subsequent
> request a cold database read until the warmer catches up, and the warmer's per-country synchronous
> calls take a long time.
>
> `INVALIDATE_CACHE_HARD` is a **string**, compared with `.lower() == 'true'`. A Python boolean
> raises `AttributeError`.

## Full invalidation also re-warms

```python
update_all_cached_values.delay()
update_all_entity_cached_values.delay()
```

Both fire — including the school-side task that is **commented out of the beat schedule**. So a
manual full invalidation runs the legacy warmer as well as the entity one. That is probably
intentional belt-and-braces, but it doubles the work and both tasks will contend for the same three
worker slots.

Note neither is called with `clean_cache=True`, so the warmers fill rather than clear — correct
here, since the endpoint has already invalidated.

## Targeted invalidation

`DELETE /api/accounts/invalidate-cache-patterns/` with a JSON body:

```json
{"key": "country", "id": 5, "code": "br"}
```

```json
{"key": "layer", "id": 42}
```

### Country patterns

```python
keys = [
    "*COUNTRIES_LIST_",
    "*PUBLISHED_LAYERS_LIST_*",
    "*GLOBAL_COUNTRY_SEARCH_MAPPING_",
    "*country_id_\\['{0}'\\]*".format(country_id),
    "*country_id_{0}*".format(country_id),
    "*COUNTRY_INFO_pk_{0}".format(country_code),
]
```

### Layer patterns

```python
keys = [
    "*PUBLISHED_LAYERS_LIST_*",
    "*DATA_LAYER_INFO_{0}*".format(layer_id),
    "*DATA_LAYER_MAP_{0}*".format(layer_id),
    "*layer_id_\\['{0}'\\]*".format(layer_id),
    "*layer_id_{0}*".format(layer_id),
]
```

Two patterns per id in each set — one for the bare value and one for the list form
`country_id_['5']`, because cache keys are built from `request.query_params` where a repeated
parameter arrives as a list. That escaping is fragile but necessary given how the keys are
constructed.

> **`{"key": "all"}` — or an empty body — falls through to the full invalidation path**, including
> firing both warmers. The default when no body is sent is `'all'`. Be deliberate about the payload.

Only `country` and `layer` are recognised. Any other `key` value produces an **empty `keys` list**,
so `invalidate_many([])` does nothing and the endpoint still returns a success message. A typo in
the key name is silently a no-op.

## What invalidation costs

Both soft and hard modes call `cache.keys(pattern)` internally
([proco/utils/cache.py:37](../../proco/utils/cache.py#L37)), which is a Redis **`KEYS`** scan over
the keyspace — a blocking O(N) operation.

> Targeted invalidation issues **one `KEYS` scan per pattern** — six for a country, five for a
> layer. On a large Redis this is noticeably worse than a single full scan. Prefer one full soft
> invalidation over several targeted ones if you need to clear more than a couple of things.

## When you need this

| Situation | Action |
|---|---|
| Published a data layer | Targeted: `{"key": "layer", "id": …}` |
| Activated a filter for a country | Targeted: `{"key": "country", "id": …, "code": …}` |
| Corrected country data | Targeted country |
| Bulk-published master data | Full soft invalidation |
| Post-deploy, everything stale | Trigger the warmer instead — see [../08-operations/runbooks.md](../08-operations/runbooks.md#8-everything-is-slow-after-a-deploy) |

Publishing through the admin console does **not** invalidate automatically. A newly published layer
or filter will not appear until the cache turns over, which by default means the 04:45 UTC warm.
That is the single most common "I published it but it is not showing" cause.

## Related

- [../08-operations/caching-and-performance.md](../08-operations/caching-and-performance.md)
- [../05-background-jobs/cache-warming.md](../05-background-jobs/cache-warming.md)
- [data-layers.md](data-layers.md)
