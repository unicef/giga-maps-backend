# Azure AI Translator

Translates UI text on demand, exposed through a single endpoint.

## Configuration

[config/settings/base.py:565](../../config/settings/base.py#L565):

| Variable | Default | Purpose |
|---|---|---|
| `AI_TRANSLATION_ENDPOINT` | `None` | Service endpoint |
| `AI_TRANSLATION_KEY` | `None` | Subscription key |
| `AI_TRANSLATION_REGION` | `None` | Azure region |
| `AI_TRANSLATION_SUPPORTED_TARGETS` | `[]` | Allowed target languages, as a list |
| `AI_TRANSLATION_CACHE_KEY_LIMIT` | `2000` | Cap on cached translation entries |

`.env_example` suggests `AI_TRANSLATION_CACHE_KEY_LIMIT=10000`, five times the code default.

`AI_TRANSLATION_SUPPORTED_TARGETS` defaults to an **empty list**, which makes every target language
invalid. Unset means the feature is off, not "all languages".

## Endpoint

```
GET /api/accounts/translate/text/<str:target>/
```

`TranslateTextFromEnViewSet` ([proco/accounts/api.py:389](../../proco/accounts/api.py#L389)),
URL name `translate-a-text-to-given-target-language`.

Source language is always English — the view name says `FromEn`. Only the target varies, and it must
appear in `AI_TRANSLATION_SUPPORTED_TARGETS`.

## Caching

Translations are cached in Redis under the **no-expiry** partition
(`NO_EXPIRY_CACHE_PREFIX`, default `NO_EXPIRY_CACHE`), bounded by
`AI_TRANSLATION_CACHE_KEY_LIMIT`.

> Unlike the soft cache, no-expiry entries are never refreshed and never marked stale. A translation
> cached once persists until Redis loses it or someone invalidates the prefix explicitly. This is
> correct for translations — the English source rarely changes — but it does mean **fixing a bad
> translation requires clearing the cache**, not just correcting the source string:
>
> ```bash
> redis-cli --scan --pattern 'NO_EXPIRY_CACHE_*' | head
> ```

The key limit exists to stop unbounded growth from arbitrary input. Once reached, behaviour depends
on the implementation in `proco/accounts/` — verify before relying on any particular eviction
semantics.

## Frontend

The frontend calls this through `src/api/translation-request-fx.ts`, separate from the main
`backend-request-fx.ts`, so translation failures do not affect other requests.

## Troubleshooting

| Symptom | Check |
|---|---|
| Everything renders in English | `AI_TRANSLATION_ENDPOINT` / `KEY` / `REGION` set? |
| One language never works | Is it in `AI_TRANSLATION_SUPPORTED_TARGETS`? Default is `[]` |
| A correction does not take effect | Cached under `NO_EXPIRY_CACHE_*` — clear it |
| Translations stop after a while | `AI_TRANSLATION_CACHE_KEY_LIMIT` reached |
