# GigaMaps Backend — Documentation

Documentation for the [`giga-maps-backend`](https://github.com/unicef/giga-maps-backend) repository
(Django + DRF + Celery), including the user- and admin-facing flows it serves through
[`giga-maps-frontend`](https://github.com/unicef/giga-maps-frontend).

> **Scope.** Everything here is derived from the code on the `staging` branch of both repos.
> Every non-obvious claim links to the file and line it came from. Anything that could not be
> confirmed from source alone is explicitly marked `> **Unverified**`.

---

## I want to…

| …do this | Go here |
|---|---|
| Understand what the system *is* and how the pieces fit | [00-overview/architecture.md](00-overview/architecture.md) |
| Get it running on my machine | [01-getting-started/local-setup.md](01-getting-started/local-setup.md) |
| Know what a "school" vs an "entity" is | [00-overview/glossary.md](00-overview/glossary.md), [02-domain-model/entities.md](02-domain-model/entities.md) |
| Trace what happens when a user opens the map | [03-user-flows/map-exploration.md](03-user-flows/map-exploration.md) |
| Understand the admin console | [04-admin-flows/README.md](04-admin-flows/README.md) |
| See every scheduled job and when it runs | [05-background-jobs/README.md](05-background-jobs/README.md) |
| Find out where external data comes from | [06-integrations/README.md](06-integrations/README.md) |
| Look up an API endpoint | [07-api-reference/endpoint-index.md](07-api-reference/endpoint-index.md) |
| Recover from a failed ingestion / data loss | [08-operations/runbooks.md](08-operations/runbooks.md) |
| Make the API faster / understand caching | [08-operations/caching-and-performance.md](08-operations/caching-and-performance.md) |
| Onboard as a new developer | [01-getting-started/onboarding-path.md](01-getting-started/onboarding-path.md) |

---

## Structure

```
documentation/
├── 00-overview/          What the system is, how it is put together, vocabulary
├── 01-getting-started/   Local setup, environment variables, guided onboarding
├── 02-domain-model/      Data model: countries, schools, entities, statistics, DB routing
├── 03-user-flows/        Public / authenticated end-user journeys, FE route → BE endpoint
├── 04-admin-flows/       Admin console journeys, RBAC, review & publish workflows
├── 05-background-jobs/   The 25 scheduled Celery jobs, grouped by job family
├── 06-integrations/      External systems the backend talks to
├── 07-api-reference/     API conventions and a complete endpoint index
├── 08-operations/        Deployment, caching, monitoring, runbooks, management commands
└── 09-contributing/      Code layout, testing, conventions
```

---

## Conventions used in these docs

- **File references** are written as `path/to/file.py:123` and link to the line in the repo.
- **Mermaid diagrams** render on GitHub natively. Sequence diagrams show request flow;
  state diagrams show lifecycle.
- **`> **Unverified**`** marks anything inferred rather than read directly from code — usually
  runtime/infrastructure behaviour that is not visible in the repository.
- **`> **Legacy**`** marks code paths that still exist but are superseded.
- Times are **UTC** throughout, matching `app.conf.timezone = 'UTC'` in
  [proco/taskapp/__init__.py:15](../proco/taskapp/__init__.py#L15).

## Findings raised while writing these docs

Documenting the code surfaced a number of things that look like defects rather than design. They are
described in place, with the code that produces them. Collected here so they are not lost:

| Severity | Finding | Where |
|---|---|---|
| **High** | Public vector-tile endpoints build SQL by string-formatting raw query-string values (`limit`, `country_id`, `school_id`, `admin1_id` and the `__in` variants) into the statement, with no authentication required | [03-user-flows/map-exploration.md](03-user-flows/map-exploration.md#unparameterised-sql-on-an-unauthenticated-endpoint) |
| **High** | Write-access API keys are hidden from every admin listing, accepted from any authenticated user, and issued with far-future expiry — they cannot be audited or revoked through the UI | [04-admin-flows/api-key-approval.md](04-admin-flows/api-key-approval.md#who-sees-what) |
| Medium | `/test/` echoes all request headers, including `Authorization`, to any caller, and is registered unconditionally rather than behind `DEBUG` | [07-api-reference/endpoint-index.md](07-api-reference/endpoint-index.md#root-level-routes) |
| Medium | Delta Share bearer tokens are written to a file in `BASE_DIR` during ingestion and removed in a `try/except OSError: pass` — a crashed worker leaves live credentials on disk | [06-integrations/delta-sharing.md](06-integrations/delta-sharing.md#the-profile-file) |
| Medium | Publishing a master-data row hard-deletes every other staging row sharing its `school_id_giga`, **unscoped by country** | [04-admin-flows/school-master-review-publish.md](04-admin-flows/school-master-review-publish.md#what-publishing-does) |
| Medium | CI runs tests only on `develop` and with `continueOnError: true`; the staging image build has its branch condition commented out | [08-operations/deployment.md](08-operations/deployment.md#steps) |
| Medium | `BackgroundTask` has no failure state — a crashed job stays `running` forever and blocks later runs | [04-admin-flows/background-task-console.md](04-admin-flows/background-task-console.md) |
| Medium | Per-country ingestion failures are logged at `warning` and the job still reports success, so a country can stop updating silently for weeks | [05-background-jobs/ingestion-master-data.md](05-background-jobs/ingestion-master-data.md#failures-are-per-country-and-non-fatal) |
| Medium | The `entities` app (3 629 lines) has **no tests**; `giga_meter` is excluded from coverage measurement | [09-contributing/testing.md](09-contributing/testing.md#current-test-layout) |
| Low | `user.permissions` is recomputed with two queries on every access, several times per authenticated request | [08-operations/caching-and-performance.md](08-operations/caching-and-performance.md#1-userpermissions-is-recomputed-on-every-access) |
| Low | Cache invalidation uses Redis `KEYS` (blocking) rather than `SCAN`, nightly and on demand | [08-operations/caching-and-performance.md](08-operations/caching-and-performance.md#invalidation) |
| Low | 45 of 120 environment variables are absent from `.env_example`, including the entire `HEALTH_MASTER_*` group | [01-getting-started/environment-variables.md](01-getting-started/environment-variables.md#variables-missing-from-envexample) |
| Low | Cognitive Search index names use underscores in code defaults and hyphens in `.env_example`; a mismatch yields empty search with no error | [06-integrations/azure-cognitive-search.md](06-integrations/azure-cognitive-search.md#configuration) |
| Low | `CONTACT_MANAGERS` defaults to `test@test.test` and Anymail suppresses bounces, so contact-form enquiries can vanish silently | [03-user-flows/contact.md](03-user-flows/contact.md#recipients) |
| Low | The requester-facing API-key sort has unreachable `Case`/`When` branches, so pending extensions do not stand out | [04-admin-flows/api-key-approval.md](04-admin-flows/api-key-approval.md#sort-order) |

## Known documentation debt

- The task table in the repository root [README.md](../README.md) is **out of date** — it lists
  13 jobs, including `clean_old_realtime_data` which no longer exists, and several schedules
  differ from the live ones. [05-background-jobs/README.md](05-background-jobs/README.md) is the
  authoritative list.
- The **Entity** subsystem (`/api/v2/entities/`) is fully live in the backend but the frontend
  `staging` branch does not call it yet. See
  [02-domain-model/entities.md](02-domain-model/entities.md) for what this means in practice.
