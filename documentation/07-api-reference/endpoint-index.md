# Endpoint index

Every route registered under `/api/`, generated from the `api_urls.py` files and verified against
[config/urls.py](../../config/urls.py). **120 path registrations across 10 route groups.**

Conventions (authentication, pagination, `expand`, errors) are in
[conventions.md](conventions.md).

> 🔵 marks a URL name present in `READ_ONLY_DATABASE_ALLOWED_REQUESTS` — GETs to it are routed to
> the read replica. See
> [../08-operations/caching-and-performance.md](../08-operations/caching-and-performance.md).

> **Two apps share the `/api/locations/` prefix.** `locations.api_urls` is included before
> `schools.api_urls` in [config/urls.py:35](../../config/urls.py#L35), so on a collision the
> `locations` pattern wins. They are listed separately below.

## Root-level routes

| Path | Purpose |
|---|---|
| `/health/` | Liveness probe — returns `OK` |
| `/test/` | Echoes request headers as JSON |
| `/sentry-debug/` | Deliberately raises `ZeroDivisionError` to verify Sentry wiring |
| `/__debug__/` | django-debug-toolbar, `DEBUG` only |
| `/metrics` etc. | django-prometheus, when `ENABLED_BACKEND_PROMETHEUS_METRICS` |

> `/test/` returns **all request headers**, including `Authorization`, to any caller. It is
> registered unconditionally — not behind `DEBUG`. Worth confirming it is blocked at the ingress in
> production, or removing it.

## Route groups

### `/api/auth/` — Authentication, users and roles

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/auth/users/` | GET,POST | `UserViewSet` | `create-and-list-users` |
| `/api/auth/users/<int:pk>/` | GET,PUT | `UserViewSet` | `user-details` |
| `/api/auth/user_details/` | GET,POST,PUT | `UserDetailsViewSet` | `get-and-create-self-user` |
| `/api/auth/roles/` | GET,POST | `RoleViewSet` | `create-and-list-roles` |
| `/api/auth/roles/<int:pk>/` | GET,PUT,DELETE | `RoleViewSet` | `get-update-and-delete-role` |

### `/api/locations/` — Countries, boundaries, search

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/locations/countries-boundary/` | GET | `CountryBoundaryListAPIView` | `countries-boundary` |
| `/api/locations/countries-download/` | GET | `DownloadCountriesViewSet` | `download-countries` 🔵 |
| `/api/locations/search-countries/` | GET | `CountrySearchStatListAPIView` | `search-countries-admin-schools` 🔵 |
| `/api/locations/gsearch/` | GET | `AggregateSearchViewSet` | `global-search-filter` |
| `/api/locations/country/` | GET,POST,DELETE | `CountryDataViewSet` | `list-create-destroy-country` |
| `/api/locations/country/<int:pk>/` | GET,PUT,DELETE | `CountryDataViewSet` | `update-retrieve-country` |
| `/api/locations/country-admin-metadata/` | GET | `CountryAdminMetadataViewSet` | `list-country-admin-metadata` |
| `/api/locations/mark-as-joined/` | POST | `MarkAsJoinedViewSet` | `mark-as-joined` |

### `/api/locations/` — Schools, tiles, downloads, CSV import

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/locations/country-config/` | GET | `—` | `country-config` |
| `/api/locations/schools/random/` | GET | `RandomSchoolsListAPIView` | `random-schools` |
| `/api/locations/schools/tiles/` | GET | `SchoolTileRequestHandler` | `tiles-view` 🔵 |
| `/api/locations/schools/tiles/connectivity/` | GET | `ConnectivityTileRequestHandler` | `tiles-connectivity-view` |
| `/api/locations/schools/tiles/connectivity_status/` | GET | `SchoolConnectivityStatusTileRequestHandler` | `tiles-school-connectivity-status-view` |
| `/api/locations/schools-download/` | GET | `DownloadSchoolsViewSet` | `download-schools` 🔵 |
| `/api/locations/schools/school/` | GET,POST,DELETE | `AdminViewSchoolAPIViewSet` | `list-create-destroy-school` |
| `/api/locations/schools/school/<int:pk>/` | GET,PUT | `AdminViewSchoolAPIViewSet` | `update-or-retrieve-school` |
| `/api/locations/schools/fileimport/` | GET,POST | `ImportCSVViewSet` | `file-import` |

### `/api/statistics/` — Statistics and summaries

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/statistics/global-stat/` | GET | `GlobalStatsAPIView` | `global-stat` 🔵 |
| `/api/statistics/time-players/` | GET | `TimePlayerViewSet` | `get-time-player-data` 🔵 |
| `/api/statistics/connectivity/` | GET | `ConnectivityAPIView` | `global-connectivity-stat` |
| `/api/statistics/countryconnectivity/` | GET | `ConnectivityAPIView` | `country-connectivity-stat` |
| `/api/statistics/connectivityconfigs/` | GET | `ConnectivityConfigurationsViewSet` | `get-latest-week-and-month` 🔵 |
| `/api/statistics/schoolconnectivity/` | GET | `SchoolConnectivityStatsListAPIView` | `school-connectivity-stat` |
| `/api/statistics/coverage/` | GET | `CoverageAPIView` | `global-coverage-stat` |
| `/api/statistics/schoolcoverage/` | GET | `SchoolCoverageStatsListAPIView` | `school-coverage-stat` |
| `/api/statistics/country/<str:country_code>/daily-stat/` | GET | `CountryDailyStatsListAPIView` | `country-daily-stat` |
| `/api/statistics/school/<int:school_id>/daily-stat/` | GET | `SchoolDailyStatsListAPIView` | `school-daily-stat` |
| `/api/statistics/countryweeklystatus/` | GET,POST,DELETE | `CountrySummaryAPIViewSet` | `list-create-destroy-countryweeklystatus` |
| `/api/statistics/countryweeklystatus/<int:pk>/` | GET,PUT | `CountrySummaryAPIViewSet` | `update-retrieve-countryweeklystatus` |
| `/api/statistics/countrydailystatus/` | GET,POST,DELETE | `CountryDailyConnectivitySummaryAPIViewSet` | `list-create-destroy-countrydailystatus` |
| `/api/statistics/countrydailystatus/<int:pk>/` | GET,PUT | `CountryDailyConnectivitySummaryAPIViewSet` | `update-retrieve-countrydailystatus` |
| `/api/statistics/schoolweeklystatus/` | GET,POST,DELETE | `SchoolSummaryAPIViewSet` | `list-create-destroy-schoolweeklystatus` |
| `/api/statistics/schoolweeklystatus/<int:pk>/` | GET,PUT | `SchoolSummaryAPIViewSet` | `update-retrieve-schoolweeklystatus` |
| `/api/statistics/schooldailystatus/` | GET,POST,DELETE | `SchoolDailyConnectivitySummaryAPIViewSet` | `list-create-destroy-schooldailystatus` |
| `/api/statistics/schooldailystatus/<int:pk>/` | GET,PUT | `SchoolDailyConnectivitySummaryAPIViewSet` | `update-retrieve-schooldailystatus` |

### `/api/accounts/` — API keys, data layers, filters, cache, notifications

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/accounts/apis/` | GET | `APIsListAPIView` | `list-apis` |
| `/api/accounts/api_categories/` | GET,POST | `APICategoriesViewSet` | `list-or-create-api-categories` |
| `/api/accounts/api_categories/<int:pk>/` | PUT,DELETE | `APICategoriesViewSet` | `update-and-delete-api-category` |
| `/api/accounts/api_keys/` | GET,POST | `APIKeysViewSet` | `list-or-create-api-keys` |
| `/api/accounts/api_keys/<int:pk>/` | PUT,DELETE | `APIKeysViewSet` | `update-and-delete-api-key` |
| `/api/accounts/api_keys/<int:pk>/request_extension/` | PUT | `APIKeysRequestExtensionViewSet` | `request-api-key-extension` |
| `/api/accounts/api_keys/<int:pk>/categories/` | PUT | `APIKeysAPICategoriesViewSet` | `api-key-api-categories-crud` |
| `/api/accounts/validate_api_key/` | GET | `ValidateAPIKeyViewSet` | `validate-an-api-key` |
| `/api/accounts/translate/text/<str:target>/` | GET | `TranslateTextFromEnViewSet` | `translate-a-text-to-given-target-language` |
| `/api/accounts/notifications/` | GET,POST | `NotificationViewSet` | `list-send-notifications` |
| `/api/accounts/invalidate-cache/` | GET | `InvalidateCache` | `admin-invalidate-cache` |
| `/api/accounts/invalidate-cache-patterns/` | GET | `InvalidateCacheByPattern` | `admin-invalidate-cache-based-on-patterns` |
| `/api/accounts/app_configs/` | GET | `AppStaticConfigurationsViewSet` | `get-app-static-configurations` |
| `/api/accounts/data_sources/` | GET,POST | `DataSourceViewSet` | `list-or-create-data-sources` |
| `/api/accounts/data_sources/<int:pk>/` | PUT,DELETE | `DataSourceViewSet` | `update-or-delete-data-source` |
| `/api/accounts/data_sources/<int:pk>/publish/` | PUT | `DataSourcePublishViewSet` | `publish-data-source` |
| `/api/accounts/layers/` | GET,POST | `DataLayersViewSet` | `list-or-create-data-layers` |
| `/api/accounts/layers/<int:pk>/` | PUT,DELETE | `DataLayersViewSet` | `update-or-delete-data-layer` |
| `/api/accounts/layers/<int:pk>/publish/` | PUT | `DataLayerPublishViewSet` | `publish-data-layer` |
| `/api/accounts/layers/<int:pk>/preview/` | GET | `DataLayerPreviewViewSet` | `preview-data-layer` |
| `/api/accounts/layers/<int:pk>/metadata/` | GET | `DataLayerMetadataViewSet` | `metadata-data-layer` |
| `/api/accounts/layers/<int:pk>/info/` | GET | `DataLayerInfoViewSet` | `info-data-layer` 🔵 |
| `/api/accounts/layers/<int:pk>/map/` | GET | `DataLayerMapViewSet` | `map-data-layer` |
| `/api/accounts/layers/<str:status>/` | GET | `PublishedDataLayersViewSet` | `list-published-data-layers` 🔵 |
| `/api/accounts/recent_action_log/` | GET | `LogActionViewSet` | `list-recent-action-log` |
| `/api/accounts/time-players/v2/` | GET | `TimePlayerViewSet` | `get-time-player-data-v2` |
| `/api/accounts/column_configurations/` | GET | `ColumnConfigurationViewSet` | `list-column-configurations` |
| `/api/accounts/adv_filters/` | GET,POST | `AdvanceFiltersViewSet` | `list-or-create-advance-filters` |
| `/api/accounts/adv_filters/<int:pk>/` | PUT,DELETE | `AdvanceFiltersViewSet` | `update-or-delete-advance-filter` |
| `/api/accounts/adv_filters/<int:pk>/publish/` | PUT | `AdvanceFiltersPublishViewSet` | `publish-advance-filter` |
| `/api/accounts/column_configurations/<int:pk>/choices/` | GET | `ColumnConfigurationChoicesViewSet` | `retrieve-advance-filter-live-choices` |
| `/api/accounts/adv_filters/<str:status>/<int:country_id>/` | GET | `PublishedAdvanceFiltersViewSet` | `list-published-advance-filters` 🔵 |
| `/api/accounts/adv_filters/<int:country_id>/all/` | GET | `AllPublishedAdvanceFiltersViewSet` | `list-all-published-advance-filters` |

### `/api/sources/` — Master-data review and manual loads

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/sources/load/static_live/` | GET | `StaticAndLiveDataLoaderViewSet` | `load-live-static-data-source` |
| `/api/sources/load/school_master/` | GET | `SchoolMasterLoaderViewSet` | `load-school-master-data-source` |
| `/api/sources/load/qos/` | GET | `QoSLoaderViewSet` | `load-qos-data-source` |
| `/api/sources/load/daily_check_app/` | GET | `DailyCheckAppLoaderViewSet` | `load-daily-check-app-data-source` |
| `/api/sources/school_master/` | GET,PUT,DELETE | `SchoolMasterDataViewSet` | `list-school-master-rows` |
| `/api/sources/school_master/<int:pk>/` | GET,PUT,DELETE | `SchoolMasterDataViewSet` | `retrieve-update-delete-school-master-data-row` |
| `/api/sources/school_master/publish/` | PUT | `SchoolMasterDataPublishViewSet` | `publish-school-master-data-rows` |
| `/api/sources/school_master/<int:pk>/publish/` | PUT | `SchoolMasterDataPublishViewSet` | `publish-school-master-data-row` |
| `/api/sources/school_master/country-publish/` | PUT | `SchoolMasterDataPublishByCountryViewSet` | `publish-school-master-data-rows-for-country` |

### `/api/background/` — Background task registry

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/background/backgroundtask/` | GET,DELETE | `BackgroundTaskViewSet` | `list-destroy-backgroundtask` |
| `/api/background/backgroundtask/<slug:task_id>/` | GET | `BackgroundTaskViewSet` | `update-retrieve-backgroundtask` |
| `/api/background/backgroundtask/<slug:task_id>/history` | GET | `BackgroundTaskHistoryViewSet` | `background-task-history` |

### `/api/contact/` — Contact form

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/contact/contact/` | GET | `ContactAPIView` | `contact` |
| `/api/contact/contactmessage/` | GET,DELETE | `ContactAPIView` | `list-or-delete-contact` |
| `/api/contact/contactmessage/<int:pk>/` | GET | `ContactAPIView` | `retrieve-contact` |
| `/api/contact/contact/` | GET | `CreateContactAPIView` | `create-contact` |

### `/api/about_us/` — CMS content

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/about_us/slide_image/` | GET,POST,DELETE | `SlideImageAPIView` | `list_or_delete_image` |
| `/api/about_us/slide_image/<int:pk>/` | GET,PUT | `SlideImageAPIView` | `retrieve_and_update_image` |
| `/api/about_us/about_us/` | GET,POST,PUT,DELETE | `AboutUsAPIView` | `retrieve_delete_create_update_about_us` |
| `/api/about_us/about_us/active_data/` | GET | `AboutUsAPIView` | `list_about_us` |

### `/api/v2/entities/` — Entity subsystem (v2)

| Path | Methods | View | URL name |
|---|---|---|---|
| `/api/v2/entities/entity-types/` | GET | `EntityTypeListAPIView` | `entity-types` |
| `/api/v2/entities/gentity-search/` | GET | `AggregateSearchEntityViewSet` | `global-search-filter` |
| `/api/v2/entities/global-stat/` | GET | `EntityGlobalStatsAPIView` | `global-stat-all-entities` |
| `/api/v2/entities/connectivityconfigs/` | GET | `EntityConnectivityConfigurationsViewSet` | `entity-get-latest-week-and-month` |
| `/api/v2/entities/countries/` | GET | `EntityCountryViewSet` | `list-entity-countries` |
| `/api/v2/entities/countries/<str:pk>/` | GET | `EntityCountryViewSet` | `retrieve-entity-country` |
| `/api/v2/entities/connectivity-stat/` | GET | `EntityConnectivityAPIView` | `global-connectivity-stat-entities` |
| `/api/v2/entities/tiles/connectivity/` | GET | `EntityGlobalConnectivityTileRequestHandler` | `tiles-global-connectivity-view` |
| `/api/v2/entities/tiles/connectivity_status/` | GET | `EntityConnectivityStatusTileRequestHandler` | `tiles-entity-connectivity-status-view` |
| `/api/v2/entities/filters/<str:status>/<int:country_id>/` | GET | `PublishedEntityAdvanceFiltersViewSet` | `list-published-entity-filters` |
| `/api/v2/entities/layers/map/` | GET | `EntityDataLayerMapViewSet` | `entity-map-data-layer` |
| `/api/v2/entities/layers/info/` | GET | `EntityDataLayerInfoViewSet` | `entity-info-data-layer` |
| `/api/v2/entities/layers/` | GET,POST | `EntityDataLayersViewSet` | `list-or-create-data-layers-entities` |
| `/api/v2/entities/layers/<int:pk>/publish/` | PUT | `EntityDataLayerPublishViewSet` | `publish-data-layer-entities` |
| `/api/v2/entities/layers/<int:pk>/preview/` | GET | `EntityDataLayerPreviewViewSet` | `preview-data-layer-entities` |
| `/api/v2/entities/filters/` | GET,POST | `EntityAdvanceFiltersViewSet` | `list-or-create-entity-filters` |
| `/api/v2/entities/filters/<int:pk>/` | PUT,DELETE | `EntityAdvanceFiltersViewSet` | `update-or-delete-entity-filter` |
| `/api/v2/entities/filters/<int:pk>/publish/` | PUT | `EntityAdvanceFiltersPublishViewSet` | `publish-entity-filter` |
| `/api/v2/entities/column_configurations/` | GET | `EntityColumnConfigurationViewSet` | `list-entity-column-configurations` |
| `/api/v2/entities/column_configurations/<int:pk>/choices/` | GET | `EntityColumnConfigurationChoicesViewSet` | `retrieve-entity-column-configuration-choices` |
| `/api/v2/entities/layers/<int:pk>/metadata/` | GET | `EntityDataLayerMetadataViewSet` | `metadata-data-layer-entities` |
| `/api/v2/entities/layers/<int:pk>/` | PUT,DELETE | `EntityDataLayersViewSet` | `update-or-delete-data-layer-entities` |
| `/api/v2/entities/invalidate-cache-patterns/` | GET | `EntityInvalidateCacheByPattern` | `entity-v2-invalidate-cache-based-on-patterns` |
| `/api/v2/entities/layers/<str:status>/` | GET | `PublishedEntityDataLayersViewSet` | `list-published-data-layers-entities` |
---

## Notes on particular endpoints

### Method annotations

A blank Methods column means the view is a plain `APIView` or `ListAPIView` registered with
`.as_view()` rather than a router-style `{'get': ...}` mapping; those are `GET` unless the view
defines otherwise (`ValidateAPIKeyViewSet` and the publish views are `PUT`).

### Manual data loads

`/api/sources/load/*` trigger ingestion on demand. `load/school_master/` runs
`handle_published_school_master_data_row` and `handle_deleted_school_master_data_row`
**synchronously** with `publish_source='cli'`
([proco/data_sources/api.py:51](../../proco/data_sources/api.py#L51)) — expect a 504 from gunicorn's
300-second timeout on a large dataset, while the work continues.

### Cache invalidation

| Endpoint | Scope |
|---|---|
| `/api/accounts/invalidate-cache/` | Everything |
| `/api/accounts/invalidate-cache-patterns/` | Key patterns |
| `/api/v2/entities/invalidate-cache-patterns/` | Entity keys |

Guarded by `CanCleanCache`. Both issue a Redis `KEYS` scan — see
[../04-admin-flows/cache-invalidation.md](../04-admin-flows/cache-invalidation.md).

### Duplicated URL names

`global-search-filter` is used by **both** `locations.gsearch` and `entities.gentity-search`, and
`global-stat` by both `connection_statistics` and (as `global-stat-all-entities`) the entity app.
Because URL names are namespaced by app (`locations:global-search-filter`,
`entities:global-search-filter`), `reverse()` resolves correctly — but a bare name in a log line or
a `READ_ONLY_DATABASE_ALLOWED_REQUESTS` entry is ambiguous. The middleware compares
`resolve(...).url_name`, which is the **un-namespaced** name, so whitelisting `global-search-filter`
would affect both apps.

### Entity v2 parallels

`/api/v2/entities/` mirrors much of `/api/accounts/` and `/api/statistics/` for entities: its own
layers, filters, column configurations, tiles, global stats and search. See
[../02-domain-model/entities.md](../02-domain-model/entities.md).

### Statistics CRUD endpoints

`countryweeklystatus`, `countrydailystatus`, `schoolweeklystatus` and `schooldailystatus` expose
full `GET/POST/PUT/DELETE` over the aggregation tables. These are admin tools —
the values they edit are recomputed by the nightly aggregation jobs, so manual edits are
transient unless the underlying raw data is fixed too.
