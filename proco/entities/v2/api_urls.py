from django.urls import include, path
from rest_framework.routers import SimpleRouter

from proco.entities import api as entities_api
from proco.accounts.v2 import entity_api
from proco.accounts.v2 import entity_filter_api
from proco.connection_statistics.v2 import entity_api as stats_entity_api
from proco.locations.v2 import entity_api as locations_entity_api

country_entities = SimpleRouter()
country_entities.register(r'(?P<country_code>\w+)', entities_api.EntitiesViewSet, basename='entities')

app_name = 'entities'

urlpatterns = [
    # Entity types
    path('entity-types/', entities_api.EntityTypeListAPIView.as_view(), name='entity-types'),

    # Entity tiles
    path('tiles/connectivity_status/',
         entities_api.EntityConnectivityStatusTileRequestHandler.as_view(),
         name='tiles-entity-connectivity-status-view'),

    # Entity data layers - CRUD
    path('layers/', entity_api.EntityDataLayersViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-or-create-data-layers-entities'),
    path('layers/<int:pk>/', entity_api.EntityDataLayersViewSet.as_view({
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='update-or-delete-data-layer-entities'),

    # Entity data layers - publish, preview, metadata, info, map
    path('layers/<int:pk>/publish/', entity_api.EntityDataLayerPublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-data-layer-entities'),
    path('layers/<int:pk>/preview/', entity_api.EntityDataLayerPreviewViewSet.as_view(),
         name='preview-data-layer-entities'),
    path('layers/<int:pk>/metadata/', entity_api.EntityDataLayerMetadataViewSet.as_view({
        'get': 'retrieve',
    }), name='metadata-data-layer-entities'),
    path('layers/<int:pk>/info/', entity_api.EntityDataLayerInfoViewSet.as_view(),
         name='entity-info-data-layer'),
    path('layers/<int:pk>/map/', entity_api.EntityDataLayerMapViewSet.as_view(),
         name='entity-map-data-layer'),

    # Published entity data layers
    path('layers/<str:status>/', entity_api.PublishedEntityDataLayersViewSet.as_view({
        'get': 'list',
    }), name='list-published-data-layers-entities'),

    # Entity column configurations
    path('column_configurations/', entity_filter_api.EntityColumnConfigurationViewSet.as_view({
        'get': 'list',
    }), name='list-entity-column-configurations'),
    path('column_configurations/<int:pk>/choices/', entity_filter_api.EntityColumnConfigurationChoicesViewSet.as_view({
        'get': 'retrieve',
    }), name='retrieve-entity-column-configuration-choices'),

    # Entity advance filters - CRUD
    path('filters/', entity_filter_api.EntityAdvanceFiltersViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-or-create-entity-filters'),
    path('filters/<int:pk>/', entity_filter_api.EntityAdvanceFiltersViewSet.as_view({
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='update-or-delete-entity-filter'),
    path('filters/<int:pk>/publish/', entity_filter_api.EntityAdvanceFiltersPublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-entity-filter'),

    # Published entity filters
    path('filters/<str:status>/<int:country_id>/', entity_filter_api.PublishedEntityAdvanceFiltersViewSet.as_view({
        'get': 'list',
    }), name='list-published-entity-filters'),

    path('countries/', locations_entity_api.EntityCountryViewSet.as_view({'get': 'list'})),
    path('countries/<str:pk>/', locations_entity_api.EntityCountryViewSet.as_view({'get': 'retrieve'})),

    # Entity Global Stat
    path('global-stat/', stats_entity_api.EntityGlobalStatsAPIView.as_view(), name='global-stat-all-entities'),
    path('connectivity-stat/', stats_entity_api.EntityConnectivityAPIView.as_view(),
         name='global-connectivity-stat-entities'),

    path('tiles/connectivity/', entities_api.EntityGlobalConnectivityTileRequestHandler.as_view(),
         name='tiles-global-connectivity-view'),

    # Cognitive Search Index based searching for Entities
    path('gentity-search/', entities_api.AggregateSearchEntityViewSet.as_view(), name='global-search-filter'),

    # Entity list by country (Should be at last)
    path('', include(country_entities.urls)),
]
