from django.urls import path

from proco.accounts.v2 import entity_api

urlpatterns = [
    path(
        'layers/<int:pk>/map/',
        entity_api.EntityDataLayerMapViewSet.as_view(),
        name='entity-map-data-layer'
    ),
    path('layers/<int:pk>/info/', entity_api.EntityDataLayerInfoViewSet.as_view(), name='entity-info-data-layer'),
    path('layers/', entity_api.EntityDataLayersViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-or-create-data-layers-entities'),
    path('layers/<int:pk>/', entity_api.EntityDataLayersViewSet.as_view({
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='update-or-delete-data-layer-entities'),
    path('layers/<int:pk>/publish/', entity_api.EntityDataLayerPublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-data-layer-entities'),
    # path('layers/<int:pk>/preview/', entity_api.EntityDataLayerPreviewViewSet.as_view(),
    # name='preview-data-layer-entities'),

    path('layers/<int:pk>/metadata/', entity_api.EntityDataLayerMetadataViewSet.as_view({
        'get': 'retrieve',
    }), name='metadata-data-layer'),
    path('layers/<str:status>/', entity_api.PublishedEntityDataLayersViewSet.as_view({
        'get': 'list',
    }), name='list-published-data-layers-entities'),
]
