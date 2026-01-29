from django.urls import path

from proco.accounts.v2 import entity_api

print("LOADED v2 api_urls")
urlpatterns = [
    path(
        'layers/<int:pk>/map/',
        entity_api.EntityDataLayerMapViewSet.as_view(),
        name='entity-map-data-layer'
    ),
    path('layers/<int:pk>/info/', entity_api.EntityDataLayerInfoViewSet.as_view(), name='entity-info-data-layer'),
]
