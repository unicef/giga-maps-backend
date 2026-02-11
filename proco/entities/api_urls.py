from django.urls import include, path
from rest_framework.routers import SimpleRouter

from proco.entities import api
from proco.entities.api import EntityTypeListAPIView

country_entities = SimpleRouter()
country_entities.register(r'countries/(?P<country_code>\w+)/entities', api.EntitiesViewSet, basename='entities')

app_name = 'entities'

urlpatterns = [
    path('', include(country_entities.urls)),
    path('v2/entities/tiles/connectivity_status/',
     api.EntityConnectivityStatusTileRequestHandler.as_view(),
     name='tiles-entity-connectivity-status-view'),
    path("entity-types/", EntityTypeListAPIView.as_view(), name="entity-types"),
]
