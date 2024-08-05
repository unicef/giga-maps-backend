from django.urls import include, path
from proco.locations import api
from rest_framework import routers

router = routers.SimpleRouter()
router.register(r'countries', api.CountryViewSet, basename='countries')

app_name = 'locations'

urlpatterns = [
    path('', include(router.urls)),
    # path('countries-boundary/', api.CountryBoundaryListAPIView.as_view(), name='countries-boundary'),
    path('countries-download/', api.DownloadCountriesViewSet.as_view({
        'get': 'list',
    }), name='download-countries'),

    # DB table based listing only for Country, Admin1 and Admin2
    path('search-countries/', api.CountrySearchStatListAPIView.as_view(), name='search-countries-admin-schools'),
    # Cognitive Search Index based searching for Schools
    path('gsearch/', api.AggregateSearchViewSet.as_view(), name='global-search-filter'),

    path('country/', api.CountryDataViewSet.as_view({
        'get': 'list',
        'post': 'create',
        'delete': 'destroy',
    }), name='list-create-destroy-country'),
    path('country/<int:pk>/', api.CountryDataViewSet.as_view({
        'get': 'retrieve',
        'put': 'update',
        'delete': 'destroy',
    }), name='update-retrieve-country'),

    path('country-admin-metadata/', api.CountryAdminMetadataViewSet.as_view({
        'get': 'list',
    }), name='list-country-admin-metadata'),
    path('mark-as-joined/', api.MarkAsJoinedViewSet.as_view({
        'post': 'create',
    }), name='mark-as-joined'),
]
