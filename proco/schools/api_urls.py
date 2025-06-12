from django.urls import include, path
from rest_framework.routers import SimpleRouter

from proco.schools import api

country_schools = SimpleRouter()
country_schools.register(r'countries/(?P<country_code>\w+)/schools', api.SchoolsViewSet, basename='schools')

app_name = 'schools'

urlpatterns = [
    path('', include(country_schools.urls)),
    path('schools/random/', api.RandomSchoolsListAPIView.as_view(), name='random-schools'),
    path('schools/tiles/', api.SchoolTileRequestHandler.as_view(), name='tiles-view'),
    path('schools/tiles/connectivity/', api.ConnectivityTileRequestHandler.as_view(), name='tiles-connectivity-view'),
    path('schools/tiles/connectivity_status/', api.SchoolConnectivityStatusTileRequestHandler.as_view(),
         name='tiles-school-connectivity-status-view'),
    path('schools-download/', api.DownloadSchoolsViewSet.as_view({
        'get': 'list',
    }), name='download-schools'),
    path('schools/school/', api.AdminViewSchoolAPIViewSet.as_view({
        'get': 'list',
        'post': 'create',
        'delete': 'destroy',
    }), name='list-create-destroy-school'),
    path('schools/school/<int:pk>/', api.AdminViewSchoolAPIViewSet.as_view({
        'put': 'update',
        'get': 'retrieve',
    }), name='update-or-retrieve-school'),
    path('schools/fileimport/', api.ImportCSVViewSet.as_view({
        'post': 'fileimport',
        'get': 'list',
    }), name='file-import'),
]
