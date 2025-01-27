from django.urls import path

from proco.connection_statistics import api

app_name = 'connection_statistics'

urlpatterns = [
    path('global-stat/', api.GlobalStatsAPIView.as_view(), name='global-stat'),
    path('time-players/', api.TimePlayerViewSet.as_view(), name='get-time-player-data'),
    path('connectivity/', api.ConnectivityAPIView.as_view(), name='global-connectivity-stat'),
    path('countryconnectivity/', api.ConnectivityAPIView.as_view(), name='country-connectivity-stat'),
    path('connectivityconfigs/', api.ConnectivityConfigurationsViewSet.as_view(), name='get-latest-week-and-month'),
    path('schoolconnectivity/', api.SchoolConnectivityStatsListAPIView.as_view(), name='school-connectivity-stat'),

    path('coverage/', api.CoverageAPIView.as_view(), name='global-coverage-stat'),
    path('schoolcoverage/', api.SchoolCoverageStatsListAPIView.as_view(), name='school-coverage-stat'),

    path(
        'country/<str:country_code>/daily-stat/',
        api.CountryDailyStatsListAPIView.as_view(),
        name='country-daily-stat',
    ),
    path('school/<int:school_id>/daily-stat/', api.SchoolDailyStatsListAPIView.as_view(), name='school-daily-stat'),

    path('countryweeklystatus/', api.CountrySummaryAPIViewSet.as_view({
        'get': 'list',
        'post': 'create',
        'delete': 'destroy',
    }), name='list-create-destroy-countryweeklystatus'),
    path('countryweeklystatus/<int:pk>/', api.CountrySummaryAPIViewSet.as_view({
        'put': 'update',
        'get': 'retrieve',
    }), name='update-retrieve-countryweeklystatus'),
    path('countrydailystatus/', api.CountryDailyConnectivitySummaryAPIViewSet.as_view({
        'get': 'list',
        'post': 'create',
        'delete': 'destroy',
    }), name='list-create-destroy-countrydailystatus'),
    path('countrydailystatus/<int:pk>/',
         api.CountryDailyConnectivitySummaryAPIViewSet.as_view({
             'put': 'update',
             'get': 'retrieve',
         }), name='update-retrieve-countrydailystatus'),
    path('schoolweeklystatus/', api.SchoolSummaryAPIViewSet.as_view({
        'get': 'list',
        'post': 'create',
        'delete': 'destroy',
    }), name='list-create-destroy-schoolweeklystatus'),
    path('schoolweeklystatus/<int:pk>/', api.SchoolSummaryAPIViewSet.as_view({
        'put': 'update',
        'get': 'retrieve',
    }), name='update-retrieve-schoolweeklystatus'),
    path('schooldailystatus/', api.SchoolDailyConnectivitySummaryAPIViewSet.as_view({
        'get': 'list',
        'post': 'create',
        'delete': 'destroy',
    }), name='list-create-destroy-schooldailystatus'),
    path('schooldailystatus/<int:pk>/',
         api.SchoolDailyConnectivitySummaryAPIViewSet.as_view({
             'put': 'update',
             'get': 'retrieve',
         }), name='update-retrieve-schooldailystatus'),
]
