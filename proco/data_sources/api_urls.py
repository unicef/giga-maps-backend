from django.urls import path

from proco.data_sources import api

app_name = 'sources'

urlpatterns = [
    path('load/static_live/', api.StaticAndLiveDataLoaderViewSet.as_view(), name='load-live-static-data-source'),
    path('load/school_master/', api.SchoolMasterLoaderViewSet.as_view(), name='load-school-master-data-source'),
    path('load/qos/', api.QoSLoaderViewSet.as_view(), name='load-qos-data-source'),
    path('load/daily_check_app/', api.DailyCheckAppLoaderViewSet.as_view(), name='load-daily-check-app-data-source'),

    path('school_master/', api.SchoolMasterDataViewSet.as_view({
        'get': 'list',
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='list-school-master-rows'),
    path('school_master/<int:pk>/', api.SchoolMasterDataViewSet.as_view({
        'get': 'retrieve',
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='retrieve-update-delete-school-master-data-row'),

    path('school_master/publish/', api.SchoolMasterDataPublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-school-master-data-rows'),
    path('school_master/<int:pk>/publish/', api.SchoolMasterDataPublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-school-master-data-row'),

    path('school_master/country-publish/', api.SchoolMasterDataPublishByCountryViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-school-master-data-rows-for-country'),
]
