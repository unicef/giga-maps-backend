from django.urls import path

from proco.accounts import api

app_name = 'accounts'

urlpatterns = [
    path('apis/', api.APIsListAPIView.as_view({
        'get': 'list',
    }), name='list-apis'),
    path('api_keys/', api.APIKeysViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-or-create-api-keys'),
    path('api_keys/<int:pk>/', api.APIKeysViewSet.as_view({
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='update-and-delete-api-key'),
    path('api_keys/<int:pk>/request_extension/', api.APIKeysRequestExtensionViewSet.as_view({
        'put': 'partial_update',
    }), name='request-api-key-extension'),
    path('validate_api_key/', api.ValidateAPIKeyViewSet.as_view(), name='validate-an-api-key'),

    path('translate/text/<str:target>/', api.TranslateTextFromEnViewSet.as_view(), name='translate-a-text-to-given-target-language'),

    # Email Endpoints
    path('notifications/', api.NotificationViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-send-notifications'),

    path('invalidate-cache/', api.InvalidateCache.as_view(), name='admin-invalidate-cache'),

    path('app_configs/', api.AppStaticConfigurationsViewSet.as_view(), name='get-app-static-configurations'),
    path('data_sources/', api.DataSourceViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-or-create-data-sources'),
    path('data_sources/<int:pk>/', api.DataSourceViewSet.as_view({
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='update-or-delete-data-source'),
    path('data_sources/<int:pk>/publish/', api.DataSourcePublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-data-source'),

    path('layers/', api.DataLayersViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-or-create-data-layers'),
    path('layers/<int:pk>/', api.DataLayersViewSet.as_view({
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='update-or-delete-data-layer'),
    path('layers/<int:pk>/publish/', api.DataLayerPublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-data-layer'),
    path('layers/<int:pk>/preview/', api.DataLayerPreviewViewSet.as_view(), name='preview-data-layer'),

    path('layers/<int:pk>/metadata/', api.DataLayerMetadataViewSet.as_view({
        'get': 'retrieve',
    }), name='metadata-data-layer'),
    path('layers/<int:pk>/info/', api.DataLayerInfoViewSet.as_view(), name='info-data-layer'),
    path('layers/<int:pk>/map/', api.DataLayerMapViewSet.as_view(), name='map-data-layer'),

    path('layers/<str:status>/', api.PublishedDataLayersViewSet.as_view({
        'get': 'list',
    }), name='list-published-data-layers'),

    path('recent_action_log/', api.LogActionViewSet.as_view({'get': 'list', }), name='list-recent-action-log'),

    path('time-players/v2/', api.TimePlayerViewSet.as_view(), name='get-time-player-data-v2'),

    path('column_configurations/', api.ColumnConfigurationViewSet.as_view({
        'get': 'list',
    }), name='list-column-configurations'),

    path('adv_filters/', api.AdvanceFiltersViewSet.as_view({
        'get': 'list',
        'post': 'create',
    }), name='list-or-create-advance-filters'),
    path('adv_filters/<int:pk>/', api.AdvanceFiltersViewSet.as_view({
        'put': 'partial_update',
        'delete': 'destroy',
    }), name='update-or-delete-advance-filter'),
    path('adv_filters/<int:pk>/publish/', api.AdvanceFiltersPublishViewSet.as_view({
        'put': 'partial_update',
    }), name='publish-advance-filter'),
    path('adv_filters/<str:status>/<int:country_id>/', api.PublishedAdvanceFiltersViewSet.as_view({
        'get': 'list',
    }), name='list-published-advance-filters'),
]
