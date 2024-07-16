from django.urls import include, path
from proco.background import api

app_name = 'background'

urlpatterns = [
    path('backgroundtask/', api.BackgroundTaskViewSet.as_view({
        'get': 'list',
        'delete': 'destroy',
    }), name='list_or_destroy_backgroundtask'),
    path('backgroundtask/<slug:task_id>/', api.BackgroundTaskViewSet.as_view({
        'get': 'retrieve',
    }), name='update_or_retrieve_backgroundtask'),
    path('backgroundtask/<slug:task_id>/history', api.BackgroundTaskHistoryViewSet.as_view({
        'get': 'list',
    }), name='background_task_history'),

]
