from django.urls import path

from proco.background import api

app_name = 'background'

urlpatterns = [
    path('backgroundtask/', api.BackgroundTaskViewSet.as_view({
        'get': 'list',
        'delete': 'destroy',
    }), name='list-destroy-backgroundtask'),
    path('backgroundtask/<slug:task_id>/', api.BackgroundTaskViewSet.as_view({
        'get': 'retrieve',
    }), name='update-retrieve-backgroundtask'),
    path('backgroundtask/<slug:task_id>/history', api.BackgroundTaskHistoryViewSet.as_view({
        'get': 'list',
    }), name='background-task-history'),

]
