from django.urls import path

from proco.custom_auth import api

app_name = 'custom_auth'

urlpatterns = [
    path('users/', api.UserViewSet.as_view({
            'post': 'create',
            'get': 'list',
        }), name='create-and-list-users'),
    path('users/<int:pk>/', api.UserViewSet.as_view({
            'get': 'retrieve',
            'put': 'partial_update',
        }), name='user-details'),
    path('user_details/', api.UserDetailsViewSet.as_view({
            'get': 'retrieve',
            'post': 'create',
            'put': 'partial_update',
        }), name='get-and-create-self-user'),
    path('roles/', api.RoleViewSet.as_view({
            'post': 'create',
            'get': 'list',
        }), name='create-and-list-roles'),
    path('roles/<int:pk>/', api.RoleViewSet.as_view({
            'get': 'retrieve',
            'put': 'partial_update',
            'delete': 'destroy',
        }), name='get-update-and-delete-role'),
]
