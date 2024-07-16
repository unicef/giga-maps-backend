from django.urls import path

from proco.about_us import api

app_name = 'about_us'

urlpatterns = [
    path('slide_image/', api.SlideImageAPIView.as_view({
        'get': 'list',
        'delete': 'destroy',
        'post': 'create',
    }), name='list_or_delete_image'),
    path('slide_image/<int:pk>/', api.SlideImageAPIView.as_view({
        'get': 'retrieve',
        'put': 'update',
    }), name='retrieve_and_update_image'),

    path('about_us/', api.AboutUsAPIView.as_view({
        'get': 'retrieve',
        'delete': 'destroy',
        'post': 'create',
        'put': 'update',
    }), name='retrieve_delete_create_update_about_us'),
    path('about_us/active_data/', api.AboutUsAPIView.as_view({
        'get': 'list',
    }), name='list_about_us'),
]
