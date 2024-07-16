from django.urls import path

from proco.contact import api

app_name = 'contact'

urlpatterns = [
    # INFO: Commented this to add authentication
    # path('contact/', api.ContactAPIView.as_view(), name='contact'),
    path('contactmessage/', api.ContactAPIView.as_view({
        'get': 'list',
        'delete': 'destroy',
    }), name='list-or-delete-contact'),
    path('contactmessage/<int:pk>/', api.ContactAPIView.as_view({
        'get': 'retrieve',
    }), name='retrieve-contact'),
    path('contact/', api.CreateContactAPIView.as_view(), name='create-contact'),
]
