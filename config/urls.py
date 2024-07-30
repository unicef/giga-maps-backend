import json

from django.conf import settings
from django.conf.urls.static import static
from django.http.response import HttpResponse
from django.urls import include, path
from django.views import View
from rest_framework.response import Response
from rest_framework.views import APIView


class TestView(View):
    def dispatch(self, request, *args, **kwargs):
        return HttpResponse(json.dumps(dict(request.headers)))


class PingAPIView(APIView):
    """
    PingAPIView
        API endpoint to check Proco system health
        Inherits: `APIView`
        Handles: GET request
    """

    def get(self, request, *args, **kwargs):
        return Response(data='OK')


def trigger_error(request):
    division_by_zero = 1 / 0


urlpatterns = [
    # path('admin/', admin.site.urls),
    path('api/', include([
        path('auth/', include('proco.custom_auth.api_urls')),
        path('locations/', include('proco.locations.api_urls')),
        path('locations/', include('proco.schools.api_urls')),
        path('background/', include('proco.background.api_urls')),
        path('statistics/', include('proco.connection_statistics.api_urls')),
        path('contact/', include('proco.contact.api_urls')),
        path('about_us/', include('proco.about_us.api_urls')),
        path('accounts/', include('proco.accounts.api_urls')),
        path('sources/', include('proco.data_sources.api_urls')),
    ])),
    path('test/', TestView.as_view()),
    path('health/', PingAPIView.as_view()),
    path('sentry-debug/', trigger_error),
]

if settings.DEBUG:
    import debug_toolbar

    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += [
        path('__debug__/', include(debug_toolbar.urls)),
    ]


if settings.ENABLED_BACKEND_PROMETHEUS_METRICS:
    urlpatterns += [
        path('', include('django_prometheus.urls')),
    ]
