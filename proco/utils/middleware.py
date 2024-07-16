from corsheaders.middleware import CorsMiddleware, ACCESS_CONTROL_ALLOW_ORIGIN
from django.conf import settings


class CustomCorsMiddleware(CorsMiddleware):
    def process_response(self, request, response):
        response = super(CustomCorsMiddleware, self).process_response(request, response)
        if ACCESS_CONTROL_ALLOW_ORIGIN not in response and settings.CORS_ALLOW_ORIGINS:
            response[ACCESS_CONTROL_ALLOW_ORIGIN] = settings.CORS_ALLOW_ORIGINS
        return response
