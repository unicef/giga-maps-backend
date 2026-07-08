import logging

from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from proco.schools.nocodb_country_config import fetch_country_configs

logger = logging.getLogger('gigamaps.' + __name__)


class CountryConfigAPIView(APIView):
    permission_classes = (AllowAny,)

    def get(self, request, *args, **kwargs):
        try:
            data = fetch_country_configs()
        except Exception:
            logger.exception('Failed to fetch country config from NocoDB')
            response = Response(data=[])
            response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response['Pragma'] = 'no-cache'
            response['Expires'] = '0'
            return response

        response = Response(data=data)
        response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response['Pragma'] = 'no-cache'
        response['Expires'] = '0'
        return response
