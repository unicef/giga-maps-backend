from django.contrib.auth import get_user_model
from django.utils.translation import ugettext as _
from rest_framework import exceptions
from rest_framework_jwt import authentication as jwt_authentication
from rest_framework_jwt.settings import api_settings

from proco.custom_auth.models import Role
from proco.core.utils import is_blank_string

jwt_decode_handler = api_settings.JWT_DECODE_HANDLER


# jwt_get_username_from_payload = api_settings.JWT_PAYLOAD_GET_USERNAME_HANDLER


class JSONWebTokenAuthentication(jwt_authentication.JSONWebTokenAuthentication):
    """
    Token based authentication using the JSON Web Token standard.
    """

    def authenticate(self, request):
        user_model = get_user_model()
        # user = user_model.objects.filter(email='admin@test.com').first()
        # payload = jwt_serializers.jwt_payload_handler(user)
        # token = jwt_serializers.jwt_encode_handler(payload)
        # payload2 = jwt_decode_handler(token)
        # header_token_decoded = jwt_decode_handler(jwt_token)
        # username_from_token = jwt_get_username_from_payload(header_token_decoded)

        try:
            return super().authenticate(request)
        except exceptions.AuthenticationFailed as ex:
            error_message = ex.get_full_details().get('message', None)

            # When authentication fails, check if it is a GET request on '/api/accounts/user_details/' API
            if (
                request.method == 'GET' and
                '/user_details/' in request.path and
                error_message != 'Signature has expired.'
            ):
                # Create user only if force == true is added in query param, otherwise raise the exception
                create_user_if_not_exist = request.query_params.get('force', 'false').lower() == 'true'

                if create_user_if_not_exist:
                    jwt_token = str(request.headers.get('Authorization', '')).replace('Bearer', '').strip()
                    if not is_blank_string(jwt_token):
                        # This is added here to fix the circular dependency issue
                        from proco.custom_auth.serializers import CreateUserSerializer
                        user_payload = jwt_decode_handler(jwt_token)
                        # Create the user with readonly permissions. If role needs to be changed then PROCO Admin will
                        # change the role from admin page
                        user_payload['role'] = Role.objects.filter(name=Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id
                        create_user_serializer = CreateUserSerializer(data=user_payload)
                        create_user_serializer.is_valid(raise_exception=True)
                        create_user_serializer.save()

                        return super().authenticate(request)
            raise ex
        except user_model.DoesNotExist:
            msg = _('Invalid signatures.')
            e = exceptions.AuthenticationFailed(msg)
            print(str(e))
            raise e
