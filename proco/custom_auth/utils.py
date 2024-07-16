from datetime import datetime, timedelta

import jwt
from django.conf import settings
from django.db.models import Q
from django.utils.translation import ugettext as _
from rest_framework import exceptions

from proco.core import utils as core_utilities
from proco.custom_auth import models as auth_models


def jwt_get_username_from_payload_handler(payload):
    """
    Override this function if username is formatted differently in payload
    """
    if 'emails' in payload:
        return payload.get('emails', [])[-1]
    return payload.get('email', '')


def create_role_permissions_data(user, slugs, role):
    role_permissions = [
        auth_models.RolePermission(**{
            'role': role,
            'slug': slug,
            'created_by': user,
            'last_modified_by': user,
        })
        for slug in slugs
    ]
    return role_permissions


def validate_azure_ad_b2c_token(decoded_token):
    """
    validate_azure_ad_b2c_token
        Validates an Azure AD B2C access token.

    Args:
        decoded_token: The access token to validate.
    Returns:
        True if the token is valid, False otherwise.
    """

    ad_b2c_settings = settings.AZURE_CONFIG.get('AD_B2C')

    # If not policy configured then pass the validation
    if ad_b2c_settings.get('SIGNUP_SIGNIN_POLICY'):
        # Validate the issuer claim.
        if 'iss' not in decoded_token or not decoded_token.get('iss').startswith(ad_b2c_settings.get('BASE_URL')):
            raise jwt.InvalidTokenError

        # Validate the audience claim.
        if decoded_token.get('aud', None) != ad_b2c_settings.get('CLIENT_ID'):
            raise jwt.InvalidTokenError

        # Validate the expiration claim by extending it to 12 more hours.
        exp_datetime = (core_utilities.get_timezone_converted_value(datetime.fromtimestamp(decoded_token['exp']))
                        + timedelta(hours=12))
        current_datetime = core_utilities.get_current_datetime_object()
        if exp_datetime < current_datetime:
            raise jwt.ExpiredSignature

        # Validate not before claim.
        nbf_datetime = core_utilities.get_timezone_converted_value(datetime.fromtimestamp(decoded_token['nbf']))
        if nbf_datetime > current_datetime:
            raise jwt.InvalidTokenError

        if 'email' not in decoded_token and 'emails' not in decoded_token:
            raise jwt.InvalidTokenError

        # Optionally, validate the signature of the token.
        # public_key = get_public_key_from_azure_ad_b2c_tenant()
        # if not jwt.verify(token, public_key):
        #     return False

    return True


def jwt_decode_handler(token):
    """
    jwt_decode_handler
    """
    try:
        token_header = jwt.get_unverified_header(token)
        print('Token header: {0}'.format(token_header))

        payload = jwt.decode(
            token,
            None,
            False,
            algorithms=[token_header.get('alg')],
            options={'verify_signature': False}
        )
        print('Token as decoded payload: {0}'.format(payload))
        validate_azure_ad_b2c_token(payload)
    except jwt.ExpiredSignature:
        msg = _('Signature has expired.')
        raise exceptions.AuthenticationFailed(msg)
    except jwt.DecodeError:
        msg = _('Error decoding signature.')
        raise exceptions.AuthenticationFailed(msg)
    except jwt.InvalidTokenError:
        raise exceptions.AuthenticationFailed()
    return payload


def get_user_emails_for_permissions(permissions, ids_to_filter=None):
    users_queryset = auth_models.ApplicationUser.objects.filter(
        roles__role__permissions__slug__in=permissions,
        roles__deleted__isnull=True,
        roles__role__deleted__isnull=True,
        roles__role__permissions__deleted__isnull=True,
        is_active=True,
    )
    # if superuser=true then staff=True is must
    users_queryset = users_queryset.filter(Q(is_superuser=False) | Q(is_superuser=True, is_staff=True))

    if ids_to_filter:
        users_queryset = users_queryset.filter(id__in=ids_to_filter)

    return list(users_queryset.values_list('email', flat=True).order_by('email').distinct('email'))
