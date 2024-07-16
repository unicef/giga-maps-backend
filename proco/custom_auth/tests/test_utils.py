from django.test import TestCase

from proco.custom_auth import models as auth_models
from proco.custom_auth import utils as auth_utilities
from proco.utils.tests import TestAPIViewSetMixin


def setup_admin_role():
    role = auth_models.Role.objects.filter(
        name=auth_models.Role.SYSTEM_ROLE_NAME_ADMIN,
        category=auth_models.Role.ROLE_CATEGORY_SYSTEM,
    ).first()
    if not role:
        role = auth_models.Role.objects.create(name=auth_models.Role.SYSTEM_ROLE_NAME_ADMIN,
                                               category=auth_models.Role.ROLE_CATEGORY_SYSTEM)

        perms = [perm[0] for perm in auth_models.RolePermission.PERMISSION_CHOICES]

        for perm in perms:
            auth_models.RolePermission.objects.get_or_create(
                role=role,
                slug=perm,
                defaults={
                    'role': role,
                    'slug': perm,
                }
            )

    return role


def setup_read_only_role():
    role = auth_models.Role.objects.filter(
        name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY,
        category=auth_models.Role.ROLE_CATEGORY_SYSTEM,
    ).first()
    if not role:
        role = auth_models.Role.objects.create(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY,
                                               category=auth_models.Role.ROLE_CATEGORY_SYSTEM)

        perms = [auth_models.RolePermission.CAN_DELETE_API_KEY, ]

        for perm in perms:
            auth_models.RolePermission.objects.get_or_create(
                role=role,
                slug=perm,
                defaults={
                    'role': role,
                    'slug': perm,
                }
            )

    return role


def setup_admin_user_by_role():
    email = 'test.admin@test.com'
    password = 'SomeRandomPass96'
    user = auth_models.ApplicationUser.objects.filter(username=email).first()
    if not user:
        user = auth_models.ApplicationUser.objects.create_user(
            username=email,
            email=email,
            password=password,
            first_name='Admin',
            last_name='User',
        )

        admin_role = setup_admin_role()
        auth_models.UserRoleRelationship.objects.create(user=user, role=admin_role)

    return user


def setup_read_only_user_by_role():
    email = 'test.read_only@test.com'
    password = 'SomeRandomPass96'
    user = auth_models.ApplicationUser.objects.filter(username=email).first()
    if not user:
        user = auth_models.ApplicationUser.objects.create_user(
            username=email,
            email=email,
            password=password,
            first_name='Read Only',
            last_name='User',
        )

        read_only_role = setup_read_only_role()
        auth_models.UserRoleRelationship.objects.create(user=user, role=read_only_role)

    return user


class UtilsUtilitiesTestCase(TestAPIViewSetMixin, TestCase):

    def test_jwt_decode_handler_utility(self):
        # TODO: Change this token to some testing valid token
        dummy_token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6Ilg1ZVhrNHh5b2pORnVtMWtsMll0djhkbE5QNC1jNTdkTzZRR1RWQndhTmsiLCJ0eXAiOiJKV1QifQ.eyJ2ZXIiOiIxLjAiLCJpc3MiOiJodHRwczovL3VuaWNlZnBhcnRuZXJzLmIyY2xvZ2luLmNvbS80OGUwNTUyOS04OGI4LTQwZTEtODI1YS0xOGM0ZTEwNzdiM2EvdjIuMC8iLCJzdWIiOiIyYmZhNmEyZC05OTgwLTRmYjktODc0Ni0wODVkNjJkODk1M2QiLCJhdWQiOiI2N2MwMGQzYi00MGQ0LTRjZTAtYjk0Yy1hNTFlZDg5MWU4MGIiLCJleHAiOjE3MDMxNjA3NzYsIm5vbmNlIjoiZDZjNzdhNjItZjY1ZS00NTI5LWFlODYtNDE0OWM1Yzg0MzM4IiwiaWF0IjoxNzAzMTU3MTc2LCJhdXRoX3RpbWUiOjE3MDMxNTcxNzUsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0Lzc3NDEwMTk1LTE0ZTEtNGZiOC05MDRiLWFiMTg5MjAyMzY2Ny8iLCJuYW1lIjoiVmlrYXNoIEt1bWFyIiwiZW1haWxzIjpbInZpa2FzaC5rdW1hcjA1QG5hZ2Fycm8uY29tIl0sInRmcCI6IkIyQ18xX1VOSUNFRl9TT0NJQUxfc2lnbnVwX3NpZ25pbiIsIm5iZiI6MTcwMzE1NzE3Nn0.JCDv9NkYBmgj03p3nBRIizdtbHvr8oO7FK1jBZvwC351QchH13bzLoRpahYUetzEhXnqlVMQBzkjkEJYm5zH_GoX7aDfDhuGp3nYe5-JWEU_lqtJvBq5y5UsFb_ptoKyZVmOBWxD7DY3Bjd_IgfPVrV2ZXzJfiO_st3uZPOjKMAGEAvUcVH54rj7rWhfGLYQB1nTaUlPeYpWIElIGEm4cS00_5kJ5g38OKq973k0E8HBZBksgMMEzXq9nYCJk5Enhuljvtv3NZaCefn9tzRJjS7zYaGw21L1x6lHStVw-utDY-e36MoMdmTLc3vtmr-3i8niF2zv3X8oixFLnsKrng'

        try:
            auth_utilities.jwt_decode_handler(dummy_token)
            self.assertTrue(True)
        except Exception:
            self.assertFalse(False)

        try:
            auth_utilities.jwt_decode_handler('dummy_token')
            self.assertTrue(True)
        except Exception:
            self.assertFalse(False)

    def test_jwt_get_username_from_payload_handler_utility(self):
        self.assertEqual(type(auth_utilities.jwt_get_username_from_payload_handler({
            'emails': ['test.read_only11@test.com', ],
        })), str)

        self.assertEqual(auth_utilities.jwt_get_username_from_payload_handler({
            'emails': ['test.read_only11@test.com', ],
        }), 'test.read_only11@test.com')

        self.assertEqual(type(auth_utilities.jwt_get_username_from_payload_handler({
            'email': 'test.read_only11@test.com',
        })), str)

        self.assertEqual(auth_utilities.jwt_get_username_from_payload_handler({
            'email': 'test.read_only11@test.com',
        }), 'test.read_only11@test.com')
