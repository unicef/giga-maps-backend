# encoding: utf-8
import logging

from collections import OrderedDict

from django.core.management.base import BaseCommand
from django.db import transaction

from proco.custom_auth.models import Role, RolePermission

logger = logging.getLogger('gigamaps.' + __name__)

role_permissions = OrderedDict({
    Role.SYSTEM_ROLE_NAME_ADMIN: [perm[0] for perm in RolePermission.PERMISSION_CHOICES],
    Role.SYSTEM_ROLE_NAME_READ_ONLY: [RolePermission.CAN_DELETE_API_KEY, ],
})


def populate_role_permissions():
    for role_name, perms in role_permissions.items():
        role = Role.objects.get(name=role_name)
        # Delete all the permissions related to this role and then re-create
        RolePermission.objects.filter(role=role, ).delete()

        for perm in perms:
            RolePermission.objects.get_or_create(
                role=role,
                slug=perm,
                defaults={
                    'role': role,
                    'slug': perm,
                }
            )


class Command(BaseCommand):
    help = "Update the System Role's permissions as it can not be updated from GUI."

    def handle(self, **options):
        logger.info('System role update operation started.')
        with transaction.atomic():
            populate_role_permissions()
        logger.info('System role update operation ended.')
