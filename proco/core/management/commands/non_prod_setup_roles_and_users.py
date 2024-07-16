from collections import OrderedDict

from django.core.management.base import BaseCommand
from django.db import transaction

from proco.custom_auth.models import Role, RolePermission, ApplicationUser, UserRoleRelationship


def populate_roles_data():
    # Create Role pre defined roles
    Role.objects.create(
        name=Role.SYSTEM_ROLE_NAME_ADMIN,
        description='Admin Role with all permissions',
        category=Role.ROLE_CATEGORY_SYSTEM,
    )

    Role.objects.create(
        name=Role.SYSTEM_ROLE_NAME_READ_ONLY,
        description='Read Only Role with GET/LIST permissions',
        category=Role.ROLE_CATEGORY_SYSTEM,
    )


role_permissions = OrderedDict({
    Role.SYSTEM_ROLE_NAME_ADMIN: [perm[0] for perm in RolePermission.PERMISSION_CHOICES],
    Role.SYSTEM_ROLE_NAME_READ_ONLY: [],
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


def clean_data():
    UserRoleRelationship.objects.all().delete()
    ApplicationUser.objects.all().delete()
    RolePermission.objects.all().delete()
    Role.objects.all().delete()


def create_user_role_relationship(user, role_name):
    role = Role.objects.get(name=role_name)
    UserRoleRelationship.objects.get_or_create(user=user, role=role)


def populate_base_users():
    super_user = ApplicationUser.objects.create(
        first_name='Admin',
        last_name='User',
        username="admin@test.com",
        email="admin@test.com",
        password="Ver1f1$$",
        is_superuser=True,
    )

    read_only_user = ApplicationUser.objects.create(
        first_name='Read Only',
        last_name='User',
        username="read_only_user@test.com",
        email="read_only_user@test.com",
        password="Ver1f1$$",
    )

    create_user_role_relationship(super_user, Role.SYSTEM_ROLE_NAME_ADMIN)
    create_user_role_relationship(read_only_user, Role.SYSTEM_ROLE_NAME_READ_ONLY)


class Command(BaseCommand):
    """requires roles to be setup"""

    def handle(self, **options):
        with transaction.atomic():
            clean_data()

            populate_roles_data()

            populate_role_permissions()

            # Assign permissions based on groups to the roles
            populate_base_users()
