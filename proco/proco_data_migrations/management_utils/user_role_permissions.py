from collections import OrderedDict

from django.db import transaction

from proco.core import utils as core_utilities
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
    Role.SYSTEM_ROLE_NAME_READ_ONLY: [RolePermission.CAN_DELETE_API_KEY, ],
})


def populate_role_permissions():
    for role_name, perms in role_permissions.items():
        role = Role.objects.get(name=role_name)
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
    UserRoleRelationship.objects.all().update(deleted=core_utilities.get_current_datetime_object())
    # ApplicationUser.objects.all().update(deleted=core_utilities.get_current_datetime_object())
    RolePermission.objects.all().update(deleted=core_utilities.get_current_datetime_object())
    Role.objects.all().update(deleted=core_utilities.get_current_datetime_object())


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


def populate_user_roles_and_permissions(apps, schema_editor):
    with transaction.atomic():
        clean_data()

        populate_roles_data()

        populate_role_permissions()

        # Assign permissions based on groups to the roles
        # populate_base_users()
