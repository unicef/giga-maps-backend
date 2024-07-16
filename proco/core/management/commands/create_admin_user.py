# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.management.base import BaseCommand
from django.core.validators import validate_email
from django.utils import timezone

from proco.custom_auth.models import Role, ApplicationUser, UserRoleRelationship


def create_user_role_relationship(user, role_name):
    role = Role.objects.get(name=role_name)
    UserRoleRelationship.objects.filter(user=user).update(deleted=timezone.now())
    UserRoleRelationship.objects.get_or_create(user=user, role=role, deleted=None)


def valid_email(value):
    print('Validating: {0}'.format(value))
    validate_email(value)
    return value


class Command(BaseCommand):
    help = 'Create/Update the user as Superuser.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-email', dest='user_email', required=True, type=valid_email,
            help='Valid email address.'
        )
        parser.add_argument(
            '-first_name', dest='user_first_name', required=True, type=str,
            help='User first name in case we want to create this user.'
        )
        parser.add_argument(
            '-last_name', dest='user_last_name', required=True, type=str,
            help='User last name in case we want to create this user.'
        )
        parser.add_argument(
            '--inactive_email', action='store_true', dest='inactive_email', default=False,
            help='If provided, user will be created as staff.'
        )

    def handle(self, **options):
        user_email = options.get('user_email')
        print('*** User create/update operation STARTED ({0}) ***'.format(user_email))

        user_instance, created = ApplicationUser.objects.update_or_create(
            username=user_email,
            defaults={
                'email': user_email,
                'first_name': options.get('user_first_name', 'FName'),
                'last_name': options.get('user_last_name', 'LName'),
                'is_superuser': True,
                'is_staff': not options['inactive_email'],
                'is_active': True,
                'password': "*****",
            },
        )

        print(user_instance.__dict__)
        create_user_role_relationship(user_instance, Role.SYSTEM_ROLE_NAME_ADMIN)
        print('*** User create/update operation ENDED ({0}) ***'.format(user_email))
