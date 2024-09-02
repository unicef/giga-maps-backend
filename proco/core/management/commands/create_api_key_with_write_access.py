import logging

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.core.validators import validate_email
from django.db.models.functions.text import Lower
from django.shortcuts import get_object_or_404

from proco.accounts import models as accounts_models
from proco.core.utils import get_current_datetime_object, get_random_string
from proco.custom_auth.models import ApplicationUser
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)


def get_user(email, force_user, first_name, last_name, inactive_email):
    logger.info('Validating: {0}'.format(email))
    validate_email(email)

    application_user = ApplicationUser.objects.all().annotate(email_lower=Lower('email')).filter(
        email_lower=str(email).lower()).first()

    if not application_user and force_user:
        logger.info('Creating the superuser as user with given email does not exist.')
        args = ['-email={0}'.format(email)]
        if first_name:
            args.append('-first_name={0}'.format(first_name))
        if last_name:
            args.append('-last_name={0}'.format(last_name))
        if inactive_email:
            args.append('--inactive_email')

        call_command('create_admin_user', *args)

        application_user = ApplicationUser.objects.all().annotate(email_lower=Lower('email')).filter(
            email_lower=str(email).lower()).first()
    elif application_user:
        logger.info('User with given email already exists.')
    else:
        logger.error('User with give email address is not present in the system. '
                     'To force this user, please pass --force_user argument.')
        exit(0)
    return application_user


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-user', dest='user_email', required=True, type=str,
            help='Provide the superuser email address.'
        )

        parser.add_argument(
            '-api_code', dest='api_code', required=True, type=str,
            choices=['SCHOOL', 'DAILY_CHECK_APP', 'COUNTRY', 'MEASUREMENT', 'SCHOOL_CONNECTIVITY'],
            help='Provide the API Code from the list.'
        )

        parser.add_argument(
            '-reason', dest='reason', required=True, type=str,
            help='Reason for which this API Key is created.'
        )

        parser.add_argument(
            '-valid_till', dest='valid_till', type=str, default='31-12-2099',
            help='Date till this API key is valid, higher than today.'
        )

        parser.add_argument(
            '--force_user', action='store_true', dest='force_user', default=False,
            help='If provided, user will be created in Giga DB as superuser if does not exist.'
        )

        parser.add_argument(
            '-first_name', dest='user_first_name', type=str,
            help='User first name in case we want to create this user.'
        )
        parser.add_argument(
            '-last_name', dest='user_last_name', type=str,
            help='User last name in case we want to create this user.'
        )
        parser.add_argument(
            '--inactive_email', action='store_true', dest='inactive_email', default=False,
            help='If provided, user will be created as a non staff user.'
        )

    def handle(self, **options):
        logger.info('Creating API Key with write access.')
        user_email = options.get('user_email')
        force_user = options.get('force_user')

        api_key_user = get_user(
            user_email,
            force_user,
            options.get('user_first_name'),
            options.get('user_last_name'),
            options.get('inactive_email'),
        )
        api_code = options.get('api_code')
        reason = options.get('reason')
        valid_till_date = date_utilities.to_date(options.get('valid_till'))

        api_key_instance = accounts_models.APIKey.objects.filter(
            api__code=api_code,
            user=api_key_user,
            status=accounts_models.APIKey.APPROVED,
            valid_to__gte=get_current_datetime_object().date(),
            has_write_access=True,
        ).first()

        if api_key_instance:
            api_key_instance.write_access_reason = reason
            api_key_instance.valid_to = valid_till_date
            api_key_instance.save(update_fields=('write_access_reason', 'valid_to',))
            logger.info('Api key with write access updated successfully!\n')
        else:
            api_key_instance = accounts_models.APIKey.objects.create(
                api=get_object_or_404(accounts_models.API.objects.all(), code=api_code),
                user=api_key_user,
                api_key=get_random_string(264),
                status=accounts_models.APIKey.APPROVED,
                valid_to=valid_till_date,
                has_write_access=True,
                write_access_reason=reason,
            )
            logger.info('Api key with write access created successfully!\n')

        logger.info('Api key: {0}'.format(api_key_instance.api_key))
