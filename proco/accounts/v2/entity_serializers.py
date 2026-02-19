import copy
import decimal
import re
from datetime import timedelta
from math import floor, ceil

from django.apps import apps
from django.conf import settings
from django.contrib.admin.models import LogEntry
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.validators import validate_email
from django.db import connection
from django.db import transaction
from django.db.models import F, Min, Max
from django.db.models import Q
from django.db.models.functions.text import Lower
from rest_flex_fields.serializers import FlexFieldsModelSerializer
from rest_framework import serializers

from proco.accounts import exceptions as accounts_exceptions
from proco.accounts import models as accounts_models
from proco.accounts import utils as account_utilities
from proco.accounts.config import app_config as account_config
from proco.accounts.serializers import BaseDataLayerCRUDSerializer
from proco.connection_statistics.models import SchoolWeeklyStatus
from proco.core import db_utils as db_utilities
from proco.core import utils as core_utilities
from proco.custom_auth import models as auth_models
from proco.custom_auth.serializers import ExpandUserSerializer
from proco.custom_auth.utils import get_user_emails_for_permissions
from proco.locations import models as locations_models
from proco.schools.models import School
from proco.utils import dates as date_utilities
from proco.utils.cache import cache_manager


class ExpandAPISerializer(FlexFieldsModelSerializer):
    """
    ExpandAPISerializer
        This serializer is used for expandable feature.
    """

    class Meta:
        model = accounts_models.API
        fields = (
            'id',
            'code',
            'name',
            'category',
        )


class APIsListSerializer(FlexFieldsModelSerializer):
    """
    APIsListSerializer
        Serializer to list all APIs.
    """
    is_unlocked = serializers.SerializerMethodField()

    country_filter_applicable = serializers.SerializerMethodField()
    school_filter_applicable = serializers.SerializerMethodField()
    giga_id_filter_applicable = serializers.SerializerMethodField()
    indicator_filter_applicable = serializers.SerializerMethodField()
    date_range_filter_applicable = serializers.SerializerMethodField()

    download_url = serializers.SerializerMethodField()
    report_title = serializers.SerializerMethodField()

    default_filters = serializers.JSONField()

    class Meta:
        model = accounts_models.API
        read_only_fields = fields = (
            'id',
            'code',
            'name',
            'category',
            'description',
            'country_filter_applicable',
            'school_filter_applicable',
            'giga_id_filter_applicable',
            'indicator_filter_applicable',
            'date_range_filter_applicable',
            'documentation_url',
            'download_url',
            'report_title',
            'default_filters',
            'is_unlocked',
            'created',
            'last_modified_at',
        )

    def get_is_unlocked(self, api_instance):
        # Check if API has valid key for the user
        request_user = core_utilities.get_current_user(context=self.context)
        if request_user and not request_user.is_anonymous:
            return api_instance.api_keys.all().filter(
                user=request_user,
                status=accounts_models.APIKey.APPROVED,
                valid_to__gte=core_utilities.get_current_datetime_object().date(),
                has_write_access=False,
            ).exists()
        return False

    def get_country_filter_applicable(self, api_instance):
        # Check if Country filter is applicable for API
        default_filters = api_instance.default_filters
        if isinstance(default_filters, dict):
            return 'country_id' in default_filters
        return False

    def get_school_filter_applicable(self, api_instance):
        # Check if School filter is applicable for API
        default_filters = api_instance.default_filters
        if isinstance(default_filters, dict):
            return 'school_id' in default_filters
        return False

    def get_giga_id_filter_applicable(self, api_instance):
        # Check if School GIGA ID filter is applicable for API
        default_filters = api_instance.default_filters
        if isinstance(default_filters, dict):
            return 'giga_id_school' in default_filters
        return False

    def get_indicator_filter_applicable(self, api_instance):
        # Check if indicator filter is applicable for API
        default_filters = api_instance.default_filters
        if isinstance(default_filters, dict):
            return 'indicator' in default_filters
        return False

    def get_date_range_filter_applicable(self, api_instance):
        # Check if Start Date and End Date filter is applicable for API
        default_filters = api_instance.default_filters
        if isinstance(default_filters, dict):
            return 'start_date' in default_filters or 'end_date' in default_filters
        return False

    def apply_api_key_filters(self, filters):
        return filters if isinstance(filters, dict) > 0 else {}

    def get_download_url(self, api_instance):
        if api_instance.download_url:
            request_user = core_utilities.get_current_user(context=self.context)

            if request_user and not request_user.is_anonymous:
                valid_api_key = api_instance.api_keys.all().filter(
                    user=request_user,
                    status=accounts_models.APIKey.APPROVED,
                    valid_to__gte=core_utilities.get_current_datetime_object().date(),
                    has_write_access=False,
                ).first()

                if valid_api_key:
                    filters = self.apply_api_key_filters(valid_api_key.filters)
                    return api_instance.download_url.format(**filters)
        return api_instance.download_url

    def get_report_title(self, api_instance):
        if account_utilities.is_giga_meter_api(api_instance):
            return

        report_file_name = api_instance.report_title
        if (
            api_instance.category == accounts_models.API.API_CATEGORY_PUBLIC and
            core_utilities.is_blank_string(report_file_name)
        ):
            report_file_name = str('_'.join([api_instance.name, api_instance.category, '{dt}']))

        return report_file_name.format(
            dt=date_utilities.format_datetime(core_utilities.get_current_datetime_object(), frmt='%d-%m-%Y_%H-%M-%S'),
        )


class APIKeyCountryRelationshipSerializer(serializers.ModelSerializer):
    class Meta:
        model = accounts_models.APIKeyCountryRelationship

        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
        )

        fields = read_only_fields + (
            'api_key',
            'country',
        )

        extra_kwargs = {
            'api_key': {'required': True},
            'country': {'required': True},
        }

    def create(self, validated_data):
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            # set created_by and last_modified_by value
            if request_user is not None:
                validated_data['created_by'] = validated_data.get('created_by') or request_user
                validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user

            instance = super().create(validated_data)
        return instance


class APIKeyAPICategoryRelationshipSerializer(serializers.ModelSerializer):
    class Meta:
        model = accounts_models.APIKeyAPICategoryRelationship

        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
        )

        fields = read_only_fields + (
            'api_key',
            'api_category',
        )

        extra_kwargs = {
            'api_key': {'required': True},
            'api_category': {'required': True},
        }

    def validate_api_category(self, api_category):
        api_instance = self.context.get('api_instance')

        if api_instance and api_instance.api_categories.filter(id=api_category.id, deleted__isnull=True).exists():
            return api_category
        raise accounts_exceptions.InvalidAPICategoryAssignedError()

    def create(self, validated_data):
        request_user = core_utilities.get_current_user(context=self.context)
        # set created_by and last_modified_by value
        if request_user is not None:
            validated_data['created_by'] = validated_data.get('created_by') or request_user
            validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user

        instance = super().create(validated_data)
        return instance


class APICategoriesCRUDSerializer(FlexFieldsModelSerializer):
    """
    APICategoriesCRUDSerializer
        Serializer to list/create/update all API Categories.
    """

    class Meta:
        model = accounts_models.APICategory
        read_only_fields = (
            'id',
        )
        fields = read_only_fields + (
            'code',
            'name',
            'description',
            'api',
            'is_default',
            'created_by',
            'last_modified_by',
        )

        expandable_fields = {
            'api': (ExpandAPISerializer, {'source': 'api'}),
            'last_modified_by': (ExpandUserSerializer, {'source': 'last_modified_by'}),
            'created_by': (ExpandUserSerializer, {'source': 'created_by'}),
        }

    def validate_code(self, code):
        if re.match(r'[A-Za-z0-9-\' _]*$', code):
            code = str(code).upper()
            # If its Existing API Category, then code should match. Else raise error
            if self.instance and code != self.instance.code:
                raise accounts_exceptions.InvalidAPICategoryCodeError(message_kwargs={'code': code})

            # If its new API Category, then code should be unique. Else raise error
            if not self.instance and accounts_models.APICategory.objects.filter(code__iexact=code).exists():
                raise accounts_exceptions.DuplicateAPICategoryCodeError(message_kwargs={'code': code})
            return code

        raise accounts_exceptions.InvalidAPICategoryCodeError()

    def validate_name(self, name):
        if re.match(account_config.valid_name_pattern, name):
            return name
        raise accounts_exceptions.InvalidAPICategoryNameError()

    def validate_is_default(self, is_default):
        if is_default:
            api_instance = self.context.get('api_instance')
            if api_instance and accounts_models.APICategory.objects.filter(api_id=api_instance.id,
                                                                           is_default=True).exists():
                raise accounts_exceptions.InvalidDefaultAPICategoryError()

        return is_default


class APIKeysListSerializer(FlexFieldsModelSerializer):
    """
    APIKeysListSerializer
        Serializer to list all API Keys.
    """

    filters = serializers.JSONField()
    is_active = serializers.SerializerMethodField()
    has_active_extension_request = serializers.SerializerMethodField()

    active_countries_list = serializers.JSONField()
    active_api_categories_list = serializers.JSONField()

    class Meta:
        model = accounts_models.APIKey
        read_only_fields = fields = (
            'id',
            'api_key',
            'description',
            'valid_from',
            'valid_to',
            'user',
            'api',
            'filters',
            'status',
            'status_updated_by',
            'created',
            'last_modified_at',
            'is_active',
            'extension_valid_to',
            'extension_status',
            'extension_status_updated_by',
            'has_active_extension_request',
            'active_countries_list',
            'active_api_categories_list',
        )

        expandable_fields = {
            'user': (ExpandUserSerializer, {'source': 'user'}),
            'api': (ExpandAPISerializer, {'source': 'api'}),
            'status_updated_by': (ExpandUserSerializer, {'source': 'status_updated_by'}),
            'last_modified_by': (ExpandUserSerializer, {'source': 'last_modified_by'}),
            'created_by': (ExpandUserSerializer, {'source': 'created_by'}),
            'extension_status_updated_by': (ExpandUserSerializer, {'source': 'extension_status_updated_by'}),
        }

    def get_is_active(self, api_key_instance):
        """
        get_is_active
            API key is in-active when it has expired or rejected by Admin.
        """
        return (api_key_instance.valid_to >= core_utilities.get_current_datetime_object().date() and
                api_key_instance.status == accounts_models.APIKey.APPROVED)

    def get_has_active_extension_request(self, api_key_instance):
        """
        get_has_active_extension_request
            API key is in-active when it has expired or rejected by Admin.
        """
        return (api_key_instance.extension_valid_to is not None and
                api_key_instance.extension_valid_to >= core_utilities.get_current_datetime_object().date() and
                api_key_instance.extension_status == accounts_models.APIKey.INITIATED and
                api_key_instance.valid_to >= core_utilities.get_current_datetime_object().date() and
                api_key_instance.status == accounts_models.APIKey.APPROVED)

    def to_representation(self, api_key):
        all_category_ids = list(api_key.active_categories.all().values_list('api_category_id', flat=True))
        if len(all_category_ids) > 0:
            active_category_list = list(accounts_models.APICategory.objects.filter(
                id__in=all_category_ids).values('id', 'name', 'code', 'is_default'))
        else:
            active_category_list = list(accounts_models.APICategory.objects.filter(
                is_default=True,
                api=api_key.api,
            ).values('id', 'name', 'code', 'is_default'))

        setattr(api_key, 'active_api_categories_list', active_category_list)

        all_country_ids = list(api_key.active_countries.all().values_list('country_id', flat=True))
        if len(all_country_ids) > 0:
            active_countries_list = list(
                locations_models.Country.objects.filter(id__in=all_country_ids).values('id', 'name'))
        else:
            active_countries_list = []

        setattr(api_key, 'active_countries_list', active_countries_list)
        return super().to_representation(api_key)


class CreateAPIKeysSerializer(serializers.ModelSerializer):
    """
    CreateAPIKeysSerializer
        Serializer to create API Key.
    """

    active_countries_list = serializers.JSONField(required=False)

    class Meta:
        model = accounts_models.APIKey

        read_only_fields = (
            'id',
            'api_key',
            'valid_from',
            'user',
        )

        fields = read_only_fields + (
            'api',
            'filters',
            'status',
            'valid_to',
            'active_countries_list',
            'description',
        )

    def validate_status(self, status):
        api_instance = self.context.get('api_instance')

        if api_instance:
            if (
                api_instance.category == accounts_models.API.API_CATEGORY_PUBLIC and
                status != accounts_models.APIKey.APPROVED
            ):
                raise accounts_exceptions.InvalidAPIKeyStatusForPublicAPIError()
            elif (
                api_instance.category == accounts_models.API.API_CATEGORY_PRIVATE and
                status != accounts_models.APIKey.INITIATED
            ):
                raise accounts_exceptions.InvalidAPIKeyStatusForPrivateAPIError()
        return status

    def validate_filters(self, filters):
        api_instance = self.context.get('api_instance')

        if api_instance and api_instance.default_filters and len(api_instance.default_filters) > 0:
            if not filters or not isinstance(filters, dict):
                raise accounts_exceptions.InvalidAPIKeyFiltersError()

        return filters

    def _validate_active_api_key_count_for_giga_meter(self, data, api_instance, request_user, active_countries_list):
        if not active_countries_list or len(active_countries_list) == 0:
            raise accounts_exceptions.CountryRequiredForGigaMeterAPIKeyError()

        total_active_giga_meter_api_keys = accounts_models.APIKey.objects.filter(
            api=data['api'],
            user=request_user,
            status__in=[accounts_models.APIKey.INITIATED, accounts_models.APIKey.APPROVED],
            valid_to__gte=core_utilities.get_current_datetime_object().date(),
            has_write_access=False,
        ).count()

        if total_active_giga_meter_api_keys > 100:
            message_kwargs = {
                'limit': 100,
                'count': total_active_giga_meter_api_keys,
                'details': 'Name - "{0}"'.format(api_instance.name)
            }
            raise accounts_exceptions.InvalidActiveAPIKeyCountForSingleAPIError(message_kwargs=message_kwargs)

        return True

    def _validate_active_api_key_count_error(self, data, api_instance, request_user, active_countries_list):
        if api_instance and api_instance.category == accounts_models.API.API_CATEGORY_PUBLIC:
            api_key_instance = accounts_models.APIKey.objects.filter(
                api=data['api'],
                user=request_user,
                status__in=[accounts_models.APIKey.INITIATED, accounts_models.APIKey.APPROVED],
                valid_to__gte=core_utilities.get_current_datetime_object().date(),
                has_write_access=False,
            ).first()

            if api_key_instance:
                message_kwargs = {
                    'limit': 1,
                    'count': 1,
                    'details': 'Name - "{0}", Valid Till - "{1}"'.format(
                        api_instance.name,
                        date_utilities.format_date(api_key_instance.valid_to),
                    )
                }
                raise accounts_exceptions.InvalidActiveAPIKeyCountForSingleAPIError(message_kwargs=message_kwargs)
        else:
            if not active_countries_list or len(active_countries_list) == 0:
                raise accounts_exceptions.CountryRequiredForPrivateAPIKeyError()

        return True

    def _get_status_by_api_category(self, api_instance, request_user):
        # If API key is created for a Public API, then update status as APPROVED
        # If API key is created by Admin/Superuser, then also mark it as APPROVED
        if (
            (api_instance and api_instance.category == accounts_models.API.API_CATEGORY_PUBLIC) or
            core_utilities.is_superuser(request_user) or
            request_user.permissions.get(auth_models.RolePermission.CAN_APPROVE_REJECT_API_KEY, False)
        ):
            return accounts_models.APIKey.APPROVED
        return accounts_models.APIKey.INITIATED

    def create(self, validated_data):
        api_instance = self.context.get('api_instance')
        active_countries_list = validated_data.pop('active_countries_list', [])
        request_user = core_utilities.get_current_user(context=self.context)

        if account_utilities.is_giga_meter_api(api_instance):
            self._validate_active_api_key_count_for_giga_meter(
                validated_data,
                api_instance,
                request_user,
                active_countries_list
            )
        else:
            self._validate_active_api_key_count_error(
                validated_data,
                api_instance,
                request_user,
                active_countries_list
            )

        with transaction.atomic():
            # set created_by and last_modified_by value
            if request_user is not None:
                validated_data['user'] = request_user
                validated_data['api_key'] = core_utilities.get_random_string(264)
                validated_data['status'] = self._get_status_by_api_category(api_instance, request_user)
                validated_data['created_by'] = validated_data.get('created_by') or request_user
                validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user
                validated_data['valid_to'] = core_utilities.get_current_datetime_object().date() + timedelta(days=90)
            instance = super().create(validated_data)

            for country_id in active_countries_list:
                api_key_country_data = {
                    'api_key': instance.id,
                    'country': country_id,
                }

                api_key_country_relationships = APIKeyCountryRelationshipSerializer(
                    data=api_key_country_data,
                    context=self.context,
                )
                api_key_country_relationships.is_valid(raise_exception=True)
                api_key_country_relationships.save()

            # Once API Key is created, send the status email to the user
            if (
                settings.ENABLED_API_KEY_EMAILS and
                request_user is not None and
                instance.status == accounts_models.APIKey.APPROVED
            ):
                email_content = {
                    'subject': account_config.api_key_generation_email_subject_format % (
                        core_utilities.get_project_title(), str(instance.api.name).title(),
                    ),
                    'user_name': request_user.first_name + ' ' + request_user.last_name,
                    'template': account_config.api_key_generated_email_template,
                    'api_name': str(instance.api.name).title(),
                }
                account_utilities.send_email_over_mailjet_service([request_user], **email_content)

            elif settings.ENABLED_API_KEY_EMAILS and instance.status == accounts_models.APIKey.INITIATED:
                countries = []
                if len(active_countries_list) == locations_models.Country.objects.all().count():
                    countries.append('All countries')
                elif len(active_countries_list) > 10:
                    countries.append('{0} countries'.format(len(active_countries_list)))
                elif len(active_countries_list) > 0:
                    countries = list(locations_models.Country.objects.filter(
                        id__in=active_countries_list,
                    ).values_list('name', flat=True).order_by('name'))
                else:
                    countries.append('0 countries')

                email_subject = account_config.private_api_key_generation_email_subject_format % (
                    core_utilities.get_project_title(), str(instance.api.name).title(),
                )

                email_message = account_config.private_api_key_generation_email_message_format
                email_message = email_message.format(
                    api_name=str(instance.api.name).title(),
                    requested_user=request_user.first_name + str(
                        ' ' + request_user.last_name if not core_utilities.is_blank_string(
                            request_user.last_name) else '') + ' (' + request_user.email + ')',
                    countries=', '.join(countries),
                    description=instance.description,
                    dashboard_url='API Key Dashboard url: {}'.format(settings.API_KEY_ADMIN_DASHBOARD_URL)
                    if settings.API_KEY_ADMIN_DASHBOARD_URL else '')

                email_content = {
                    'subject': email_subject,
                    'message': email_message,
                    'user_name': 'Admin',
                }

                # Once API Key request initiated for approval, send the email to the APPROVERS
                request_approvers = get_user_emails_for_permissions(
                    [auth_models.RolePermission.CAN_APPROVE_REJECT_API_KEY],
                )

                if len(request_approvers) > 0:
                    account_utilities.send_email_over_mailjet_service(request_approvers, **email_content)
            return instance

    def to_representation(self, api_key):
        all_country_ids = list(api_key.active_countries.all().values_list('country_id', flat=True))
        if len(all_country_ids) > 0:
            active_countries_list = list(
                locations_models.Country.objects.filter(id__in=all_country_ids).values('id', 'name'))
        else:
            active_countries_list = []

        setattr(api_key, 'active_countries_list', active_countries_list)
        return super().to_representation(api_key)


class UpdateAPIKeysSerializer(serializers.ModelSerializer):
    """
    UpdateAPIKeysSerializer
        Serializer to approve/decline API key request or to extend the key validity.
    """

    class Meta:
        model = accounts_models.APIKey
        read_only_fields = (
            'id',
            'api_key',
            'valid_from',
            'user',
            'api',
            'filters',
        )

        fields = read_only_fields + (
            'valid_to',
            'status',
            'extension_valid_to',
            'extension_status',
        )

    def validate_valid_to(self, valid_to):
        """
        Validation error if valid_to is passed in the Update body, and it is more than 90 days from current date
        """
        if (
            valid_to and
            valid_to > core_utilities.get_current_datetime_object().date() + timedelta(days=90)
        ):
            raise accounts_exceptions.InvalidAPIKeyFiltersError()
        return valid_to

    def validate_status(self, status):
        api_instance = self.context.get('api_instance')

        if api_instance:
            if (
                api_instance.category == accounts_models.API.API_CATEGORY_PRIVATE and
                status not in [accounts_models.APIKey.APPROVED, accounts_models.APIKey.DECLINED, ]
            ):
                raise accounts_exceptions.InvalidAPIKeyStatusForPrivateAPIError()
            elif api_instance.category == accounts_models.API.API_CATEGORY_PUBLIC:
                raise accounts_exceptions.InvalidAPIKeyStatusForPublicAPIError()
        return status

    def _validate_extension_valid_to(self, instance, extension_valid_to):
        """
        Validation error if extension_valid_to is passed in the Update body,
        and it is more than 90 days from current date or less than/equal to current valid date
        """
        if extension_valid_to:
            if extension_valid_to < instance.valid_to:
                message_kwargs = {
                    'msg': f'Extension date "{extension_valid_to}" can not be less than/equal '
                           f'to current validity date "{instance.valid_to}"'
                }
                raise accounts_exceptions.InvalidAPIKeyExtensionError(message_kwargs=message_kwargs)

            elif extension_valid_to > core_utilities.get_current_datetime_object().date() + timedelta(days=365):
                message_kwargs = {
                    'msg': f'Extension date "{extension_valid_to}" can not be more than 365 days'
                }
                raise accounts_exceptions.InvalidAPIKeyExtensionError(message_kwargs=message_kwargs)
        return extension_valid_to

    def _validate_extension_status(self, instance, extension_status):
        if (
            instance and instance.status == accounts_models.APIKey.APPROVED and
            instance.valid_to >= core_utilities.get_current_datetime_object().date()
        ):
            if extension_status in [accounts_models.APIKey.APPROVED, accounts_models.APIKey.DECLINED]:
                return extension_status
        raise accounts_exceptions.InvalidAPIKeyExtensionStatusError()

    def update(self, instance, validated_data):
        """
        update
            This method is used to update API key
        :param instance:
        :param validated_data:
        :return:
        """
        # Only Admin has update permission on API key objects
        # If Admin updating the extension fields then
        # 1. Check if valid extension_status
        # 2. If valid extension_status, then set the extension_valid_to to 90 days from now if not provided
        # 3. If extension_status == APPROVED, then extend the valid_to date to extension date
        if 'extension_status' in validated_data:
            extension_status = validated_data.get('extension_status')
            self._validate_extension_status(instance, extension_status)
            if 'extension_valid_to' not in validated_data:
                validated_data['extension_valid_to'] = instance.extension_valid_to

            if extension_status == accounts_models.APIKey.APPROVED:
                validated_data['valid_to'] = validated_data['extension_valid_to']
        elif validated_data.get('status', None) == accounts_models.APIKey.APPROVED:
            validated_data['valid_to'] = core_utilities.get_current_datetime_object().date() + timedelta(days=90)

        if 'extension_valid_to' in validated_data:
            self._validate_extension_valid_to(instance, validated_data.get('extension_valid_to'))

        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            if request_user is not None:
                if 'status' in validated_data:
                    validated_data['status_updated_by'] = request_user
                if 'extension_status' in validated_data:
                    validated_data['extension_status_updated_by'] = request_user

                validated_data['last_modified_by'] = request_user

            instance = super().update(instance, validated_data)

        if settings.ENABLED_API_KEY_EMAILS and instance.user is not None:
            email_content = {
                'api_name': str(instance.api.name).title(),
                'user_name': instance.user.first_name + ' ' + instance.user.last_name,
            }

            # Once API Key is APPROVED/REJECTED, send the status email to the user
            if validated_data.get('status', None) == accounts_models.APIKey.APPROVED:
                email_content['subject'] = account_config.api_key_generation_email_subject_format % (
                        core_utilities.get_project_title(), email_content['api_name'],
                    )
                email_content['template'] = account_config.api_key_generated_email_template

            elif validated_data.get('status', None) == accounts_models.APIKey.DECLINED:
                email_content['subject'] = account_config.private_api_key_rejected_email_subject_format % (
                    core_utilities.get_project_title(), email_content['api_name'],
                )
                email_content['template'] = account_config.api_key_rejected_email_template

            # Once API Key extension is APPROVED/REJECTED, send the status email to the user
            elif validated_data.get('extension_status', None) == accounts_models.APIKey.APPROVED:
                email_content['subject'] = account_config.api_key_extension_approved_email_subject_format % (
                    core_utilities.get_project_title(), email_content['api_name'],
                )
                email_content['template'] = account_config.api_key_extension_approved_email_template

            elif validated_data.get('extension_status', None) == accounts_models.APIKey.DECLINED:
                email_content['subject'] = account_config.api_key_extension_rejected_email_subject_format % (
                    core_utilities.get_project_title(), email_content['api_name'],
                )
                email_content['template'] = account_config.api_key_extension_rejected_email_template

            if 'subject' in email_content:
                account_utilities.send_email_over_mailjet_service([instance.user], **email_content)

        return instance


class UpdateAPIKeysForExtensionSerializer(serializers.ModelSerializer):
    """
    UpdateAPIKeysForExtensionSerializer
        Serializer to approve/decline API key request or to extend the key validity.
    """

    class Meta:
        model = accounts_models.APIKey
        read_only_fields = (
            'id',
            'api_key',
            'valid_from',
            'valid_to',
            'status',
            'user',
            'api',
            'filters',
            'extension_status',
        )

        fields = read_only_fields + (
            'extension_valid_to',
        )

    def _validate_has_active_extension_request(self, api_key_instance):
        """
        get_has_active_extension_request
            API key is in-active when it has expired or rejected by Admin.
        """
        has_active_request = (
            api_key_instance.extension_valid_to is not None and
            api_key_instance.extension_valid_to >= core_utilities.get_current_datetime_object().date() and
            api_key_instance.extension_status == accounts_models.APIKey.INITIATED and
            api_key_instance.valid_to >= core_utilities.get_current_datetime_object().date() and
            api_key_instance.status == accounts_models.APIKey.APPROVED
        )
        if has_active_request:
            message_kwargs = {
                'msg': 'Invalid API Key Extension Request as an active request already logged'
            }
            raise accounts_exceptions.InvalidAPIKeyExtensionError(message_kwargs=message_kwargs)

    def validate_extension_valid_to(self, extension_valid_to):
        """
        Validation error if valid_to is passed in the Update body, and it is more than 90 days from current date
        """
        if (
            extension_valid_to and
            (
                extension_valid_to <= core_utilities.get_current_datetime_object().date() + timedelta(days=365) or
                core_utilities.is_superuser(core_utilities.get_current_user(context=self.context))
            )
        ):
            return extension_valid_to
        message_kwargs = {
            'msg': 'Invalid API Key Extension Request Date as only 365 days extension is allowed from the current date'
        }
        raise accounts_exceptions.InvalidAPIKeyExtensionError(message_kwargs=message_kwargs)

    def _get_extension_status_by_api_key_status(self, instance, request_user):
        if instance.user.id != request_user.id:
            # If extension is not request for own API key, then raise the error
            raise accounts_exceptions.InvalidAPIKeyExtensionStatusError()

        if (
            instance and instance.status == accounts_models.APIKey.APPROVED and
            instance.valid_to >= core_utilities.get_current_datetime_object().date()
        ):
            if core_utilities.is_superuser(request_user):
                # Extension can be requested only for an APPROVED and ACTIVE API key
                return accounts_models.APIKey.APPROVED

            # Extension can be requested only for an APPROVED and ACTIVE API key
            return accounts_models.APIKey.INITIATED
        raise accounts_exceptions.InvalidAPIKeyExtensionStatusError()

    def update(self, instance, validated_data):
        """
        update
            This method is used to update API key
        :param instance:
        :param validated_data:
        :return:
        """
        request_user = core_utilities.get_current_user(context=self.context)

        self._validate_has_active_extension_request(instance)

        validated_data['extension_status'] = self._get_extension_status_by_api_key_status(instance, request_user)
        if 'extension_valid_to' not in validated_data:
            validated_data['extension_valid_to'] = core_utilities.get_current_datetime_object().date() + timedelta(
                days=90)

        if validated_data['extension_status'] == accounts_models.APIKey.APPROVED:
            validated_data['valid_to'] = validated_data['extension_valid_to']
            validated_data['extension_status_updated_by'] = request_user

        if request_user is not None:
            validated_data['last_modified_by'] = request_user

        instance = super().update(instance, validated_data)

        if settings.ENABLED_API_KEY_EMAILS and instance.extension_status == accounts_models.APIKey.INITIATED:
            email_subject = account_config.private_api_key_extension_request_email_subject_format % (
                core_utilities.get_project_title(), str(instance.api.name).title(),
            )

            email_message = account_config.private_api_key_extension_request_email_message_format
            email_message = email_message.format(
                api_name=str(instance.api.name).title(),
                requested_user=request_user.first_name + str(
                    ' ' + request_user.last_name if not core_utilities.is_blank_string(
                        request_user.last_name) else '') + ' (' + request_user.email + ')',
                till_date=date_utilities.format_date(instance.extension_valid_to),
                dashboard_url='API Key Dashboard url: {}'.format(settings.API_KEY_ADMIN_DASHBOARD_URL)
                if settings.API_KEY_ADMIN_DASHBOARD_URL else '')

            email_content = {
                'subject': email_subject,
                'message': email_message,
                'user_name': 'Admin',
            }

            # Once API Key extension request initiated for approval, send the email to the APPROVERS
            request_approvers = get_user_emails_for_permissions(
                [auth_models.RolePermission.CAN_APPROVE_REJECT_API_KEY],
            )
            if len(request_approvers) > 0:
                account_utilities.send_email_over_mailjet_service(request_approvers, **email_content)
        return instance


class UpdateAPIKeysForAPICategoriesSerializer(serializers.ModelSerializer):
    """
    UpdateAPIKeysForAPICategoriesSerializer
        Serializer to add the API category to an API key.
    """

    active_api_categories_list = serializers.JSONField(required=True)

    class Meta:
        model = accounts_models.APIKey
        read_only_fields = (
            'id',
            'status',
            'api',
            'user',
        )

        fields = read_only_fields + (
            'active_api_categories_list',
        )

    def update(self, instance, validated_data):
        """
        update
            This method is used to update API key + API Category relationship
        :param instance:
        :param validated_data:
        :return:
        """
        request_user = core_utilities.get_current_user(context=self.context)
        active_api_categories_list = validated_data.pop('active_api_categories_list', [])
        instance.active_categories.all().update(
            deleted=core_utilities.get_current_datetime_object(),
            last_modified_by=request_user,
        )

        for category_id in active_api_categories_list:
            api_key_category_data = {
                'api_key': instance.id,
                'api_category': category_id,
            }

            api_key_category_relationships = APIKeyAPICategoryRelationshipSerializer(
                data=api_key_category_data,
                context=self.context,
            )
            api_key_category_relationships.is_valid(raise_exception=True)
            api_key_category_relationships.save()

        # Once API Key - Category relationship is created, send the status email to the user
        if settings.ENABLED_API_KEY_EMAILS and request_user is not None:
            email_subject = account_config.api_key_api_category_on_update_email_subject_format % (
                core_utilities.get_project_title(), instance.api.name,
            )
            email_message = account_config.api_key_api_category_on_update_email_message_format
            email_content = {'subject': email_subject, 'message': email_message}
            account_utilities.send_standard_email(request_user, email_content)
        return instance

    def to_representation(self, api_key):
        all_category_ids = list(api_key.active_categories.all().values_list('api_category_id', flat=True))
        if len(all_category_ids) > 0:
            active_api_categories_list = list(accounts_models.APICategory.objects.filter(
                id__in=all_category_ids).values('id', 'code', 'name', 'is_default'))
        else:
            active_api_categories_list = list(accounts_models.APICategory.objects.filter(
                is_default=True,
                api=api_key.api,
            ).values('id', 'name', 'code', 'is_default'))

        setattr(api_key, 'active_api_categories_list', active_api_categories_list)
        return super().to_representation(api_key)


class MessageListSerializer(serializers.ModelSerializer):
    recipient = serializers.JSONField()

    class Meta:
        model = accounts_models.Message
        read_only_fields = fields = (
            'id',
            'severity',
            'type',
            'sender',
            'recipient',
            'is_sent',
            'subject_text',
            'message_text',
            'template',
            'description',
            'created',
            'last_modified_at',
        )


class SendNotificationSerializer(serializers.ModelSerializer):
    """
    SendNotificationSerializer
        Serializer to create Email Notification and send the email if valid email.
    """

    recipient = serializers.JSONField()

    class Meta:
        model = accounts_models.Message

        read_only_fields = (
            'id',
        )

        fields = read_only_fields + (
            'severity',
            'type',
            'sender',
            'recipient',
            'is_sent',
            'subject_text',
            'message_text',
            'template',
            'description',
        )

        extra_kwargs = {
            'type': {'required': True},
            'recipient': {'required': True},
            'subject_text': {'required': True},
            'message_text': {'required': True},
        }

    def validate_recipient(self, recipients):
        if isinstance(recipients, str) or isinstance(recipients, int):
            recipients = [recipients, ]

        message_type = self.initial_data.get('type')

        if message_type == accounts_models.Message.TYPE_EMAIL:
            if isinstance(recipients, list):
                for email in recipients:
                    validate_email(email)
                return recipients
            raise accounts_exceptions.InvalidEmailId()
        # For SMS notification
        elif message_type == accounts_models.Message.TYPE_SMS:
            if isinstance(recipients, list):
                for phone_number in recipients:
                    if (
                        core_utilities.is_blank_string(phone_number) or
                        not core_utilities.is_valid_mobile_number(phone_number)
                    ):
                        raise accounts_exceptions.InvalidPhoneNumberError()
                return recipients
            raise accounts_exceptions.InvalidPhoneNumberError()
        elif message_type == accounts_models.Message.TYPE_NOTIFICATION:
            if isinstance(recipients, list):
                for user_id in recipients:
                    if (
                        core_utilities.is_blank_string(user_id) or
                        not isinstance(user_id, int) or
                        not auth_models.ApplicationUser.objects.filter(id=user_id).exists()
                    ):
                        raise accounts_exceptions.InvalidUserIdError()
                return recipients
            raise accounts_exceptions.InvalidPhoneNumberError()

        return recipients

    def to_internal_value(self, data):
        """
        to_internal_value
            Add fields in initial data for Message instance.
        :param data:
        :return:
        """
        if not data:
            data = {}
        else:
            message_type = data.get('type')
            if message_type == accounts_models.Message.TYPE_EMAIL:
                data['sender'] = settings.DEFAULT_FROM_EMAIL
                data['template'] = data['template'] \
                    if 'template' in data \
                    else account_config.standard_email_template_name
        return super().to_internal_value(data)

    def create(self, validated_data):
        message_type = validated_data.get('type')

        # if its Email notification, send the email over MailJet service and update the status
        if message_type == accounts_models.Message.TYPE_EMAIL:
            email_content = copy.deepcopy(validated_data)
            email_content.update({'subject': validated_data['subject_text'], 'message': validated_data['message_text']})
            response = account_utilities.send_email_over_mailjet_service(validated_data['recipient'], **email_content)

            if isinstance(response, int):
                validated_data['is_sent'] = True

        # if it's just an application level notification, create the message instance
        else:
            validated_data['is_sent'] = True

        with transaction.atomic():
            instance = super().create(validated_data)
        return instance


class ExpandDataSourceSerializer(FlexFieldsModelSerializer):
    request_config = serializers.JSONField()
    column_config = serializers.JSONField()

    class Meta:
        model = accounts_models.DataSource
        fields = (
            'id',
            'name',
            'description',
            'version',
            'data_source_type',
            'request_config',
            'column_config',
            'status',
            # 'published_by',
            # 'published_at',
            # 'last_modified_at',
            # 'last_modified_by',
            # 'created',
            # 'created_by',
        )


class DataSourceListSerializer(FlexFieldsModelSerializer):
    request_config = serializers.JSONField()
    column_config = serializers.JSONField()

    class Meta:
        model = accounts_models.DataSource
        read_only_fields = fields = (
            'id',
            'name',
            'description',
            'version',
            'data_source_type',
            'request_config',
            'column_config',
            'status',
            'published_by',
            'published_at',
            'last_modified_at',
            'last_modified_by',
            'created',
            'created_by',
        )

        expandable_fields = {
            'published_by': (ExpandUserSerializer, {'source': 'published_by'}),
            'last_modified_by': (ExpandUserSerializer, {'source': 'last_modified_by'}),
            'created_by': (ExpandUserSerializer, {'source': 'created_by'}),
        }


class BaseDataSourceCRUDSerializer(serializers.ModelSerializer):
    request_config = serializers.JSONField()
    column_config = serializers.JSONField()

    def validate_name(self, name):
        if re.match(account_config.valid_name_pattern, name):
            if accounts_models.DataSource.objects.filter(name=name).exists():
                raise accounts_exceptions.DuplicateDataSourceNameError(message_kwargs={'name': name})
            return name
        raise accounts_exceptions.InvalidDataSourceNameError()

    def validate_column_config(self, column_config):
        if isinstance(column_config, dict) and len(column_config) > 0:
            column_config = [column_config]

        errors = False

        if isinstance(column_config, list) and len(column_config) > 0:
            for column in column_config:
                if (
                    core_utilities.is_blank_string(column.get('name', None)) or
                    core_utilities.is_blank_string(column.get('type', None))
                ):
                    errors = True
        if not errors:
            return column_config

        raise accounts_exceptions.InvalidDataSourceColumnConfigError()


class CreateDataSourceSerializer(BaseDataSourceCRUDSerializer):
    class Meta:
        model = accounts_models.DataSource

        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
        )

        fields = read_only_fields + (
            'name',
            'description',
            'version',
            'data_source_type',
            'request_config',
            'column_config',
            'status',
        )

        extra_kwargs = {
            'name': {'required': True},
            'request_config': {'required': True},
            'column_config': {'required': True},
        }

    def validate_status(self, status):
        if status in [accounts_models.DataSource.DATA_SOURCE_STATUS_DRAFT,
                      accounts_models.DataSource.DATA_SOURCE_STATUS_READY_TO_PUBLISH]:
            return status
        raise accounts_exceptions.InvalidDataSourceStatusError()

    def create(self, validated_data):
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            # set created_by and last_modified_by value
            if request_user is not None:
                validated_data['created_by'] = validated_data.get('created_by') or request_user
                validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user

            instance = super().create(validated_data)
        return instance


class UpdateDataSourceSerializer(BaseDataSourceCRUDSerializer):
    class Meta:
        model = accounts_models.DataSource
        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
            'published_by',
            'published_at',
        )

        fields = read_only_fields + (
            'name',
            'description',
            'version',
            'data_source_type',
            'request_config',
            'column_config',
            'status',
        )

        extra_kwargs = {
            'status': {'required': True},
        }

    def validate_name(self, name):
        if re.match(account_config.valid_name_pattern, name):
            if name != self.instance.name and accounts_models.DataSource.objects.filter(name=name).exists():
                raise accounts_exceptions.DuplicateDataSourceNameError(message_kwargs={'name': name})
            return name
        raise accounts_exceptions.DuplicateDataSourceNameError(message_kwargs={'name': name})

    def validate_status(self, status):
        if (
            (
                status in [accounts_models.DataSource.DATA_SOURCE_STATUS_DRAFT,
                           accounts_models.DataSource.DATA_SOURCE_STATUS_READY_TO_PUBLISH] and
                self.instance.status in [accounts_models.DataSource.DATA_SOURCE_STATUS_DRAFT,
                                         accounts_models.DataSource.DATA_SOURCE_STATUS_READY_TO_PUBLISH]
            ) or
            (
                status == accounts_models.DataSource.DATA_SOURCE_STATUS_DISABLED and
                self.instance.status == accounts_models.DataSource.DATA_SOURCE_STATUS_PUBLISHED)
        ):
            return status
        raise accounts_exceptions.InvalidDataSourceStatusUpdateError()


class PublishDataSourceSerializer(BaseDataSourceCRUDSerializer):
    class Meta:
        model = accounts_models.DataSource
        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
            'name',
            'description',
            'version',
            'data_source_type',
            'request_config',
            'column_config',
        )

        fields = read_only_fields + (
            'published_by',
            'published_at',
            'status',
        )

        extra_kwargs = {
            'status': {'required': True},
        }

    def validate_status(self, status):
        if (
            status == accounts_models.DataSource.DATA_SOURCE_STATUS_PUBLISHED and
            self.instance.status == accounts_models.DataSource.DATA_SOURCE_STATUS_READY_TO_PUBLISH
        ):
            return status

        raise accounts_exceptions.InvalidDataSourceStatusUpdateError()

    def update(self, instance, validated_data):
        """
        update
            This method is used to update Data Source
        :param instance:
        :param validated_data:
        :return:
        """
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            if request_user is not None:
                validated_data['published_by'] = request_user
                validated_data['published_at'] = core_utilities.get_current_datetime_object()

            instance = super().update(instance, validated_data)
        return instance


class EntityDataLayersListSerializer(FlexFieldsModelSerializer):
    applicable_countries = serializers.JSONField()
    global_benchmark = serializers.JSONField()
    legend_configs = serializers.JSONField()

    data_sources_list = serializers.JSONField()
    data_source_column = serializers.JSONField()
    # data_source_column_function = serializers.JSONField()

    benchmark_metadata = serializers.SerializerMethodField()

    active_countries_list = serializers.JSONField()
    entity_type__code = serializers.CharField(source='entity_type.code')

    class Meta:
        model = accounts_models.DataLayer
        read_only_fields = fields = (
            'id',
            'icon',
            'code',
            'name',
            'description',
            'version',
            'type',
            'category',
            'entity_type',
            'entity_type__code',
            'applicable_countries',
            'global_benchmark',
            'legend_configs',
            'status',
            'published_by',
            # 'published_at',
            # 'created',
            'last_modified_at',
            'is_reverse',
            'data_sources_list',
            'data_source_column',
            # 'data_source_column_function',
            'benchmark_metadata',
            'active_countries_list',
        )

        expandable_fields = {
            # 'data_source': (ExpandAPISerializer, {'source': 'data_source'}),
            'published_by': (ExpandUserSerializer, {'source': 'published_by'}),
            'last_modified_by': (ExpandUserSerializer, {'source': 'last_modified_by'}),
            'created_by': (ExpandUserSerializer, {'source': 'created_by'}),
        }

    def get_benchmark_metadata(self, instance):
        parameter_col = instance.data_sources.all().first().data_source_column
        parameter_column_unit = str(parameter_col.get('unit', '')).lower()
        display_unit = parameter_col.get('display_unit', '')

        benchmark_metadata = {
            'parameter_column_unit': parameter_column_unit,
            'display_unit': display_unit,
            'benchmark_name': instance.global_benchmark.get('benchmark_name', 'Global'),
            'benchmark_type': instance.global_benchmark.get('benchmark_type', 'global'),
        }

        if instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            convert_unit = instance.global_benchmark.get('convert_unit')

            unit_agg_str = '{val}'
            if convert_unit == 'mbps' and parameter_column_unit == 'bps':
                unit_agg_str = '{val} / (1000 * 1000)'
            elif convert_unit == 'mbps' and parameter_column_unit == 'kbps':
                unit_agg_str = '{val} / 1000'
            elif convert_unit == 'kbps' and parameter_column_unit == 'bps':
                unit_agg_str = '{val} / 1000'
            elif convert_unit == 'kbps' and parameter_column_unit == 'mbps':
                unit_agg_str = '{val} * 1000'
            elif convert_unit == 'bps' and parameter_column_unit == 'kbps':
                unit_agg_str = '{val} * 1000'
            elif convert_unit == 'bps' and parameter_column_unit == 'mbps':
                unit_agg_str = '{val} * 1000 * 1000'

            benchmark_metadata.update({
                'benchmark_value': instance.global_benchmark.get('value'),
                'benchmark_unit': instance.global_benchmark.get('unit'),
                'base_benchmark': str(parameter_col.get('base_benchmark', 1)),
                'round_unit_value': unit_agg_str,
            })

        return benchmark_metadata

    def to_representation(self, data_layer):
        data_sources_list = []
        active_countries_list = []

        data_source_columns = {}
        # data_source_column_functions = {}

        linked_data_sources = data_layer.data_sources.all()
        for relationship_instance in linked_data_sources:
            data_source_serializer = ExpandDataSourceSerializer(instance=relationship_instance.data_source)
            data_sources_list.append(data_source_serializer.data)
            data_source_columns[relationship_instance.data_source.id] = relationship_instance.data_source_column
            # if relationship_instance.data_source_column_function:
            #     data_source_column_functions[relationship_instance.data_source.id] = relationship_instance.data_source_column_function

        linked_countries = data_layer.active_countries.all()
        for relationship_instance in linked_countries:
            active_countries_list.append({
                'country': relationship_instance.country_id,
                'is_default': relationship_instance.is_default,
                'data_sources': relationship_instance.data_sources,
            })

        setattr(data_layer, 'data_sources_list', data_sources_list)
        setattr(data_layer, 'data_source_column', data_source_columns)
        # setattr(data_layer, 'data_source_column_function', list(data_source_column_functions.values())[-1] if data_source_column_functions else {})
        setattr(data_layer, 'active_countries_list', active_countries_list)
        return super().to_representation(data_layer)


class DataLayerDataSourceRelationshipSerializer(serializers.ModelSerializer):
    data_source_column = serializers.JSONField()
    # data_source_column_function = serializers.JSONField(required=False)

    class Meta:
        model = accounts_models.DataLayerDataSourceRelationship

        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
        )

        fields = read_only_fields + (
            'data_layer',
            'data_source',
            'data_source_column',
            # 'data_source_column_function',
        )

        extra_kwargs = {
            'data_layer': {'required': True},
            'data_source': {'required': True},
            'data_source_column': {'required': True},
        }

    def create(self, validated_data):
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            # set created_by and last_modified_by value
            if request_user is not None:
                validated_data['created_by'] = validated_data.get('created_by') or request_user
                validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user

            instance = super().create(validated_data)
        return instance


class CreateEntityDataLayersSerializer(BaseDataLayerCRUDSerializer):
    applicable_countries = serializers.JSONField(required=False)
    global_benchmark = serializers.JSONField(required=False)
    legend_configs = serializers.JSONField(required=False)

    data_sources_list = serializers.JSONField()
    data_source_column = serializers.JSONField()
    # data_source_column_function = serializers.JSONField()

    class Meta:
        model = accounts_models.DataLayer

        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
        )

        fields = read_only_fields + (
            'icon',
            'code',
            'name',
            'description',
            'version',
            'type',
            'category',
            'entity_type',
            'applicable_countries',
            'global_benchmark',
            'legend_configs',
            'status',
            'is_reverse',
            'data_sources_list',
            'data_source_column',
            # 'data_source_column_function',
        )

        extra_kwargs = {
            'icon': {'required': True},
            # 'code': {'required': True},
            'name': {'required': True},
            'type': {'required': True},
            'data_sources_list': {'required': True},
            'data_source_column': {'required': True},
            'entity_type': {'required': True},
        }

    def validate_status(self, status):
        if status in [accounts_models.DataLayer.LAYER_STATUS_DRAFT,
                      accounts_models.DataLayer.LAYER_STATUS_READY_TO_PUBLISH]:
            return status
        raise accounts_exceptions.InvalidDataLayerStatusError()

    def to_internal_value(self, data):
        if not data.get('code') and data.get('name'):
            data['code'] = core_utilities.normalize_str(str(data.get('name'))).upper()
        elif data.get('code'):
            data['code'] = str(data.get('code')).upper()
        return super().to_internal_value(data)

    def create(self, validated_data):
        """
        create
            This method is used to create Data Layer
        :param validated_data:
        :return:
        """
        data_sources_list = validated_data.pop('data_sources_list', [])
        data_source_column = validated_data.pop('data_source_column', None)
        # data_source_column_function = validated_data.pop('data_source_column_function', {})

        request_user = core_utilities.get_current_user(context=self.context)
        # set created_by and last_modified_by value
        if request_user is not None:
            validated_data['created_by'] = validated_data.get('created_by') or request_user
            validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user
        data_layer_instance = super().create(validated_data)

        for data_source in data_sources_list:
            data_layer_source_data = {
                'data_source': data_source.id,
                'data_layer': data_layer_instance.id,
                'data_source_column': data_source_column,
                # 'data_source_column_function': data_source_column_function,
            }

            data_layer_source_relationships = DataLayerDataSourceRelationshipSerializer(
                data=data_layer_source_data,
                context=self.context,
            )
            data_layer_source_relationships.is_valid(raise_exception=True)
            data_layer_source_relationships.save()

        # Once Data Layer is created, send the status email to the PUBLISHERS
        publishers = get_user_emails_for_permissions([auth_models.RolePermission.CAN_PUBLISH_DATA_LAYER])

        if settings.ENABLED_DATA_LAYER_EMAILS and request_user is not None and len(publishers) > 0:
            email_subject = account_config.data_layer_creation_email_subject_format % (
                core_utilities.get_project_title(), data_layer_instance.name,
            )
            email_message = account_config.data_layer_creation_email_message_format
            email_content = {'subject': email_subject, 'message': email_message}
            account_utilities.send_email_over_mailjet_service(publishers, cc=[request_user.email, ],
                                                              **email_content)

        return data_layer_instance


class UpdateEntityDataLayerSerializer(BaseDataLayerCRUDSerializer):
    data_sources_list = serializers.JSONField()
    data_source_column = serializers.JSONField()
    # data_source_column_function = serializers.JSONField(required=False)

    applicable_countries = serializers.JSONField(required=False)
    global_benchmark = serializers.JSONField(required=False)
    legend_configs = serializers.JSONField(required=False)

    class Meta:
        model = accounts_models.DataLayer
        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
            'published_by',
            'published_at',
            'code',
        )

        fields = read_only_fields + (
            'icon',
            'name',
            'description',
            'version',
            'type',
            'category',
            'entity_type',
            'applicable_countries',
            'global_benchmark',
            'legend_configs',
            'status',
            'is_reverse',
            'data_sources_list',
            'data_source_column',
            # 'data_source_column_function',
        )

        extra_kwargs = {
            'status': {'required': True},
        }

    def validate_status(self, status):
        if status in [accounts_models.DataLayer.LAYER_STATUS_DRAFT,
                      accounts_models.DataLayer.LAYER_STATUS_READY_TO_PUBLISH]:
            if self.instance.status in [accounts_models.DataLayer.LAYER_STATUS_DRAFT,
                                        accounts_models.DataLayer.LAYER_STATUS_READY_TO_PUBLISH]:
                return status
        if self.instance.status in [accounts_models.DataLayer.LAYER_STATUS_DISABLED,
                                    accounts_models.DataLayer.LAYER_STATUS_PUBLISHED]:
            request_user = core_utilities.get_current_user(context=self.context)
            user_is_publisher = len(get_user_emails_for_permissions(
                [auth_models.RolePermission.CAN_PUBLISH_DATA_LAYER],
                ids_to_filter=[request_user.id]
            )) > 0

            if user_is_publisher:
                return self.instance.status

        raise accounts_exceptions.InvalidDataLayerStatusUpdateError()

    def _validate_user(self, instance):
        """
        _validate_user
        :param instance:
        :return:
        """
        request_user = core_utilities.get_current_user(context=self.context)

        # Check user is same who created the data layer or not
        # if: yes then return true
        # 6. Editor only able to edit the layer created by him/her. Editor can only view the layer created by others.
        # else: check
        # 3. If necessary, the Publisher can edit the details and then approve the changes to the data layer.

        if (
            request_user == instance.created_by or
            len(get_user_emails_for_permissions([auth_models.RolePermission.CAN_PUBLISH_DATA_LAYER],
                                                ids_to_filter=[request_user.id])) > 0
        ):
            return True
        raise accounts_exceptions.InvalidUserOnDataLayerUpdateError()

    def update(self, instance, validated_data):
        """
        update
            This method is used to update Data Layer
        :param instance:
        :param validated_data:
        :return:
        """
        self._validate_user(instance)

        data_sources_list = validated_data.pop('data_sources_list', None)
        data_source_column = validated_data.pop('data_source_column', None)
        # data_source_column_function = validated_data.pop('data_source_column_function', {})

        with transaction.atomic():
            if data_sources_list is not None:
                instance.data_sources.all().update(deleted=core_utilities.get_current_datetime_object())

                for data_source in data_sources_list:
                    data_layer_source_data = {
                        'data_source': data_source.id,
                        'data_layer': instance.id,
                        'data_source_column': data_source_column,
                        # 'data_source_column_function': data_source_column_function,
                    }

                    data_layer_source_relationships = DataLayerDataSourceRelationshipSerializer(
                        data=data_layer_source_data,
                        context=self.context,
                    )
                    data_layer_source_relationships.is_valid(raise_exception=True)
                    data_layer_source_relationships.save()

            data_layer_instance = super().update(instance, validated_data)

            if data_layer_instance.status == accounts_models.DataLayer.LAYER_STATUS_PUBLISHED:
                args = ['--reset', '-layer_id={0}'.format(instance.id)]
                call_command('populate_active_data_layer_for_countries', *args)

            # Once Data Layer is created, send the status email to the PUBLISHERS
            request_user = core_utilities.get_current_user(context=self.context)

            publishers = get_user_emails_for_permissions([auth_models.RolePermission.CAN_PUBLISH_DATA_LAYER])

            if (
                settings.ENABLED_DATA_LAYER_EMAILS and
                request_user is not None and
                len(publishers) > 0 and
                request_user.email not in publishers
            ):
                if validated_data.get('status', None) == accounts_models.DataLayer.LAYER_STATUS_READY_TO_PUBLISH:
                    email_subject = account_config.data_layer_update_ready_to_publish_email_subject_format % (
                        core_utilities.get_project_title(), data_layer_instance.name,
                    )
                    email_message = account_config.data_layer_update_ready_to_publish_email_message_format
                    email_message = email_message.format(
                        dashboard_url='Dashboard url: {}'.format(settings.DATA_LAYER_DASHBOARD_URL)
                        if settings.DATA_LAYER_DASHBOARD_URL else '')
                else:
                    email_subject = account_config.data_layer_update_email_subject_format % (
                        core_utilities.get_project_title(), data_layer_instance.name,
                    )
                    email_message = account_config.data_layer_update_email_message_format

                email_content = {'subject': email_subject, 'message': email_message}
                account_utilities.send_email_over_mailjet_service(publishers, cc=[request_user.email, ],
                                                                  **email_content)

            return data_layer_instance


class PublishEntityDataLayerSerializer(BaseDataLayerCRUDSerializer):
    class Meta:
        model = accounts_models.DataLayer
        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
            'icon',
            'code',
            'name',
            'entity_type',
            'description',
            'version',
            'type',
            'category',
            'is_reverse',
        )

        fields = read_only_fields + (
            'published_by',
            'published_at',
            'status',
        )

        extra_kwargs = {
            'status': {'required': True},
        }

    def validate_status(self, status):
        if (
            status == accounts_models.DataLayer.LAYER_STATUS_PUBLISHED and
            self.instance.status in [accounts_models.DataLayer.LAYER_STATUS_READY_TO_PUBLISH,
                                     accounts_models.DataLayer.LAYER_STATUS_DISABLED]
        ) or (
            status == accounts_models.DataLayer.LAYER_STATUS_DISABLED and
            self.instance.status == accounts_models.DataLayer.LAYER_STATUS_PUBLISHED
        ):
            return status
        raise accounts_exceptions.InvalidDataLayerStatusUpdateError()

    def update(self, instance, validated_data):
        """
        update
            This method is used to update Data Layer
        :param instance:
        :param validated_data:
        :return:
        """
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            if (
                request_user is not None and
                validated_data['status'] == accounts_models.DataLayer.LAYER_STATUS_PUBLISHED
            ):
                validated_data['published_by'] = request_user
                validated_data['published_at'] = core_utilities.get_current_datetime_object()

            instance = super().update(instance, validated_data)

            args = ['--reset', '-layer_id={0}'.format(instance.id)]
            call_command('populate_active_data_layer_for_countries', *args)

        return instance


class LogActionSerializer(serializers.ModelSerializer):
    object_data = serializers.SerializerMethodField()
    content_type = serializers.SerializerMethodField()
    section_type = serializers.SerializerMethodField()
    action_flag = serializers.SerializerMethodField()

    username = serializers.SerializerMethodField()

    class Meta:
        model = LogEntry
        fields = (
            'id',
            'object_data',
            'content_type',
            'section_type',
            'action_flag',
            'action_time',
            'object_id',
            'object_repr',
            'change_message',
            'username',
        )

    def make_url(self, url):
        url = url.split('/accounts/recent_action_log')
        url = url[0] + url[1]
        url = url.replace('connection_statistics', 'statistics')
        return url

    def get_object_data(self, instance):
        request = self.context.get('request')
        if instance.content_type and instance.object_id:
            url_name = "%s/%s/%s" % (
                instance.content_type.app_label,
                instance.content_type.model,
                instance.object_id,
            )
            return self.make_url(request.build_absolute_uri(url_name))

    def get_section_type(self, instance):
        if instance.content_type:
            return apps.get_model(instance.content_type.app_label,
                                  instance.content_type.model)._meta.verbose_name.title()

    def get_content_type(self, instance):
        if instance.content_type:
            return instance.content_type.app_label

    def get_action_flag(self, instance):
        if instance.action_flag == 1:
            return 'Added'
        elif instance.action_flag == 2:
            return 'Changed'
        return 'Deletion'

    def get_username(self, instance):
        user_name = None
        if instance.user:
            user_name = instance.user.first_name
            if not core_utilities.is_blank_string(instance.user.last_name):
                user_name += ' ' + instance.user.last_name
        return user_name


class DataLayerCountryRelationshipSerializer(serializers.ModelSerializer):
    data_sources = serializers.JSONField()
    legend_configs = serializers.JSONField()

    class Meta:
        model = accounts_models.DataLayerCountryRelationship

        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
        )

        fields = read_only_fields + (
            'data_layer',
            'country',
            'is_default',
            'data_sources',
            'legend_configs',
            'is_applicable',
        )

        extra_kwargs = {
            'data_layer': {'required': True},
            'country': {'required': True},
            'is_default': {'required': True},
            'data_sources': {'required': True},
            'legend_configs': {'required': True},
        }

    def create(self, validated_data):
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            # set created_by and last_modified_by value
            if request_user is not None:
                validated_data['created_by'] = validated_data.get('created_by') or request_user
                validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user

            instance = super().create(validated_data)
        return instance







