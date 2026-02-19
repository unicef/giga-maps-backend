import re
from math import floor, ceil

from django.conf import settings
from django.core.management import call_command
from django.db import connection
from django.db.models import F, Min, Max
from rest_flex_fields.serializers import FlexFieldsModelSerializer
from rest_framework import serializers

from proco.accounts import exceptions as accounts_exceptions
from proco.accounts import models as accounts_models
from proco.accounts.config import app_config as account_config
from proco.connection_statistics.models import EntityWeeklyStatus
from proco.core import db_utils as db_utilities
from proco.core import utils as core_utilities
from proco.custom_auth import models as auth_models
from proco.custom_auth.serializers import ExpandUserSerializer
from proco.custom_auth.utils import get_user_emails_for_permissions
from proco.entities.models import Entity


class ExpandColumnConfigurationSerializer(FlexFieldsModelSerializer):
    options = serializers.SerializerMethodField()

    class Meta:
        model = accounts_models.ColumnConfiguration
        read_only_fields = fields = (
            'name',
            'label',
            'type',
            'table_name',
            'table_alias',
            'table_label',
            'options',
        )

    def get_options(self, instance):
        options = instance.options
        if isinstance(options, dict) and 'active_countries_filter' in options:
            del options['active_countries_filter']
        return options


class PublishedEntityAdvanceFiltersListSerializer(FlexFieldsModelSerializer):
    options = serializers.SerializerMethodField()

    class Meta:
        model = accounts_models.AdvanceFilter
        read_only_fields = fields = (
            'name',
            'type',
            'description',
            'column_configuration',
            'options',
            'query_param_filter'
        )

        expandable_fields = {
            'column_configuration': (ExpandColumnConfigurationSerializer, {'source': 'column_configuration'}),
        }

    def include_none_filter(self, parameter_table, parameter_field):
        select_qs = Entity.objects.filter(country_id=self.context['country_id'])
        none_check_sql = f'"entities_entity"."{parameter_field}" IS NULL'
        if parameter_table == 'entity_static':
            last_weekly_status_field = 'last_weekly_status__{}'.format(parameter_field)
            select_qs = select_qs.select_related('last_weekly_status').annotate(**{
                parameter_table + '_' + parameter_field: F(last_weekly_status_field)
            })

            none_check_sql = f'"connection_statistics_entityweeklystatus"."{parameter_field}" IS NULL'
        return select_qs.extra(where=[none_check_sql]).exists()

    def update_range_filter_options(self, options, parameter_table, parameter_field, parameter_options):
        last_weekly_status_field = 'last_weekly_status__{}'.format(parameter_field)

        options['include_none_filter'] = self.include_none_filter(parameter_table, parameter_field)

        if options.get('range_auto_compute', False):
            select_qs = Entity.objects.filter(country_id=self.context['country_id'])
            if parameter_table == 'entity_static':
                parameter_field_props = EntityWeeklyStatus._meta.get_field(parameter_field)

                select_qs = select_qs.select_related('last_weekly_status').values('country_id').annotate(
                    min_value=Min(F(last_weekly_status_field)),
                    max_value=Max(F(last_weekly_status_field)),
                )
            else:
                parameter_field_props = Entity._meta.get_field(parameter_field)

                select_qs = select_qs.values('country_id').annotate(
                    min_value=Min(parameter_field),
                    max_value=Max(parameter_field),
                )

            country_range_json = list(
                select_qs.values('country_id', 'min_value', 'max_value').order_by('country_id').distinct())[-1]

            if country_range_json and country_range_json['min_value'] is not None and country_range_json['max_value'] is not None:
                del country_range_json['country_id']

                country_range_json['min_value'] = floor(country_range_json['min_value'])
                country_range_json['max_value'] = ceil(country_range_json['max_value'])

                if 'downcast_aggr_str' in parameter_options:
                    downcast_eval = parameter_options['downcast_aggr_str']
                    country_range_json['min_value'] = floor(
                        eval(downcast_eval.format(val=country_range_json['min_value'])))
                    country_range_json['max_value'] = ceil(
                        eval(downcast_eval.format(val=country_range_json['max_value'])))

                country_range_json['min_place_holder'] = 'Min ({})'.format(country_range_json['min_value'])
                country_range_json['max_place_holder'] = 'Max ({})'.format(country_range_json['max_value'])
            else:
                internal_type = parameter_field_props.get_internal_type()
                min_value, max_value = connection.ops.integer_field_range(internal_type)
                country_range_json = {
                    'min_place_holder': 'Min',
                    'max_place_holder': 'Max',
                    'min_value': min_value,
                    'max_value': max_value
                }

            options['active_range'] = country_range_json

    def update_boolean_filter_options(self, options, parameter_table, parameter_field):
        join_condition = ''
        filter_condition = ''

        select_qry = """
        SELECT DISTINCT {col} AS {col_name}
        FROM entities_entity AS entities
        {join_condition}
        WHERE entities.deleted IS NULL
            AND entities.country_id = {c_id}
            {filter_condition}
        ORDER BY {col_name} DESC NULLS LAST
        """

        if parameter_table == 'entity_static':
            join_condition = ('INNER JOIN connection_statistics_entityweeklystatus AS entity_static '
                              'ON entities.last_weekly_status_id = entity_static.id')
            filter_condition = 'AND entity_static.deleted IS NULL'

        sql_qry = select_qry.format(
            col_name=parameter_field,
            col=parameter_table + '.' + parameter_field,
            c_id=self.context['country_id'],
            join_condition=join_condition,
            filter_condition=filter_condition)
        choices = []
        data = db_utilities.sql_to_response(sql_qry, label=self.__class__.__name__)
        for value in data:
            field_value = value[parameter_field]

            if core_utilities.is_blank_string(field_value):
                choices.append({
                    'label': 'Unknown',
                    'value': 'none'
                })
            else:
                choices.append({
                    'label': 'Yes' if field_value else 'No',
                    'value': 'true' if field_value else 'false',
                })
        options['choices'] = choices

    def get_options(self, instance):
        options = instance.options
        if isinstance(options, dict):
            parameter_details = instance.column_configuration
            parameter_field = parameter_details.name
            field_type = parameter_details.type
            parameter_table = parameter_details.table_alias

            parameter_options = parameter_details.options

            if options.get('live_choices', False):
                join_condition = ''
                filter_condition = ''

                select_qry = """
                SELECT DISTINCT {col} AS {col_name}
                FROM entities_entity AS entities
                {join_condition}
                WHERE entities.deleted IS NULL
                    AND entities.country_id = {c_id}
                    {filter_condition}
                ORDER BY {col_name} ASC NULLS LAST
                """

                if parameter_table == 'entity_static':
                    join_condition = ('INNER JOIN connection_statistics_entityweeklystatus AS entity_static '
                                      'ON entities.last_weekly_status_id = entity_static.id')
                    filter_condition = 'AND entity_static.deleted IS NULL'

                sql_qry = select_qry.format(
                    col_name=parameter_field,
                    col=f"LOWER(NULLIF({parameter_table + '.' + parameter_field}, ''))" if field_type == 'str' else parameter_table + '.' + parameter_field,
                    c_id=self.context['country_id'],
                    join_condition=join_condition,
                    filter_condition=filter_condition)
                choices = []
                data = db_utilities.sql_to_response(sql_qry, label=self.__class__.__name__)
                for value in data:
                    field_value = value[parameter_field]
                    if core_utilities.is_blank_string(field_value):
                        choices.append({
                            'label': 'Unknown',
                            'value': 'none'
                        })
                    else:
                        choices.append({
                            'label': field_value.title()
                            if field_type == accounts_models.ColumnConfiguration.TYPE_STR else field_value,
                            'value': field_value
                        })
                options['choices'] = choices

            if instance.type == accounts_models.AdvanceFilter.TYPE_RANGE:
                self.update_range_filter_options(options, parameter_table, parameter_field, parameter_options)
            elif instance.type == accounts_models.AdvanceFilter.TYPE_BOOLEAN:
                self.update_boolean_filter_options(options, parameter_table, parameter_field)

        return options


class EntityAdvanceFiltersListSerializer(FlexFieldsModelSerializer):
    active_countries_list = serializers.JSONField()
    options = serializers.JSONField()
    entity_type_code = serializers.CharField(source='entity_type.code')

    class Meta:
        model = accounts_models.AdvanceFilter
        read_only_fields = fields = (
            'id',
            'code',
            'name',
            'description',
            'type',
            'options',
            'query_param_filter',
            'column_configuration',
            'status',
            'published_by',
            'active_countries_list',
            'entity_type',
            'entity_type_code',
        )

        expandable_fields = {
            'column_configuration': (ExpandColumnConfigurationSerializer, {'source': 'column_configuration'}),
            'published_by': (ExpandUserSerializer, {'source': 'published_by'}),
            'last_modified_by': (ExpandUserSerializer, {'source': 'last_modified_by'}),
            'created_by': (ExpandUserSerializer, {'source': 'created_by'}),
        }

    def to_representation(self, instance):
        active_countries_list = list(instance.active_countries.all().order_by(
            'country_id').values_list('country_id', flat=True).distinct('country_id'))
        setattr(instance, 'active_countries_list', active_countries_list)
        return super().to_representation(instance)


class BaseEntityAdvanceFilterListCRUDSerializer(serializers.ModelSerializer):
    def validate_name(self, name):
        if re.match(account_config.valid_filter_name_pattern, name):
            return name
        raise accounts_exceptions.InvalidAdvanceFilterNameError()

    def validate_code(self, code):
        if re.match(r'[a-zA-Z0-9-\' _]*$', code):
            if (
                (self.instance and code != self.instance.code) or
                (not self.instance and accounts_models.AdvanceFilter.objects.filter(code__iexact=code).exists())
            ):
                raise accounts_exceptions.DuplicateAdvanceFilterCodeError(message_kwargs={'code': code.upper()})
            return code.upper()
        raise accounts_exceptions.InvalidAdvanceFilterCodeError()


class CreateEntityAdvanceFilterSerializer(BaseEntityAdvanceFilterListCRUDSerializer):
    options = serializers.JSONField(required=False)

    class Meta:
        model = accounts_models.AdvanceFilter

        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
        )

        fields = read_only_fields + (
            'code',
            'name',
            'description',
            'type',
            'status',
            'options',
            'query_param_filter',
            'column_configuration',
            'entity_type',
        )

        extra_kwargs = {
            'name': {'required': True},
            'type': {'required': True},
            'column_configuration': {'required': True},
            'entity_type': {'required': True},
        }

    def validate_status(self, status):
        return accounts_models.AdvanceFilter.FILTER_STATUS_DRAFT

    def to_internal_value(self, data):
        if not data.get('code') and data.get('name'):
            data['code'] = core_utilities.normalize_str(str(data.get('name'))).upper()
        return super().to_internal_value(data)

    def create(self, validated_data):
        request_user = core_utilities.get_current_user(context=self.context)

        if request_user is not None:
            validated_data['created_by'] = validated_data.get('created_by') or request_user
            validated_data['last_modified_by'] = validated_data.get('last_modified_by') or request_user

        return super().create(validated_data)


class UpdateEntityAdvanceFilterSerializer(BaseEntityAdvanceFilterListCRUDSerializer):
    options = serializers.JSONField(required=False)

    class Meta:
        model = accounts_models.AdvanceFilter
        read_only_fields = (
            'id',
            'last_modified_at',
            'published_by',
            'published_at'
        )

        fields = read_only_fields + (
            'code',
            'name',
            'description',
            'type',
            'status',
            'column_configuration',
            'options',
            'query_param_filter',
            'entity_type',
        )

        extra_kwargs = {
            'status': {'required': True},
        }

    def validate_status(self, status):
        if (
            status == accounts_models.AdvanceFilter.FILTER_STATUS_DRAFT and
            self.instance.status == accounts_models.AdvanceFilter.FILTER_STATUS_DRAFT
        ) or (
            status == accounts_models.AdvanceFilter.FILTER_STATUS_DISABLED and
            self.instance.status == accounts_models.AdvanceFilter.FILTER_STATUS_PUBLISHED
        ):
            return status

        if self.instance.status in [accounts_models.AdvanceFilter.FILTER_STATUS_DISABLED,
                                    accounts_models.AdvanceFilter.FILTER_STATUS_PUBLISHED]:
            request_user = core_utilities.get_current_user(context=self.context)
            user_is_publisher = len(get_user_emails_for_permissions(
                [auth_models.RolePermission.CAN_PUBLISH_ADVANCE_FILTER],
                ids_to_filter=[request_user.id]
            )) > 0

            if user_is_publisher:
                return self.instance.status

        raise accounts_exceptions.InvalidAdvanceFilterUpdateError()

    def validate_code(self, code):
        if re.match(r'[a-zA-Z0-9-\' _]*$', code):
            if accounts_models.AdvanceFilter.objects.filter(code__iexact=code).exclude(pk=self.instance.id).exists():
                raise accounts_exceptions.DuplicateAdvanceFilterCodeError(message_kwargs={'code': code.upper()})
            return code.upper()
        raise accounts_exceptions.InvalidAdvanceFilterCodeError()


class PublishEntityAdvanceFilterSerializer(serializers.ModelSerializer):
    options = serializers.JSONField(required=False)
    entity_type_code = serializers.CharField(source='entity_type.code', read_only=True)

    class Meta:
        model = accounts_models.AdvanceFilter
        read_only_fields = (
            'id',
            'created',
            'last_modified_at',
            'code',
            'name',
            'description',
            'type',
            'column_configuration',
            'options',
            'query_param_filter',
            'published_by',
            'published_at',
            'entity_type',
            'entity_type_code',
        )

        fields = read_only_fields + (
            'status',
        )

    def update(self, instance, validated_data):
        validated_data['status'] = accounts_models.AdvanceFilter.FILTER_STATUS_PUBLISHED
        validated_data['published_at'] = core_utilities.get_current_datetime_object()
        validated_data['published_by'] = core_utilities.get_current_user(context=self.context)

        instance = super().update(instance, validated_data)

        args = ['--reset', '-filter_id={0}'.format(instance.id)]
        call_command('populate_active_filters_for_countries', *args)

        return instance


class EntityColumnConfigurationChoicesSerializer(FlexFieldsModelSerializer):
    values = serializers.SerializerMethodField()

    class Meta:
        model = accounts_models.ColumnConfiguration
        read_only_fields = fields = (
            'name',
            'label',
            'type',
            'description',
            'values',
        )

    def get_values(self, instance):
        choices = []

        parameter_field = instance.name
        field_type = instance.type
        parameter_table = instance.table_alias

        join_condition = ''
        filter_condition = ''

        select_qry = """
        SELECT DISTINCT {col} AS {col_name}
        FROM entities_entity AS entities
        {join_condition}
        WHERE entities.deleted IS NULL
            {filter_condition}
        ORDER BY {col_name} ASC NULLS LAST
        """

        if parameter_table == 'entity_static':
            join_condition = ('INNER JOIN connection_statistics_entityweeklystatus AS entity_static '
                              'ON entities.last_weekly_status_id = entity_static.id')
            filter_condition = 'AND entity_static.deleted IS NULL'

        sql_qry = select_qry.format(
            col_name=parameter_field,
            col=f"LOWER(NULLIF({parameter_table + '.' + parameter_field}, ''))"
            if field_type == 'str' else parameter_table + '.' + parameter_field,
            join_condition=join_condition,
            filter_condition=filter_condition)

        data = db_utilities.sql_to_response(sql_qry, label=self.__class__.__name__)
        for value in data:
            field_value = value[parameter_field]
            if core_utilities.is_blank_string(field_value):
                choices.append({
                    'label': 'Unknown',
                    'value': 'none'
                })
            else:
                choices.append({
                    'label': field_value.title()
                    if field_type == accounts_models.ColumnConfiguration.TYPE_STR else field_value,
                    'value': field_value
                })

        return choices


class EntityColumnConfigurationListSerializer(FlexFieldsModelSerializer):
    options = serializers.JSONField()
    entity_type_code = serializers.CharField(source='entity_type.code')

    class Meta:
        model = accounts_models.ColumnConfiguration
        read_only_fields = fields = (
            'id',
            'name',
            'label',
            'type',
            'description',
            'table_name',
            'table_alias',
            'table_label',
            'is_filter_applicable',
            'options',
            'entity_type',
            'entity_type_code',
        )
