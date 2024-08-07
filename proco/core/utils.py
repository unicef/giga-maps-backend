import gc
import locale
import logging
import re
import secrets
from decimal import Decimal

import pytz
from django.conf import settings
from django.utils import timezone

from proco.core.config import app_config as core_config

logger = logging.getLogger('gigamaps.' + __name__)


def get_timezone_converted_value(value, tz=settings.TIME_ZONE):
    """
    get_timezone_converted_value
        Method to convert the timezone of the datetime field value
    :param tz: timezone
    :param value: DateTime instance
    :return:
    """
    response_timezone = pytz.timezone(tz)
    return value.astimezone(response_timezone)


def get_current_datetime_object(timestamp=None):
    """
    get_current_datetime_object
        Method to get current datetime object with timezone
    :return:
    """
    if timestamp:
        return get_timezone_converted_value(timestamp)
    return get_timezone_converted_value(timezone.now())


def is_superuser(user):
    """
    is_superuser
        is_superuser to check if the logged-in user is a superuser or not.
    :param: user : Logged-in user object
    :return: True if user is superuser else False
    """
    if user.is_staff or user.is_superuser:
        return True

    from proco.custom_auth.models import Role
    user_role = user.get_roles()
    if user_role is not None and user_role.name in (Role.SYSTEM_ROLE_NAME_ADMIN,):
        return True
    return False


def get_current_user(context=None, request=None, user=None):
    """
    get_current_user
        Returns the current user of requested action
    :param context:
    :param request:
    :param user:
    :return:
    """
    if context is not None:
        request = context.get('request')
        user = context.get('user', None)
    request_user = (request and getattr(request, 'user')) or user
    return request_user


def to_boolean(data):
    """
    to_boolean
        convert string or int to boolean
    :param data: data
    :return: boolean
    """
    if isinstance(data, bool):
        return data
    elif isinstance(data, (str, int)):
        data = str(data).lower()
        if data in ('false', '0'):
            return False
    return True


def is_blank_string(val):
    """Check if the given string is empty."""
    if val is None:
        return True
    elif isinstance(val, str):
        attr = val.strip().lower()
        return len(attr) == 0
    return False


def sanitize_str(val):
    """Remove the head/tailing spaces from string."""
    if isinstance(val, str):
        return val.strip()
    return val


def normalize_str(val):
    """Remove the extra chars from string."""
    if not isinstance(val, str):
        return val
    wh = '\t\n\r\v\f'
    punctuation = r"""!"#$%&'()*+,./:;<=>?@[\]^`{|}~_-""" + wh
    return re.sub(r'[' + re.escape(punctuation) + ']', '', val)


def get_random_string(length=264, allowed_chars='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._*#'):
    """
    Return a securely generated random string.
    """
    return ''.join(secrets.choice(allowed_chars) for _ in range(length))


def format_decimal_data(value):
    """
    format_decimal_data
        Format data if it is a decimal value
    :param value:
    :return:
    """
    if value is not None and isinstance(value, Decimal):
        locale.setlocale(locale.LC_ALL, '')
        # if the value is a decimal instance, add comma separator to number, like '1,999,999'
        value = locale.format_string('%d', value, grouping=True)

    return value


def is_export(request, action):
    if request.query_params.get('override_max') is True:
        return True
    if request.content_type.lower() in (
        core_config.content_type_json,
        core_config.content_type_plain,
    ) or action not in core_config.export_factory_supported_actions:
        return False
    return True


def convert_to_int(val, default=0, orig=False):
    """Parse to integer value from string. In case of failure to do so, assign default value."""
    try:
        return int(val)
    except:
        if orig:
            return val
        else:
            return int(default)


def convert_to_float(val, default=0, orig=False):
    """Parse to float value from string. In case of failure to do so, assign default value."""
    try:
        return float(val)
    except:
        if orig:
            return val
        else:
            return float(default)


def get_footer_copyright():
    """ Get the copyright message for email/html page"""
    current_year = str(get_current_datetime_object().year)
    copy_right = core_config.copyright_text[0] + current_year + core_config.copyright_text[1]
    return copy_right


def get_random_choice(choices):
    """ Accepts a list of choices and return the randomly chosen choice. """
    return secrets.choice(choices)


def get_support_email():
    """
    get email id for sending emails
    :return:
    """
    emails = settings.SUPPORT_EMAIL_ID
    if len(emails) > 0:
        email_options = emails.split(',')
        return get_random_choice(email_options)
    return ''


def get_project_title():
    """ Get the project title/name """
    return settings.PROJECT_FULL_NAME or settings.PROJECT_SHORT_NAME


def is_valid_mobile_number(mobile_number):
    """
    is_valid_mobile_number
        Utility method to validate phone numbers
    :param mobile_number:
    :return:
    """
    return mobile_number.isdigit() and len(mobile_number) == core_config.mobile_number_length


def queryset_iterator(queryset, chunk_size=1000, print_msg=True):
    """
    Iterate over a Django Queryset ordered by the primary key

    This method loads a maximum of chunk_size (default: 1000) rows in its
    memory at the same time while django normally would load all rows in its
    memory. Using the iterator() method only causes it to not preload all the
    classes.

    Note that the implementation of the iterator does not support ordered query sets.
    """
    if not queryset:
        logger.debug('Queryset has not data to iterate over: {0}'.format(queryset.query))
        return list(queryset)

    pk = 0
    last_pk = queryset.order_by('-pk')[0].pk
    queryset = queryset.order_by('pk')
    while pk < last_pk:
        if print_msg:
            logger.debug('Current selection query: {0}'.format(queryset.filter(pk__gt=pk)[:chunk_size].query))
        row = list(queryset.filter(pk__gt=pk)[:chunk_size])
        pk = row[-1].pk
        yield row
        gc.collect()


def column_normalize(data_df, valid_columns=None):
    """
    column_normalize
        This method is used to normalize the column names of a data frame.

    :param data_df: data frame
    :param valid_columns: list - list of supported normalized column names,
    if empty/None keep all columns otherwise remove missing columns from data frame
    :return:
    """
    _columns = dict()
    _to_delete = []
    for column in data_df.columns.tolist():
        original = column
        column = normalize_str(column)
        _columns[original] = str(column)
        if valid_columns is not None and column not in valid_columns:
            _to_delete.append(column)
    data_df.rename(columns=_columns, inplace=True)
    if _to_delete:
        data_df.drop(_to_delete, axis=1, inplace=True)


def bulk_create_or_update(records, model, unique_fields, batch_size=1000):
    # Let's define two lists:
    # - one to hold the values that we want to insert,
    # - and one to hold the new values alongside existing primary keys to update
    records_to_create = []
    records_to_update = []

    # This is where we check if the records are pre-existing,
    # and add primary keys to the objects if they do
    records = [
        {
            'id': model.objects.filter(**{f: record[f] for f in unique_fields}).first().id
            if model.objects.filter(**{f: record[f] for f in unique_fields}).first() is not None else None,
            **record,
        }
        for record in records
    ]

    # This is where we delegate our records to our split lists:
    # - if the record already exists in the DB (the 'id' primary key), add it to the update list.
    # - Otherwise, add it to the creation list.
    [
        records_to_update.append(record)
        if record['id'] is not None
        else records_to_create.append(record)
        for record in records
    ]

    if len(records_to_create) > 0:
        logger.debug('Total records to create: {}'.format(len(records_to_create)))
        # Remove the 'id' field, as these will all hold a value of None,
        # since these records do not already exist in the DB
        [record.pop('id') for record in records_to_create]

        model.objects.bulk_create(
            [model(**values) for values in records_to_create], batch_size=batch_size
        )

    if len(records_to_update) > 0:
        logger.debug('Total records to update: {}'.format(len(records_to_update)))
        for f in unique_fields:
            [record.pop(f) for record in records_to_update]

        model.objects.bulk_update(
            [
                model(**values)
                for values in records_to_update
            ],
            set(records_to_update[0].keys()) - {'id', },
            batch_size=batch_size,
        )


def get_giga_filter_fields(request):
    from proco.accounts.models import AdvanceFilter
    from proco.utils.cache import cache_manager

    filter_field_data = {}

    if request.query_params.get('cache', 'on').lower() in ['on', 'true']:
        filter_field_data = cache_manager.get('GIGA_FILTERS_FIELDS')

    if not filter_field_data:
        filter_field_data = {}
        filters_data = AdvanceFilter.objects.filter(status=AdvanceFilter.FILTER_STATUS_PUBLISHED)
        for data in filters_data:
            parameter = data.column_configuration
            table_filters = filter_field_data.get(parameter.table_alias, [])
            table_filters.append(parameter.name + '__' + data.query_param_filter)
            if isinstance(data.options, dict) and data.options.get('include_none_filter', False):
                table_filters.append(parameter.name + '__none_' + data.query_param_filter)
            filter_field_data[parameter.table_alias] = table_filters
    return filter_field_data


def get_filter_sql(request, filter_key, table_name):
    filter_fields = get_giga_filter_fields(request)
    query_params = request.query_params.dict()

    advance_filters = set(filter_fields.get(filter_key, [])) & set(query_params.keys())

    sql_list = []
    for field_filter in advance_filters:
        filter_value = str(query_params[field_filter]).lower()
        sql_str = None
        field_name = None

        if field_filter.endswith('__exact'):
            field_name = field_filter.replace('__exact', '')

            if filter_value == 'none':
                sql_str = """coalesce(TRIM({table_name}."{field_name}"), '') = ''"""
            else:
                sql_str = """{table_name}."{field_name}" = '{value}'"""
        elif field_filter.endswith('__iexact'):
            field_name = field_filter.replace('__iexact', '')

            if filter_value == 'none':
                sql_str = """coalesce(TRIM({table_name}."{field_name}"), '') = ''"""
            else:
                sql_str = """LOWER({table_name}."{field_name}") = '{value}'"""
        elif field_filter.endswith('__contains'):
            field_name = field_filter.replace('__contains', '')
            sql_str = """{table_name}."{field_name}"::text LIKE '{value}'"""
        elif field_filter.endswith('__icontains'):
            field_name = field_filter.replace('__icontains', '')
            sql_str = """LOWER({table_name}."{field_name}")::text LIKE '{value}'"""
        elif field_filter.endswith('__on'):
            field_name = field_filter.replace('__on', '')

            if filter_value == 'none':
                sql_str = """{table_name}."{field_name}" IS NULL"""
            else:
                sql_str = """{table_name}."{field_name}" = {value}"""
        elif field_filter.endswith('__range'):
            field_name = field_filter.replace('__range', '')
            if ',' not in filter_value:
                filter_value += ',null'

            start, end = filter_value.split(',')
            if start != 'null':
                sql_list.append("""{table_name}."{field_name}" >= {value}""".format(
                    table_name=table_name,
                    field_name=field_name,
                    value=start,
                ))
            if end != 'null':
                sql_list.append("""{table_name}."{field_name}" <= {value}""".format(
                    table_name=table_name,
                    field_name=field_name,
                    value=end,
                ))
        elif field_filter.endswith('__none_range'):
            field_name = field_filter.replace('__none_range', '')
            none_sql_str = """{table_name}."{field_name}" IS NULL""".format(
                table_name=table_name,
                field_name=field_name,
            )
            if ',' not in filter_value:
                filter_value += ',null'

            start, end = filter_value.split(',')
            range_sql_list = []
            if start != 'null':
                range_sql_list.append("""{table_name}."{field_name}" >= {value}""".format(
                    table_name=table_name,
                    field_name=field_name,
                    value=start,
                ))
            if end != 'null':
                range_sql_list.append("""{table_name}."{field_name}" <= {value}""".format(
                    table_name=table_name,
                    field_name=field_name,
                    value=end,
                ))
            if len(range_sql_list) == 0:
                sql_list.append(none_sql_str)
            elif len(range_sql_list) == 1:
                sql_list.append('(' + none_sql_str + ' OR ' + range_sql_list[0] + ')')
            elif len(range_sql_list) == 2:
                sql_list.append('(' + none_sql_str + ' OR (' + range_sql_list[0] + ' AND ' + range_sql_list[1] + '))')
        elif field_filter.endswith('__in'):
            field_name = field_filter.replace('__in', '')
            filter_value = ','.join(["'" + str(f).lower() + "'" for f in filter_value.split(',')])
            sql_str = """LOWER({table_name}."{field_name}") IN ({value})"""

        if sql_str:
            sql_list.append(sql_str.format(table_name=table_name, field_name=field_name, value=filter_value))

    return ' AND '.join(sql_list)
