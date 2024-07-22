import csv
import json
from math import ceil
from urllib.parse import urlsplit

from django.http import HttpResponse
from django.urls import resolve, is_valid_path
from rest_framework import status
from rest_framework.response import Response

from proco.accounts import models as accounts_models
from proco.core import exceptions as core_exceptions
from proco.core import utils as core_utilities
from proco.core.config import app_config as core_configs
from proco.utils import dates as date_utilities


class ActionSerializerMixin(object):
    action_serializers = {}

    def get_action_from_metadata(self, action):
        if action == 'metadata':
            # then get 'action' from query parameter and if not found, raise exception
            method = self.request.data.get('action', None)
            if method is None:
                raise core_exceptions.RequiredMetadataActionFieldError()
            action = self.action_map.get(method.lower(), None)
            # action_map contains actions for current API only. eg: for create API URL,
            # if user provides 'put' the following exception would occur
            if action is None:
                raise core_exceptions.InvalidMetadataUnsupportedActionFieldError(
                    current_action=method,
                )
        return action

    def get_serializer_class(self):
        action = self.action.lower()
        action = self.get_action_from_metadata(action)

        if action in self.action_serializers:
            self.serializer_class = self.action_serializers[action]

        return super().get_serializer_class()


class DownloadAPIDataToCSVMixin(object):

    def perform_pre_checks(self, request, *args, **kwargs):
        """
        perform_pre_checks
            Check all the pre-requisites before exporting the data to CSV file.
        """
        page_size = core_utilities.convert_to_int(request.query_params.get('page_size'), default=10)
        # If page_size is more than allowed limit, then raise the error
        if core_utilities.is_export(request, self.action) and page_size > core_configs.exports_upper_limit:
            raise core_exceptions.InvalidExportRecordsCountError()

        headers = dict(request.headers)
        api_key = headers.get('Api-Key')
        # If API Key is not provided, then raise the error
        if core_utilities.is_blank_string(api_key):
            raise core_exceptions.RequiredAPIKeyFilterError()

        valid_api_key = accounts_models.APIKey.objects.filter(
            user=request.user,  # Check if API key is created by the current user
            api_key=api_key,  # Check the API key in database table
            status=accounts_models.APIKey.APPROVED,  # API Key must be APPROVED to enable the download/documentation
            valid_to__gte=core_utilities.get_current_datetime_object().date(),  # Check if given key is not expired
            api__download_url__isnull=False,  # Down URL must be configured to API to allow the user to download data
        ).first()

        # If any of the above query condition not satisfied then raise the error
        if not valid_api_key:
            raise core_exceptions.InvalidAPIKeyError()

        download_url = urlsplit(str(valid_api_key.api.download_url)).path
        # Check if the requested URL and API download URL is same for the given key
        # Otherwise there are chances that user create the key for another API and request the download for other API
        if not is_valid_path(download_url) or resolve(download_url).view_name != request.resolver_match.view_name:
            raise core_exceptions.InvalidAPIKeyError()

        return valid_api_key

    def get_filename(self, request, api_key_instance, total_records):
        report_title = request.query_params.get('report_title')

        if not report_title:
            page_size = core_utilities.convert_to_int(request.query_params.get('page_size'), default=10)

            total_pages = ceil(total_records / page_size)
            current_page_no = core_utilities.convert_to_int(request.query_params.get('page'), default=1)

            report_file_name = api_key_instance.api.report_title
            if (
                api_key_instance.api.category == accounts_models.API.API_CATEGORY_PUBLIC and
                core_utilities.is_blank_string(report_file_name)
            ):
                report_file_name = str('_'.join([api_key_instance.api.name, api_key_instance.api.category, '{dt}']))

            report_title = report_file_name.format(
                dt='page_' + str(current_page_no) + '_out_of_' + str(total_pages) + '_dated_' +
                   date_utilities.format_datetime(core_utilities.get_current_datetime_object(),
                                                  frmt='%d%m%Y_%H%M%S'),
            )

        return report_title

    def write(self, request, api_key_instance, data, total_records):
        if len(data) == 0:
            return Response(data={'error': ['No data available']}, status=status.HTTP_400_BAD_REQUEST)

        report_name = self.get_filename(request, api_key_instance, total_records)

        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{report_name}"'

        csv_header = list(data[0].keys()) if len(data) > 0 else []
        writer = csv.DictWriter(response, fieldnames=csv_header)
        header = dict(zip(csv_header, csv_header))
        writer.writerow(header)

        for row in data:
            writer.writerow(row)

        return response

    def list_export(self, request, *args, **kwargs):
        valid_api_key_instance = self.perform_pre_checks(request, *args, **kwargs)

        queryset = self.filter_queryset(self.get_queryset())

        if self.apply_query_pagination:
            response = self.get_custom_paginated_response(queryset)
            total_records = response.data.get('count', 0)
            return self.write(request, valid_api_key_instance, response.data.get('results', []), total_records)
        else:
            # Change the pagination limit to 20K as asked by Client
            self.paginator.max_page_size = core_configs.exports_upper_limit

            page = self.paginate_queryset(queryset)
            if page is not None:
                serializer = self.get_serializer(page, many=True)
            else:
                serializer = self.get_serializer(queryset, many=True)

            total_records = self.paginator.page.paginator.count

            return self.write(request, valid_api_key_instance, serializer.data, total_records)


class DownloadSerializerMixin:
    report_fields = {}
    boolean_flags = {True: 'Yes', False: 'No'}

    def to_record_representation(self, record):
        final_record = {}
        for field, attr in self.Meta.report_fields.items():
            value = record.get(field, '')

            if isinstance(attr, dict):
                header_name = attr.get('name')

                if attr and attr.get('is_exportable') is False:
                    continue

                if attr and attr.get('is_computed'):
                    method = getattr(self, 'get_{}'.format(field), None)
                    value = method(record) if callable(method) else value

                if value is None:
                    value = ''
                elif attr and attr.get('field_type') == 'date':
                    if date_utilities.is_datetime(value):
                        value = date_utilities.to_date(value)
                    elif date_utilities.is_date(value):
                        value = date_utilities.format_date(value)
                elif attr and attr.get('field_type') == 'json':
                    value = json.loads(value)
                    value = ', '.join(filter(None, value.values())) if value else ' '
            else:
                header_name = str(attr)

            if value is None:
                value = ''
            elif isinstance(value, bool):
                value = self.boolean_flags.get(value)
            elif isinstance(value, str):
                # replace next line and export delimiter from value if present
                value = value.replace('\r', ' ')
                value = value.replace('\n', ' ')
                value = value.replace('\"', '')
                value = value.replace(core_configs.export_file_delimiter, ' ')
            else:
                # format decimal values
                value = core_utilities.format_decimal_data(value)

            final_record[header_name] = str(value)

        return final_record
