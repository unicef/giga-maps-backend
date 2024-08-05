""" Config file to specify application configurations used in the PROCO app"""
import json
import os
from django.conf import settings

FILTERS_FIELDS = None


class AppConfig(object):

    @property
    def copyright_text(self):
        """copyright text format"""
        return ['©2023-', ' All rights reserved.']

    @property
    def csv_content_type(self):
        return 'csv'

    @property
    def content_type_csv(self):
        return 'text/csv'

    @property
    def content_type_json(self):
        return 'application/json'

    @property
    def content_type_plain(self):
        return 'text/plain'

    @property
    def export_factory_supported_actions(self):
        return 'list', 'retrieve'

    @property
    def exports_upper_limit(self):
        """
        exports_upper_limit
            Returns the maximum number of records that can be exported
        :return:
        """
        return 20000

    @property
    def export_file_delimiter(self):
        return ','

    @property
    def mobile_number_length(self):
        """Length of valid mobile number"""
        return 10

    @property
    def get_giga_filter_fields(self):
        global FILTERS_FIELDS
        if FILTERS_FIELDS is None:
            filter_fields = {}
            filters_data = settings.FILTERS_DATA
            for data in filters_data:
                parameter = data['parameter']
                table_filters = filter_fields.get(parameter['table'], [])
                table_filters.append(parameter['field'] + '__' + parameter['filter'])
                if data.get('include_none_filter', False):
                    table_filters.append(parameter['field'] + '__none_' + parameter['filter'])
                filter_fields[parameter['table']] = table_filters
            FILTERS_FIELDS = filter_fields
        return FILTERS_FIELDS


app_config = AppConfig()
