""" Config file to specify application configurations used in the PROCO app"""


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


app_config = AppConfig()
