from django.test import TestCase
from django.urls import reverse

from proco.utils.tests import TestAPIViewSetMixin
from proco.utils.urls import add_url_params


# INFO - TODO: Writing this file here is wrong but as under utils directory tests.py file is already present
#  So tests directory not possible until we changes tests.py file name
class UtilsUrlsUtilitiesTestCase(TestAPIViewSetMixin, TestCase):

    def test_add_url_params_utilit_y(self):
        view_name = 'connection_statistics:school-connectivity-stat'
        query_param = {
            'country_id': '144',
            'school_ids': '34554,34555,34557',
            'indicator': 'download',
            'start_date': '2023-08-21',
            'end_date': '2023-08-27',
            'is_weekly': 'true',
        }

        url = reverse(view_name)

        updated_urls = add_url_params(url, query_param)

        self.assertNotEquals(url, updated_urls)

        self.assertNotIn('is_weekly=true', url)
        self.assertIn('is_weekly=true', updated_urls)

        self.assertNotIn('indicator=download', url)
        self.assertIn('indicator=download', updated_urls)

