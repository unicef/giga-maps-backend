import os

import pandas as pd
from django.conf import settings
from django.test import TestCase

from proco.core.utils import get_current_datetime_object
from proco.data_sources import utils as sources_utilities
from proco.schools.tests.factories import SchoolFactory
from proco.utils.dates import format_date
from proco.utils.tests import TestAPIViewSetMixin


class UtilsUtilitiesTestCase(TestAPIViewSetMixin, TestCase):

    def test_normalize_school_name(self):
        self.assertEqual(sources_utilities.normalize_school_name(''), 'Name unknown')
        self.assertEqual(sources_utilities.normalize_school_name('Test School Name'), 'Test School Name')
        self.assertEqual(sources_utilities.normalize_school_name('"Test School Name"'), 'Test School Name')

    def test_normalize_school_master_data_frame(self):
        df = pd.DataFrame.from_dict({'school_name': ['Test School'], 'school_id_govt': ['1234567']})
        self.assertEqual(len(sources_utilities.normalize_school_master_data_frame(df)), len(df))

        df = pd.DataFrame.from_dict({'school_name': ['Test School']})
        self.assertEqual(len(sources_utilities.normalize_school_master_data_frame(df)), len(df))

    def test_normalize_qos_data_frame(self):
        df = pd.DataFrame.from_dict({'school_name': ['Test School'], 'school_id_govt': ['1234567']})
        self.assertEqual(len(sources_utilities.normalize_qos_data_frame(df)), len(df))

        df = pd.DataFrame.from_dict({'school_name': ['Test School']})
        self.assertEqual(len(sources_utilities.normalize_qos_data_frame(df)), len(df))

    def test_has_changes_for_review(self):
        school = SchoolFactory()

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': 'School Name',
            'school_id_govt': 'School Id Govt',
            'admin1_id_giga': 'admin1_id_giga',
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': 1234567,
            'longitude': 1234567,
            'education_level': 'Education Level',
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': 'School Id Govt',
            'admin1_id_giga': 'admin1_id_giga',
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': 1234567,
            'longitude': 1234567,
            'education_level': 'Education Level',
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': 'admin1_id_giga',
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': 1234567,
            'longitude': 1234567,
            'education_level': 'Education Level',
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': None,
            'latitude': 1234567,
            'longitude': 1234567,
            'education_level': 'Education Level',
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': None,
            'latitude': school.geopoint.y,
            'longitude': 1234567,
            'education_level': 'Education Level',
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': None,
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': 'Education Level',
        }, school))

        self.assertFalse(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': school.admin1.giga_id_admin,
            'admin2_id_giga': None,
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': school.education_level,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': school.education_level,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': school.education_level,
        }, None))

    def test_parse_row(self):
        df = pd.DataFrame.from_dict({'school_name': ['Test School'], 'timestamp': [pd.Timestamp(0)]})

        self.assertEqual(type(sources_utilities.parse_row(df.iloc[0])), dict)

    def test_get_request_headers(self):
        request_configs = {
            'url': '/code/measurements/v2',
            'method': 'GET',
            'data_limit': 1000,
            'query_params': {
                'page': '{page_no}',
                'size': '{page_size}',
                'orderBy': 'created_at',
                'filterBy': 'created_at',
                'filterCondition': 'gt',
                'filterValue': 'last_measurement_date',
            },
            'auth_token_required': True,
            'headers': {
                'Content-Type': 'application/json'
            }
        }

        self.assertIn('Authorization', sources_utilities.get_request_headers(request_configs))
        self.assertEqual(sources_utilities.get_request_headers(request_configs)['Authorization'],
                         'Bearer dummy_value_to_raise_401_response_error_as_valid_key_not_available')

    # def test_load_qos_data_source_response_to_model(self):
    #     self.assertIsNone(sources_utilities.load_qos_data_source_response_to_model())
    #
    #     profile_file = os.path.join(
    #         settings.BASE_DIR,
    #         'qos_profile_{dt}.share'.format(
    #             dt=format_date(get_current_datetime_object())
    #         )
    #     )
    #
    #     self.assertFalse(os.path.isfile(profile_file))

    def test_sync_qos_realtime_data(self):
        self.assertIsNone(sources_utilities.sync_qos_realtime_data())

    def test_sync_dailycheckapp_realtime_data(self):
        self.assertIsNone(sources_utilities.sync_dailycheckapp_realtime_data())
