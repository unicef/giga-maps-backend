from unittest.mock import MagicMock, patch
import pandas as pd
from django.test import TestCase

from proco.data_sources import models as sources_models
from proco.data_sources import utils as sources_utilities
from proco.schools.tests.factories import SchoolFactory
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
            'connectivity_RT': None,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': 'School Id Govt',
            'admin1_id_giga': 'admin1_id_giga',
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': 1234567,
            'longitude': 1234567,
            'education_level': 'Education Level',
            'connectivity_RT': None,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': 'admin1_id_giga',
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': 1234567,
            'longitude': 1234567,
            'education_level': 'Education Level',
            'connectivity_RT': None,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': None,
            'latitude': 1234567,
            'longitude': 1234567,
            'education_level': 'Education Level',
            'connectivity_RT': None,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': None,
            'latitude': school.geopoint.y,
            'longitude': 1234567,
            'education_level': 'Education Level',
            'connectivity_RT': None,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': None,
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': 'Education Level',
            'connectivity_RT': None,
        }, school))

        self.assertFalse(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': school.admin1.giga_id_admin,
            'admin2_id_giga': None,
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': school.education_level,
            'connectivity_RT': None,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': school.education_level,
            'connectivity_RT': None,
        }, school))

        self.assertTrue(sources_utilities.has_changes_for_review({
            'school_name': school.name,
            'school_id_govt': school.external_id,
            'admin1_id_giga': None,
            'admin2_id_giga': 'admin2_id_giga',
            'latitude': school.geopoint.y,
            'longitude': school.geopoint.x,
            'education_level': school.education_level,
            'connectivity_RT': None,
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
        self.assertIsNone(sources_utilities.sync_qos_realtime_data(123))

    def test_sync_dailycheckapp_realtime_data(self):
        self.assertIsNone(sources_utilities.sync_dailycheckapp_realtime_data())

    @patch('requests.get')
    def test_load_daily_check_app_data_source_response_to_model_with_unexpected_fields(self, mock_get):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 200
        mock_response_1.json.return_value = [
            {
                'timestamp': '2026-08-20T10:00:00Z',
                'created_at': '2026-08-20T10:00:00Z',
                'school_id': '12345',
                'giga_id_school': 'GIGA-12345',
                'download': 10.5,
                'upload': 5.2,
                'latency': 25,
                'country_code': 'BRA',
                'source': 'DailyCheckApp',
                'protocol': 'https',
                'extra_random_field': 'foobar',
            }
        ]
        mock_response_2 = MagicMock()
        mock_response_2.status_code = 200
        mock_response_2.json.return_value = []
        mock_get.side_effect = [mock_response_1, mock_response_2]

        request_configs = {
            'url': 'https://example.com/measurements',
            'method': 'GET',
            'data_limit': 1000,
            'auth_token_required': False,
            'headers': {'Content-Type': 'application/json'}
        }

        initial_count = sources_models.DailyCheckAppMeasurementData.objects.count()
        sources_utilities.load_daily_check_app_data_source_response_to_model(
            sources_models.DailyCheckAppMeasurementData,
            request_configs
        )
        self.assertEqual(sources_models.DailyCheckAppMeasurementData.objects.count(), initial_count + 1)
        record = sources_models.DailyCheckAppMeasurementData.objects.latest('created_at')
        self.assertEqual(record.school_id, '12345')
        self.assertEqual(record.download, 10.5)

    def test_sync_school_master_data_country_not_found(self):
        # Country does not exist in DB, should return None and not raise ValueError
        result = sources_utilities.sync_school_master_data(
            '/path/to/profile', 'share_name', 'schema_name', 'NONEXISTENT', {}, {}, {}
        )
        self.assertIsNone(result)

    @patch('delta_sharing.get_table_version')
    def test_sync_school_master_data_404_table_version(self, mock_get_table_version):
        from proco.locations.tests.factories import CountryFactory
        from requests.exceptions import HTTPError
        from requests.models import Response

        country = CountryFactory(iso3_format='TST')
        mock_response = Response()
        mock_response.status_code = 404
        mock_get_table_version.side_effect = HTTPError('404 Client Error: Not Found', response=mock_response)

        result = sources_utilities.sync_school_master_data(
            '/path/to/profile', 'share_name', 'schema_name', 'TST', {}, {}, {}
        )
        self.assertIsNone(result)

    @patch('delta_sharing.load_table_changes_as_pandas')
    @patch('delta_sharing.get_table_version')
    @patch('proco.data_sources.models.SchoolMasterData.get_last_version')
    def test_sync_school_master_data_version_reset(self, mock_get_last_version, mock_get_table_version, mock_load_changes):
        from proco.locations.tests.factories import CountryFactory

        country = CountryFactory(iso3_format='UKR')
        mock_get_last_version.return_value = 20
        mock_get_table_version.return_value = 3
        mock_load_changes.return_value = pd.DataFrame()

        sources_utilities.sync_school_master_data(
            '/path/to/profile', 'share_name', 'schema_name', 'UKR', {}, {}, {}
        )
        # Should have reset start version from 20 to 0 because 20 > 3
        mock_load_changes.assert_called_once()
        args, kwargs = mock_load_changes.call_args
        self.assertEqual(args[1], 0)
        self.assertEqual(args[2], 3)

    @patch('delta_sharing.load_table_changes_as_pandas')
    @patch('delta_sharing.get_table_version')
    def test_sync_school_master_data_400_load_changes(self, mock_get_table_version, mock_load_changes):
        from proco.locations.tests.factories import CountryFactory
        from requests.exceptions import HTTPError
        from requests.models import Response

        country = CountryFactory(iso3_format='ERR')
        mock_get_table_version.return_value = 5
        mock_response = Response()
        mock_response.status_code = 400
        mock_load_changes.side_effect = HTTPError('400 Client Error: Bad Request', response=mock_response)

        result = sources_utilities.sync_school_master_data(
            '/path/to/profile', 'share_name', 'schema_name', 'ERR', {}, {}, {}
        )
        self.assertIsNone(result)
