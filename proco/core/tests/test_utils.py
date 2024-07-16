from django.test import TestCase
from django.utils import timezone

from proco.core import db_utils as core_db_utilities
from proco.core import utils as core_utilities
from proco.utils.tests import TestAPIViewSetMixin


class UtilsUtilitiesTestCase(TestAPIViewSetMixin, TestCase):

    def test_to_boolean_utility(self):
        self.assertFalse(core_utilities.to_boolean(False))
        self.assertTrue(core_utilities.to_boolean(True))

        self.assertFalse(core_utilities.to_boolean('false'))
        self.assertTrue(core_utilities.to_boolean('true'))

        self.assertFalse(core_utilities.to_boolean('0'))
        self.assertTrue(core_utilities.to_boolean('1'))

        self.assertFalse(core_utilities.to_boolean(0))
        self.assertTrue(core_utilities.to_boolean(1))

    def test_is_blank_string_utility(self):
        self.assertTrue(core_utilities.is_blank_string(None))
        self.assertTrue(core_utilities.is_blank_string(''))
        self.assertTrue(core_utilities.is_blank_string('   '))

        self.assertFalse(core_utilities.is_blank_string(123))
        self.assertFalse(core_utilities.is_blank_string('ABC'))
        self.assertFalse(core_utilities.is_blank_string('1'))

    def test_get_current_datetime_object_utility(self):
        current_date = core_utilities.get_current_datetime_object(timezone.now())
        self.assertEqual(type(current_date), type(timezone.now()))

        current_date = core_utilities.get_current_datetime_object()
        self.assertEqual(type(current_date), type(timezone.now()))

    def test_sanitize_str_utility(self):
        self.assertEqual(core_utilities.sanitize_str('ABCD'), 'ABCD')
        self.assertEqual(core_utilities.sanitize_str('ABCD   '), 'ABCD')
        self.assertEqual(core_utilities.sanitize_str('   ABCD   '), 'ABCD')
        self.assertEqual(core_utilities.sanitize_str('   ABCD'), 'ABCD')
        self.assertEqual(core_utilities.sanitize_str(1234), 1234)

    def test_normalize_str_utility(self):
        self.assertEqual(core_utilities.normalize_str('ABCD'), 'ABCD')
        self.assertEqual(core_utilities.normalize_str('ABCD '), 'ABCD ')
        self.assertEqual(core_utilities.normalize_str(1234), 1234)

        self.assertEqual(core_utilities.normalize_str('ABCD$$'), 'ABCD')
        self.assertEqual(core_utilities.normalize_str('ABCD$%^'), 'ABCD')
        self.assertEqual(core_utilities.normalize_str('ABCD@&^%#'), 'ABCD')

    def test_get_random_string_utility(self):
        self.assertEqual(len(core_utilities.get_random_string()), 264)
        self.assertEqual(len(core_utilities.get_random_string(length=255)), 255)

    def test_format_decimal_data_utility(self):
        self.assertIsNone(core_utilities.format_decimal_data(None))
        self.assertEqual(core_utilities.format_decimal_data(199.505), 199.505)

    def test_convert_to_int_utility(self):
        self.assertIsNone(core_utilities.convert_to_int(None, orig=True))
        self.assertEqual(core_utilities.convert_to_int(199.505), 199)
        self.assertEqual(core_utilities.convert_to_int(199), 199)
        self.assertEqual(core_utilities.convert_to_int('199'), 199)
        self.assertEqual(core_utilities.convert_to_int('ABCD'), 0)

    def test_convert_to_float_utility(self):
        self.assertIsNone(core_utilities.convert_to_float(None, orig=True))
        self.assertEqual(core_utilities.convert_to_float(199.505), 199.505)
        self.assertEqual(core_utilities.convert_to_float(199), 199)
        self.assertEqual(core_utilities.convert_to_float('199'), 199)
        self.assertEqual(core_utilities.convert_to_float('ABCD'), 0)

    def test_get_footer_copyright_utility(self):
        self.assertEqual(type(core_utilities.get_footer_copyright()), str)
        self.assertIn(str(str(core_utilities.get_current_datetime_object().year)),
                      core_utilities.get_footer_copyright())

    def test_get_random_choice_utility(self):
        self.assertEqual(type(core_utilities.get_random_choice(['a', 'b', 'c', 'd'])), str)

        self.assertIn(core_utilities.get_random_choice(['aa', 'bb', 'cc', 'dd']), ['aa', 'bb', 'cc', 'dd'])

    def test_get_sender_email_utility(self):
        self.assertEqual(type(core_utilities.get_sender_email()), str)

        self.assertIsNotNone(core_utilities.get_sender_email())

    def test_get_support_email_utility(self):
        self.assertEqual(type(core_utilities.get_support_email()), str)

        self.assertIsNotNone(core_utilities.get_support_email())

    def test_get_project_title_utility(self):
        self.assertEqual(type(core_utilities.get_project_title()), str)

        self.assertIsNotNone(core_utilities.get_project_title())

    def test_is_valid_mobile_number_utility(self):
        self.assertTrue(core_utilities.is_valid_mobile_number('1234567890'))

        self.assertFalse(core_utilities.is_valid_mobile_number('ABCDEF'))


class DBUtilsUtilitiesTestCase(TestAPIViewSetMixin, TestCase):

    def test_sql_to_response_utility(self):
        sql = 'SELECT id FROM locations_country WHERE id = 123456787'
        result = core_db_utilities.sql_to_response(sql, label='InvalidCountrySearch')

        self.assertEqual(len(result), 0)
        self.assertEqual(type(result), list)

    def test_sql_to_response_utility_with_invalid_query(self):
        sql = 'SELECT invalid_column_name FROM locations_country WHERE id = 123456787'
        result = core_db_utilities.sql_to_response(sql, label='InvalidCountrySearch')
        self.assertIsNone(result)

        sql = ''
        result = core_db_utilities.sql_to_response(sql, label='InvalidCountrySearch')
        self.assertIsNone(result)

