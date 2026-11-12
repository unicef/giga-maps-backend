from django.http import QueryDict
from django.test import SimpleTestCase

from proco.accounts import exceptions as accounts_exceptions
from proco.accounts import utils as account_utilities


class ValidateIdQueryParamsTestCase(SimpleTestCase):

    def assert_valid(self, query_string):
        account_utilities.validate_id_query_params(QueryDict(query_string))

    def assert_invalid(self, query_string):
        with self.assertRaises(accounts_exceptions.InvalidIdQueryParamError):
            account_utilities.validate_id_query_params(QueryDict(query_string))

    def test_numeric_ids_are_accepted(self):
        self.assert_valid('school_id=1')
        self.assert_valid('school_id__in=1,2,3')
        self.assert_valid('school_id__in=1, 2 ,3')
        self.assert_valid('country_id=144&admin1_id=7&layer_id=30')
        self.assert_valid('exclude_schools_same_coords_except_id=5')

    def test_numeric_prefixed_ids_are_accepted(self):
        self.assert_valid('school_entity_id__in=1,2&health_layer_id=40&health_country_id=144')

    def test_blank_values_are_skipped(self):
        self.assert_valid('school_id__in=')
        self.assert_valid('school_id=%20')

    def test_non_id_params_are_ignored(self):
        self.assert_valid('benchmark=global&start_date=11-05-2026&school_ids=abc')
        self.assert_valid('school__connectivity_type__in=all&filter__school__coverage__exact=all')
        self.assert_valid('giga_id_school=abc&external_id=abc')

    def test_non_numeric_ids_are_rejected(self):
        self.assert_invalid('school_id=all')
        self.assert_invalid('school_id__in=1,abc')
        self.assert_invalid('school_id__in=1,,2')
        self.assert_invalid('entity_id=-1')
        self.assert_invalid('country_id=undefined')
        self.assert_invalid('admin1_id__in=1.5')
        self.assert_invalid('layer_id=abc')
        self.assert_invalid('exclude_schools_same_coords_except_id=abc')

    def test_non_numeric_prefixed_ids_are_rejected(self):
        self.assert_invalid('school_entity_id__in=1,abc')
        self.assert_invalid('health_school_id=all')
        self.assert_invalid('health_country_id=abc')

    def test_any_prefix_is_rejected(self):
        self.assert_invalid('a__b_entity_id=abc')
        self.assert_invalid('a-b_entity_id__in=abc')

    def test_sql_fragments_are_rejected(self):
        self.assert_invalid('school_id=0) OR 1=1 OR (1=1')
        self.assert_invalid('school_id__in=1,2);--')

    def test_unexpected_digits_are_rejected(self):
        self.assert_invalid('school_id=%D9%A1')

    def test_every_repeated_value_is_checked(self):
        self.assert_invalid('school_id=abc&school_id=1')
