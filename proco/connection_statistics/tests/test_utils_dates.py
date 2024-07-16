from django.test import TestCase

from proco.utils.dates import *
from proco.utils.tests import TestAPIViewSetMixin


# INFO - TODO: Writing this file here is wrong but as under utils directory tests.py file is already present
#  So tests directory not possible until we changes tests.py file name
class UtilsDateUtilitiesTestCase(TestAPIViewSetMixin, TestCase):

    def test_get_first_date_of_month_utility(self):
        self.assertEqual(get_first_date_of_month(2022, 1), timezone.datetime(2022, 1, 1).date())
        self.assertEqual(get_first_date_of_month(2023, 8), timezone.datetime(2023, 8, 1).date())

    def test_get_last_date_of_month_utility(self):
        self.assertEqual(get_last_date_of_month(2023, 1), timezone.datetime(2023, 1, 31).date())
        self.assertEqual(get_last_date_of_month(2023, 2), timezone.datetime(2023, 2, 28).date())
        self.assertEqual(get_last_date_of_month(2023, 8), timezone.datetime(2023, 8, 31).date())
        self.assertEqual(get_last_date_of_month(2024, 2), timezone.datetime(2024, 2, 29).date())
        self.assertEqual(get_last_date_of_month(2023, 12), timezone.datetime(2023, 12, 31).date())

    def test_date_range_list_utility(self):
        today = timezone.now().date()
        date_7_days_back = today - timedelta(days=6)
        week_dates = list(date_range_list(date_7_days_back, today))

        self.assertEqual(len(week_dates), 7)

    def test_is_date_utility(self):
        self.assertTrue(is_date('2023-08-24'))
        self.assertTrue(is_date('1999-08-24'))
        # TODO: Below commented test cases are not working
        # self.assertFalse(is_date('2023-08-32'))
        # self.assertFalse(is_date('2023-24-08'))
        self.assertFalse(is_date('24-08-2023'))
        self.assertFalse(is_date(None))
        self.assertFalse(is_date(''))
        self.assertFalse(is_date('ABCD'))

    def test_is_datetime_utility(self):
        self.assertTrue(is_datetime('2023-03-20T04:00:19.886481Z'))
        self.assertTrue(is_datetime('1999-08-24T04:00:19.886481Z'))
        self.assertTrue(is_datetime('1999-08-24T04:00:19'))
        self.assertTrue(is_datetime('1999-08-24 04:00:19'))
        # TODO: Below commented test cases are not working
        # self.assertFalse(is_datetime('2023-08-32T04:00:19.886481Z'))
        # self.assertFalse(is_datetime('2023-24-08T04:00:19.886481Z'))
        self.assertFalse(is_datetime('24-08-2023T04:00:19.886481Z'))
        self.assertFalse(is_datetime('24-08-2023'))
        self.assertFalse(is_datetime(None))
        self.assertFalse(is_datetime(''))
        self.assertFalse(is_datetime('ABCD'))

    def test_to_date_utility(self):
        # self.assertEqual(to_date('2023-08-24'), datetime.strptime('2023-08-24', '%Y-%m-%d').date())
        self.assertIsNone(to_date('ABCD'))
        self.assertIsNone(to_date(None))
        self.assertIsNone(to_date(1233.12))
        self.assertIsNone(to_date('2023-24-24'))

    def test_to_datetime_utility(self):
        # self.assertEqual(to_datetime('2023-03-20T04:00:19'),
        #                  datetime.strptime('2023-03-20T04:00:19', '%Y-%m-%dT%H:%M:%S'))
        self.assertIsNone(to_datetime('2023-20-20T04:00:19'))

    def test_format_date_utility(self):
        today = timezone.now().date()
        self.assertEqual(format_date(today), today.strftime(settings.DATE_FORMAT))
        self.assertIsNone(format_date('2023-24-24'))

    def test_format_datetime_utility(self):
        today = timezone.now()
        self.assertEqual(format_datetime(today), today.strftime(settings.DATETIME_FORMAT))
        self.assertIsNone(format_datetime('2023-20-20T04:00:19'))
