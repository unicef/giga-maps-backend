from django.test import TestCase

from proco.giga_meter.config import app_config as giga_meter_configs
from proco.utils.tests import TestAPIViewSetMixin


class ConfigTestCase(TestAPIViewSetMixin, TestCase):

    def test_app_name(self):
        self.assertEqual(type(giga_meter_configs.app_name), str)
        self.assertGreater(len(giga_meter_configs.app_name), 0)
