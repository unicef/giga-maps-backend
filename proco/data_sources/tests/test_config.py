from django.test import TestCase

from proco.data_sources.config import app_config as sources_configs
from proco.utils.tests import TestAPIViewSetMixin


class ConfigTestCase(TestAPIViewSetMixin, TestCase):

    def test_school_master_update_email_subject_format(self):
        self.assertEqual(type(sources_configs.school_master_update_email_subject_format), str)
        self.assertGreater(len(sources_configs.school_master_update_email_subject_format), 0)

    def test_school_master_update_email_message_format(self):
        self.assertEqual(type(sources_configs.school_master_update_email_message_format), str)
        self.assertGreater(len(sources_configs.school_master_update_email_message_format), 0)

    def test_school_master_records_to_review_email_subject_format(self):
        self.assertEqual(type(sources_configs.school_master_records_to_review_email_subject_format), str)
        self.assertGreater(len(sources_configs.school_master_records_to_review_email_subject_format), 0)

    def test_school_master_records_to_review_email_message_format(self):
        self.assertEqual(type(sources_configs.school_master_records_to_review_email_message_format), str)
        self.assertGreater(len(sources_configs.school_master_records_to_review_email_message_format), 0)
