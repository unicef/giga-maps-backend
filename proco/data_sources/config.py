class AppConfig(object):

    @property
    def school_master_update_email_subject_format(self):
        """format for the email subject when an API Key is generated for a Public API."""
        return '%s - School Master Data Source has updated records'

    @property
    def school_master_update_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return """We would like to inform you that the School Master Data Source has been updated with new records. Kindly review the details on your dashboard.

        {dashboard_url}{delete_msg}{error_msg}"""

    @property
    def school_master_records_to_review_email_subject_format(self):
        """format for the email subject when an API Key is generated for a Public API."""
        return '%s - Reminder Mail - School Master Data Source has records to review'

    @property
    def school_master_records_to_review_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return """The School Master Data Source contains records that require review. We kindly request that you review these records and take any necessary actions from your end. This will ensure that the application users can access the updated data.

        Please check your dashboard for the details.

        {dashboard_url}
        """


app_config = AppConfig()
