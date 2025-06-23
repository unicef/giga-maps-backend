class AppConfig(object):

    @property
    def valid_name_pattern(self):
        """Regex to validate names"""
        return r'[a-zA-Z0-9-\' _()]*$'

    @property
    def valid_filter_name_pattern(self):
        """Regex to validate names"""
        return r'[a-zA-Z0-9-\' _()#]*$'

    @property
    def api_key_deleted_email_template(self):
        """HTML template for API Key deletion emails"""
        return 'email/apis/api_key_deleted_email.html'

    @property
    def api_key_extension_approved_email_template(self):
        """HTML template for API Key Extension Approval emails"""
        return 'email/apis/api_key_extension_approved_email.html'

    @property
    def api_key_extension_rejected_email_template(self):
        """HTML template for API Key Extension Rejection emails"""
        return 'email/apis/api_key_extension_rejected_email.html'

    @property
    def api_key_generated_email_template(self):
        """HTML template for API Key Generation emails"""
        return 'email/apis/api_key_generated_email.html'

    @property
    def api_key_rejected_email_template(self):
        """HTML template for API Key Rejection emails"""
        return 'email/apis/api_key_rejected_email.html'

    @property
    def api_key_generation_email_subject_format(self):
        """format for the email subject when an API Key is generated for a Public API."""
        # '%s - API Key generated for "%s" API'
        return '%s - %s API Key Generated'

    @property
    def private_api_key_generation_email_subject_format(self):
        """format for the email subject when an API Key is initiated for a Private API."""
        return '%s - API key approval request for "%s" API'

    @property
    def private_api_key_generation_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return """The user has requested the API key for "{api_name}" API. Please review the request at the following link and take appropriate action:

        User: {requested_user}
        Countries: {countries}
        Description: {description}

        {dashboard_url}
        """

    @property
    def private_api_key_rejected_email_subject_format(self):
        """format for the email subject when an API Key is rejected for a Private API."""
        return '%s - Giga Maps API key request'

    @property
    def private_api_key_extension_request_email_subject_format(self):
        """format for the email subject when an API Key is initiated for a Private API."""
        return '%s - API key extension approval request for "%s" API'

    @property
    def private_api_key_extension_request_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return """The user has requested the API key extension for "{api_name}" API. Please review the request at the following link and take appropriate action:

            User: {requested_user}
            Extension till date: {till_date}

            {dashboard_url}
            """

    @property
    def api_key_extension_approved_email_subject_format(self):
        """format for the email subject when an API Key extension is approved for a Public/Private API."""
        return '%s - API Key extension approved for %s API'

    @property
    def api_key_extension_rejected_email_subject_format(self):
        """format for the email subject when an API Key extension is rejected for a Public/Private API."""
        return '%s - API Key extension rejected for %s API'

    @property
    def standard_email_template_name(self):
        """template for standard emails"""
        return 'email/standard_email.html'

    @property
    def data_layer_creation_email_subject_format(self):
        """format for the email subject when an API Key is generated for a Public API."""
        return '%s - Data Layer created with name "%s"'

    @property
    def data_layer_creation_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return 'Data layer created successfully by the editor. Please check your dashboard for the details.'

    @property
    def data_layer_update_email_subject_format(self):
        """format for the email subject when an API Key is generated for a Public API."""
        return '%s - Data Layer updated with name "%s"'

    @property
    def data_layer_update_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return 'Data layer updated successfully by the editor. Please check your dashboard for the details.'

    @property
    def data_layer_update_ready_to_publish_email_subject_format(self):
        """format for the email subject when an API Key is generated for a Public API."""
        return '%s - Data Layer submitted with name "%s"'

    @property
    def data_layer_update_ready_to_publish_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return """Data layer updated successfully by the editor and assign it to you for preview and publish. Please check your dashboard for the details.
        {dashboard_url}
        """

    @property
    def api_key_deletion_email_subject_format(self):
        """format for the email subject when an API Key is deleted by admin for an API."""
        return '%s - API key for %s API has been deleted'


app_config = AppConfig()
