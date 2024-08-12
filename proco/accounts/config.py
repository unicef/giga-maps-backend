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
    def public_api_key_generation_email_subject_format(self):
        """format for the email subject when an API Key is generated for a Public API."""
        return '%s - API Key generated for "%s" API'

    @property
    def public_api_key_generation_email_message_format(self):
        """format for the email message when an API Key is generated for a Public API."""
        return ('API key generated successfully for the requested public API. '
                'Please check your dashboard for the API key.')

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
    def private_api_key_approved_email_subject_format(self):
        """format for the email subject when an API Key is approved for a Private API."""
        return '%s - API Key approved for "%s" API'

    @property
    def private_api_key_approved_email_message_format(self):
        """format for the email message when an API Key is approved for a Private API."""
        return ('API key request for the private API has been approved by Admin. '
                'Please check your dashboard for the approved API key.')

    @property
    def private_api_key_rejected_email_subject_format(self):
        """format for the email subject when an API Key is rejected for a Private API."""
        return '%s - API Key rejected for "%s" API'

    @property
    def private_api_key_rejected_email_message_format(self):
        """format for the email message when an API Key is rejected for a Private API."""
        return ('API key request for the private API has been rejected by Admin. Status has been updated in dashboard.'
                '\n\nWe kindly request that you reach out to our support team to obtain more details '
                'regarding the rejection.')

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
        return '%s - API Key extension approved for "%s" API'

    @property
    def api_key_extension_approved_email_message_format(self):
        """format for the email message when an API Key extension is approved for a Public/Private API."""
        return ('API key extension request has been approved by Admin. API key end date has been updated '
                'in your dashboard.')

    @property
    def api_key_extension_rejected_email_subject_format(self):
        """format for the email subject when an API Key extension is rejected for a Public/Private API."""
        return '%s - API Key extension rejected for "%s" API'

    @property
    def api_key_extension_rejected_email_message_format(self):
        """format for the email message when an API Key extension is rejected for a Public/Private API."""
        return ('API key extension request has been rejected by Admin.\n\n'
                'We kindly request that you reach out to our support team to obtain more details '
                'regarding the rejection.')

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
        return '%s - API Key is deleted for "%s" API'

    @property
    def api_key_deletion_email_message_format(self):
        """format for the email message when an API Key is deleted for an API."""
        return ('The admin has deleted the API Key due to security reasons.'
                ' Please raise the key again if required.')


app_config = AppConfig()
