from abc import ABCMeta

from django.utils.translation import ugettext as _
from rest_framework import serializers


class BaseValidationError(serializers.ValidationError, metaclass=ABCMeta):
    """Base validation error class that extends serializers.ValidationError. This
     inherits the functionality of parent class but overrides message.

     If an 'extra', a kwargs dict, arguments passed when an error is raised, then those
     arguments would be stored as dict in 'extra' key and overrides default message
     to a list of messages.

     Instance variables:
       * message
       * code
       * resolution
    """
    # description of an exception
    description = ''
    # The most probable cause for the error.
    message = ''
    code = 'validation_error'
    # Suggestions for resolving the error.
    resolution = ''

    # It avoids evaluation of message during initialization of urls/models by Django.
    # Message will be evaluated only when the exception is raised.
    @property
    def prop_message(self):
        return self.message

    @prop_message.setter
    def prop_message(self, msg):
        self.__dict__['message'] = msg

    def __init__(self, **extra):
        """Initialize with prop_message and a code, but override prop_message
        if 'extra' kwargs is passed
        """
        message_kwargs = extra.pop('message_kwargs', None)

        if message_kwargs:
            self.message = self.message.format(**message_kwargs)

        if extra:
            self.prop_message = [
                self.prop_message,
                {
                    'extra': extra,
                }
            ]
        super().__init__(self.prop_message, code=self.code)


class BaseRequiredValidationError(BaseValidationError, metaclass=ABCMeta):
    """Base class for required exceptions that extends BaseValidatorError class.
    It inherits the functionality of parent class and overrides message and code variables,
    to handle invalid fields.

    Instance variables:
       * message
       * code
    """
    field_name = ''
    message = _("Field '{field_name}' is required.")
    code = 'required'
    resolution = _('Provide the required field.')

    def __init__(self, **extra):
        # now replace field name with the actual field name defined in the derived class.
        # replace two spaces with single space when field name is not defined in the derived class.
        if '{field_name}' in self.message:
            self.message = self.message.format(field_name=self.field_name).replace('  ', ' ')
        super().__init__(**extra)


class RequiredMetadataActionFieldError(BaseRequiredValidationError):
    """
    RequiredMetadataActionFieldError
        An exception class that extends BaseRequiredValidationError. This exception should
        be raised when action field is not provided in OPTIONS.

        This class overrides 'code' variable. Override 'message' variable, if required.
    """
    field_name = 'action'
    description = _('Action is required to perform this action.')
    code = 'required_metadata_action'


class BaseInvalidValidationError(BaseValidationError, metaclass=ABCMeta):
    """Base class for invalid exceptions that extends BaseValidatorError class.
    It inherits the functionality of parent class and overrides message and code variables,
    to handle invalid fields.

    Instance variables:
       * message
       * code
    """
    field_name = ''
    message = _("Field '{field_name}' has an invalid value.")
    code = 'invalid'
    resolution = _('Check the supported value(s) of the field and provide a valid value.')

    def __init__(self, **extra):
        # now replace field name with the actual field name defined in the derived class.
        # replace two spaces with single space when field name is not defined in the derived class.
        if '{field_name}' in self.message:
            self.message = self.message.format(field_name=self.field_name).replace('  ', ' ')
        if not self.description:
            # If the exception doesn't define it's own description, we add a default description
            self.description = _(self.message)
        super().__init__(**extra)


class InvalidMetadataUnsupportedActionFieldError(BaseInvalidValidationError):
    """
    InvalidMetadataUnsupportedActionFieldError
        An exception class that extends BaseInvalidValidationError. This exception should
        be raised when provided action value is not supported.

        This class overrides 'code' variable. Override 'message' variable, if required.
    """
    field_name = 'action'
    description = _('Action value is not supported.')
    code = 'invalid_metadata_action'


class InvalidExportRecordsCountError(BaseInvalidValidationError):
    """
    InvalidExportRecordsCountError
        An exception class that extends BaseInvalidValidationError. This exception should be raised
        when the export records count is greater than the permissible limit
    """

    message = _('Exporting records more than permissible limit is not allowed')
    code = 'invalid_export_records_count'


class RequiredAPIFilterError(BaseRequiredValidationError):
    """An exception class that extends BaseRequiredValidationError. This exceptions should
     be raised when API value is not provided while downloading the Public API data.

     This class overrides both 'message' and 'code' variables.
    """
    message = _('A API value is required to download the API')
    description = _('A valid API ID is required in request body.')
    code = 'required_an_api_id_in_request_body'


class RequiredAPIKeyFilterError(BaseRequiredValidationError):
    """An exception class that extends BaseRequiredValidationError. This exceptions should
     be raised when API Key value is not provided while downloading the Public API data.

     This class overrides both 'message' and 'code' variables.
    """
    message = _('A API Key value is required to download the API')
    description = _('A 264 bit valid API Key is required in request body.')
    code = 'required_an_api_key_in_request_body'


class InvalidAPIKeyError(BaseInvalidValidationError):
    """
    InvalidAPIKeyError
        An exception class that extends BaseInvalidValidationError. This exception should be raised
        when the export records count is greater than the permissible limit
    """

    message = _('Invalid API Key provided')
    code = 'invalid_api_key_provided'


class BareException(Exception):
    pass
