from abc import ABCMeta

from django.utils.translation import ugettext as _

from proco.core.exceptions import BaseValidationError


class BaseInvalidValidationError(BaseValidationError, metaclass=ABCMeta):
    """
    Base class for invalid exceptions that extends BaseValidatorError class.
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


class InvalidAPIKeyNameError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when API key name is not valid.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Invalid API Key name')
    description = _('Provide valid API Key name')
    code = 'invalid_api_key_name'


class InvalidActiveAPIKeyCountForSingleAPIError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when number of active API keys for 1 API is more than one.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('You can create up-to {limit} Active API Key for one API. 1 key for same API already Exists: {details}')
    description = _('New key can be created once existing one expires.')
    code = 'invalid_active_api_key_count_for_one_api'


class InvalidAPIKeyFiltersError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when API key filters are not valid.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Invalid API Key filters')
    description = _('Provide valid API Key filters')
    code = 'invalid_api_key_filters'


class CountryRequiredForPrivateAPIKeyError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when private API key is created without country list.

    This class overrides both 'message' and 'code' variables.
    """
    message = _("'active_countries_list' field is required for Private API Key.'")
    description = _('Provide valid country list for Private API Key')
    code = 'invalid_private_api_key_countries'


class InvalidAPIKeyStatusForPublicAPIError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when API key status is not APPROVED when it's a Public API.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Invalid API Key Status for Public API')
    description = _('Only APPROVED API key status is allowed for Public API')
    code = 'invalid_api_key_status'


class InvalidAPIKeyStatusForPrivateAPIError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when API key status is not INITIATED when it's a Private API.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Invalid API Key Status for Private API')
    description = _('Only INITIATED API key status is allowed for Private API')
    code = 'invalid_api_key_status'


class InvalidAPIError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when invalid API id is specified

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Provided API is invalid.')
    resolution = _('Enter valid API id.')
    code = 'invalid_api'


class InvalidAPIKeyExtensionStatusError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when API key extension status is not valid.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Invalid API Key Extension Status')
    description = _('API Key extension can be provided for an ACTIVE API Key')
    code = 'invalid_api_key_extension_status'


class InvalidAPIKeyExtensionError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when API key extension status is not valid.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('{msg}')
    description = _('Invalid API Key Extension Request')
    code = 'invalid_api_key_extension_request'


class InvalidEmailId(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exceptions should be raised
    when request user tried to enter invalid email id
    This class overrides both 'message' and 'code' variables.
    """
    message = _('Email address is incorrect')
    description = _('Invalid Email id provided in payload.')
    code = 'invalid_email_id'
    field_name = 'email'


class InvalidPhoneNumberError(BaseInvalidValidationError):
    """
    InvalidPhoneNumberError
        An exception class that extends BaseInvalidValidationError.
        This class overrides 'message',
        'code' variables.
    """
    message = _('An invalid phone number was entered.')
    code = 'invalid_phone_number'


class InvalidUserIdError(BaseInvalidValidationError):
    """
    InvalidUserIdError
        An exception class that extends BaseInvalidValidationError.
        This class overrides 'message',
        'code' variables.
    """
    message = _('An invalid user id was entered.')
    code = 'invalid_user_id'


class InvalidDataSourceStatusError(BaseInvalidValidationError):
    message = _('Invalid Data Source Status at creation.')
    description = _('Only "DRAFT" or "READY_TO_PUBLISH" status is allowed at creation.')
    code = 'invalid_data_source_status'


class InvalidDataSourceStatusUpdateError(BaseInvalidValidationError):
    message = _('Invalid Data Source Status at update.')
    code = 'invalid_data_source_status'


class InvalidDataSourceRequestConfigError(BaseInvalidValidationError):
    message = _('Invalid Data Source Request Configurations.')
    description = _('"url" and "method" are required parameters in data source request config object.')
    code = 'invalid_data_source_request_config'


class InvalidDataSourceColumnConfigError(BaseInvalidValidationError):
    message = _('Invalid Data Source Column Configurations.')
    description = _('"name" and "type" are required parameters in data source column config object.')
    code = 'invalid_data_source_column_config'


class InvalidDataSourceNameError(BaseInvalidValidationError):
    message = _('Invalid Data Source name.')
    description = _('Provide valid data source name')
    code = 'invalid_data_source_name'


class DuplicateDataSourceNameError(BaseInvalidValidationError):
    message = _("Data Source with name '{name}' already exists.")
    code = 'duplicate_data_source_name'


class InvalidDataLayerStatusError(BaseInvalidValidationError):
    message = _('Invalid Data Layer Status at creation.')
    description = _('Only "DRAFT" or "READY_TO_PUBLISH" status is allowed at creation.')
    code = 'invalid_data_layer_status'


class InvalidDataLayerStatusUpdateError(BaseInvalidValidationError):
    message = _('Invalid Data Layer Status at update.')
    code = 'invalid_data_layer_status'


class InvalidDataLayerNameError(BaseInvalidValidationError):
    message = _('Invalid Data Layer name.')
    description = _('Provide valid data layer name')
    code = 'invalid_data_layer_name'


class DuplicateDataLayerNameError(BaseInvalidValidationError):
    message = _("Data Layer with name '{name}' already exists.")
    code = 'duplicate_data_layer_name'


class InvalidCountryNameOrCodeError(BaseInvalidValidationError):
    message = _('Invalid Data Layer applicable country name/code provided.')
    description = _('Provide valid data layer applicable country configuration')
    code = 'invalid_data_layer_applicable_countries'


class InvalidDataSourceForDataLayerError(BaseInvalidValidationError):
    message = _('Invalid Data Source.')
    description = _('Provide valid data source')
    code = 'invalid_data_source'


class InvalidDataSourceColumnForDataLayerError(BaseInvalidValidationError):
    message = _('Invalid Data Source Column.')
    description = _('Provide valid data source column')
    code = 'invalid_data_source_column'


class InvalidUserOnDataLayerUpdateError(BaseInvalidValidationError):
    message = _('Editor can update only self created data layers.')
    description = _('Editor only able to edit the layer created by him/her. Editor can only view the layer created by '
                    'others.')
    code = 'invalid_user_on_data_layer_update'


class DummyDataLayerError(BaseInvalidValidationError):
    message = _("Dummy Error.")
    code = 'dummy_data_layer_error'
