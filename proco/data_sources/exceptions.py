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


class InvalidSchoolMasterDataRowStatusAtUpdateError(BaseInvalidValidationError):
    message = _('Invalid School Master Data row status at update: DB status: "{old}", Requested status: "{new}"')
    # description = _('"PUBLISHED" school master data row can not be updated.')
    code = 'invalid_school_master_data_row_status'


class InvalidSchoolMasterDataRowStatusError(BaseInvalidValidationError):
    message = _('Invalid School Master Data row status at publish: DB status: "{old}", Requested status: "{new}"')
    description = _('Only "DRAFT" or "UPDATED_IN_DRAFT" status is allowed at publish.')
    code = 'invalid_school_master_data_row_status'


class ZeroSchoolMasterDataRowError(BaseInvalidValidationError):
    message = _('Zero School Master Data row to update.')
    description = _('Zero rows to update.')
    code = 'invalid_school_master_data_row_count'

