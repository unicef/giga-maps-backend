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


class EmailAlreadyExistsError(BaseInvalidValidationError):
    """
    EmailAlreadyExistsError
        An exception class that extends BaseInvalidValidationError. This exception should
        be raised when the email provided is already existing

        This class overrides 'code' variable. Override 'message' variable, if required.
    """
    message = _('The entered email address already exists.')
    description = _('The entered email address already exists.')
    field_name = 'email'
    code = 'email_already_exists'


class CannotAssignCustomRoleToSuperuserUser(BaseInvalidValidationError):
    """
    CannotAssignCustomRoleToSharedUser
        An exception class that extends BaseInvalidValidationError. This exception should
        be raised when a custom role is being assigned to a shared user.

        This class overrides 'code' variable. Override 'message' variable, if required.
    """
    message = _('You can not assign a custom role to this user as this user is a superuser.')
    field_name = 'role'
    code = 'cannot_assign_custom_role_to_superuser'


class InvalidRoleError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when invalid role is specified

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Provided role is invalid.')
    resolution = _('Enter valid role id.')
    code = 'invalid_role'


class UserLoginError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should be raised
    when user tried to login with invalid credentials .
    This class overrides both 'message', 'description', 'resolution' and 'code' variables.
    """
    message = _('Unable to log in with provided credentials.')
    description = _('Unable to log in with provided credentials.')
    resolution = _('Please provide valid login credentials.')
    code = 'login_failed'


class InvalidRoleNameError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when role name is not valid.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Invalid Role name')
    description = _('Provide valid role name')
    code = 'invalid_role_name'


class InvalidCustomRoleCountError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when number of custom role exceeds max role count.

    This class overrides both 'message' and 'code' variables.
    """
    message = _('You can create up-to {limit} custom roles.')
    description = _('Delete an existing role in order to create a new one')
    code = 'invalid_custom_role_count'


class DuplicateRoleNameError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when role name already exists.

    This class overrides both 'message' and 'code' variables.
    """
    message = _("Role with '{role}' already exists.")
    code = 'duplicate_role_name'


class InvalidRoleDataError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when reference role is not valid

    This class overrides both 'message' and 'code' variables.
    """
    message = _('Invalid reference role')
    description = _('Provide valid reference role')
    code = 'invalid_reference_role'


class InvalidSystemRoleDeleteError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when role can't be deleted.

    This class overrides both 'message' and 'code' variables.
    """
    message = _("System Role can't be deleted")
    description = _("You can't delete system role")
    code = 'invalid_role_delete'


class InvalidRoleDeleteError(BaseInvalidValidationError):
    """
    An exception class that extends BaseInvalidValidationError. This exception should
    be raised when role can't be deleted.

    This class overrides both 'message' and 'code' variables.
    """
    message = _("Role {role} can't be deleted as it is associated to a user(s). Please dissociate"
                " this role from all users in order to delete it")
    code = 'invalid_role_delete'
