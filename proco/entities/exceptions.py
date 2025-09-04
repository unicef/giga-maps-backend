from django.utils.translation import ugettext as _

from proco.core.exceptions import BaseInvalidValidationError


class DuplicateEntityFieldValueError(BaseInvalidValidationError):
    message = _("Entity with {name} '{value}' already exists.")
    code = 'duplicate_entity_field_value'


class InvalidEntityFieldValueError(BaseInvalidValidationError):
    message = _("Field '{name}' has an invalid value.")
    code = 'invalid_entity_field_value'

