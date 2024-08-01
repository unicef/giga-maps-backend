from django.contrib.auth.models import AbstractBaseUser
from django.core import validators
from django.db import models
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _
from simple_history.models import HistoricalRecords

from proco.core import models as core_models
from proco.custom_auth.managers import CustomUserManager
from proco.locations.models import Country


class ApplicationUser(core_models.BaseModelMixin, AbstractBaseUser):
    email = models.EmailField(_('email address'), blank=True, null=True, unique=True)
    first_name = models.CharField(max_length=100, verbose_name='First Name')
    last_name = models.CharField(max_length=100, verbose_name='Last Name')

    date_joined = models.DateTimeField(default=timezone.now, verbose_name='Date Joined')

    is_active = models.BooleanField(default=True, verbose_name='Active')
    is_superuser = models.BooleanField(default=False)

    username = models.CharField(
        _('username'),
        max_length=254,
        unique=True,
        help_text=_('Required. 254 characters or fewer. Letters, digits and @/./+/-/_ only.'),
        validators=[
            validators.RegexValidator(
                r'^[\w.@+-]+$',
                _('Enter a valid username. This value may contain only letters, numbers and @/./+/-/_ characters.'),
            ),
        ],
        error_messages={
            'unique': _('A user with that username already exists.'),
        },
    )

    is_staff = models.BooleanField(
        _('staff status'),
        default=False,
        help_text=_('Designates whether the user can log into this admin site.'),
    )

    countries_available = models.ManyToManyField(
        Country,
        verbose_name=_('Countries Available'),
        blank=True,
        help_text=_('Countries to which the user has access and the ability to manage them.'),
        related_name='countries_available',
    )

    history = HistoricalRecords(inherit=True, excluded_fields=['password'])

    USERNAME_FIELD = 'username'
    REQUIRED_FIELDS = ['email']

    objects = CustomUserManager()

    @property
    def permissions(self):
        return self.get_user_permissions(self)

    @property
    def user_name(self):
        full_name = '{0} {1}'.format(self.first_name, self.last_name)
        return full_name.strip()

    @property
    def role_verbose_name(self):
        role_relationship = self.roles.first()
        if role_relationship is not None:
            return role_relationship.role.name
        return None

    def role(self) -> models.CharField:
        return self.get_roles().name

    def has_perm(self, perm, obj=None):
        return self.is_superuser

    def has_module_perms(self, app_label):
        return self.is_superuser

    def get_roles(self):
        first_role = self.roles.first()
        if first_role:
            return first_role.role

    def get_user_permissions(self, user):
        """
        get_user_permissions
            Helper method to retrieve all user permissions.
            :param user: user of which permissions need to determine
            :type user: custom_auth.models.User
            :returns perms: Dictionary of permissions
        """
        return self.calculate_user_permissions(user)

    def calculate_user_permissions(self, user):
        """
        calculate_user_permissions
            Helper method to calculate user's permissions based on role.
            :param user: user of which permissions need to determine
            :type user: custom_auth.models.User
            :returns all_permissions: Dictionary of permissions
        """
        user_permissions = {}
        for user_role_relations in user.roles.all():
            user_permissions.update(user_role_relations.perm_dict())

        return user_permissions

    def save(self, *args, **kwargs):
        if not self.email:
            # Unique constraint doesn't work correctly with empty string. So we need to forcibly set email to None.
            self.email = None

        super(ApplicationUser, self).save(*args, **kwargs)

    def get_full_name(self):
        """
        Returns the first_name plus the last_name, with a space in between.
        """
        full_name = '{0} {1}'.format(self.first_name, self.last_name)
        return full_name.strip()

    def get_short_name(self):
        """
        Returns the short name for the user.
        """
        return self.first_name


class Role(core_models.BaseModel):
    """ Role
            Defines the model used to store the User's roles.
        Inherits : `BaseModel`
    """

    ROLE_CATEGORY_SYSTEM = 'system'
    ROLE_CATEGORY_CUSTOM = 'custom'
    ROLE_CATEGORY_CHOICES = (
        (ROLE_CATEGORY_SYSTEM, 'System Role'),
        (ROLE_CATEGORY_CUSTOM, 'Custom Role'),
    )

    SYSTEM_ROLE_NAME_ADMIN = 'Admin'
    SYSTEM_ROLE_NAME_READ_ONLY = 'Read Only'

    SYSTEM_ROLE_NAME_CHOICES = (
        (SYSTEM_ROLE_NAME_ADMIN, 'Admin'),
        (SYSTEM_ROLE_NAME_READ_ONLY, 'Read Only User'),
    )

    name = models.CharField(
        max_length=255,
        null=False,
        verbose_name='Role',
        db_index=True,
    )
    description = models.CharField(max_length=255, null=True, blank=True)
    category = models.CharField(
        max_length=50,
        choices=ROLE_CATEGORY_CHOICES,
        verbose_name='Role',
        default=ROLE_CATEGORY_SYSTEM,
    )

    @property
    def permission_slugs(self):
        return self.permissions.all().values_list('slug', flat=True)


class UserRoleRelationship(core_models.BaseModelMixin):
    """
    UserRoleRelationship
        This model is used to store the user roles.
    """
    user = models.ForeignKey(ApplicationUser, related_name='roles', on_delete=models.DO_NOTHING)
    role = models.ForeignKey(Role, related_name='role_users', on_delete=models.DO_NOTHING)

    def perm_dict(self):
        role_perms = {
            perm_slug: True for perm_slug in self.role.permissions.values_list(
                'slug', flat=True
            )
        }
        return role_perms


class RolePermission(core_models.BaseModelMixin):
    """
    RolePermission

        These model objects are used to map `Role`s to the permissions they have.

    """

    CAN_ACCESS_USER_MANAGEMENT_TAB = 'can_access_users_tab'

    CAN_VIEW_USER = 'can_view_user'
    CAN_ADD_USER = 'can_add_user'
    CAN_DELETE_USER = 'can_delete_user'
    CAN_UPDATE_USER = 'can_update_user'

    CAN_VIEW_ALL_ROLES = 'can_view_all_roles'
    CAN_UPDATE_USER_ROLE = 'can_update_user_role'

    CAN_CREATE_ROLE_CONFIGURATIONS = 'can_create_role_configurations'
    CAN_UPDATE_ROLE_CONFIGURATIONS = 'can_update_role_configurations'
    CAN_DELETE_ROLE_CONFIGURATIONS = 'can_delete_role_configurations'

    USER_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_ACCESS_USER_MANAGEMENT_TAB, 'Can Access User Management Tab'),
        (CAN_VIEW_USER, 'Can View User'),
        (CAN_ADD_USER, 'Can Add User'),
        (CAN_UPDATE_USER, 'Can Update a User'),
        (CAN_DELETE_USER, 'Can Delete a User'),
        (CAN_VIEW_ALL_ROLES, 'Can View All Roles'),
        (CAN_UPDATE_USER_ROLE, 'Can Update User Role'),
        (CAN_CREATE_ROLE_CONFIGURATIONS, 'Can Create Role Configurations'),
        (CAN_UPDATE_ROLE_CONFIGURATIONS, 'Can Update Role Configurations'),
        (CAN_DELETE_ROLE_CONFIGURATIONS, 'Can Delete Role Configurations'),
    )

    CAN_VIEW_DATA_LAYER = 'can_view_data_layer'
    CAN_ADD_DATA_LAYER = 'can_add_data_layer'
    CAN_UPDATE_DATA_LAYER = 'can_update_data_layer'
    CAN_PUBLISH_DATA_LAYER = 'can_publish_data_layer'
    CAN_PREVIEW_DATA_LAYER = 'can_preview_data_layer'

    DATA_LAYER_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_DATA_LAYER, 'Can View Data Layer'),
        (CAN_ADD_DATA_LAYER, 'Can Add Data Layer'),
        (CAN_UPDATE_DATA_LAYER, 'Can Update Data Layer'),
        (CAN_PUBLISH_DATA_LAYER, 'Can Publish Data Layer'),
        (CAN_PREVIEW_DATA_LAYER, 'Can Preview Data Layer'),
    )

    CAN_VIEW_SCHOOL_MASTER_DATA = 'can_view_school_master_data'
    CAN_UPDATE_SCHOOL_MASTER_DATA = 'can_update_school_master_data'
    CAN_PUBLISH_SCHOOL_MASTER_DATA = 'can_publish_school_master_data'

    SCHOOL_MASTER_DATA_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_SCHOOL_MASTER_DATA, 'Can View School Master Data'),
        (CAN_UPDATE_SCHOOL_MASTER_DATA, 'Can Update School Master Data'),
        (CAN_PUBLISH_SCHOOL_MASTER_DATA, 'Can Publish School Master Data'),
    )

    CAN_DELETE_API_KEY = 'can_delete_api_key'
    CAN_APPROVE_REJECT_API_KEY = 'can_approve_reject_api_key'

    API_KEY_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_DELETE_API_KEY, 'Can Delete API Key'),
        (CAN_APPROVE_REJECT_API_KEY, 'Can Approve/Reject API Key or API key extension request'),
    )

    CAN_VIEW_COUNTRY = 'can_view_country'
    CAN_ADD_COUNTRY = 'can_add_country'
    CAN_UPDATE_COUNTRY = 'can_update_country'
    CAN_DELETE_COUNTRY = 'can_delete_country'

    COUNTRY_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_COUNTRY, 'Can View Country'),
        (CAN_ADD_COUNTRY, 'Can Add a Country'),
        (CAN_UPDATE_COUNTRY, 'Can Update a Country'),
        (CAN_DELETE_COUNTRY, 'Can Delete a Country'),
    )

    CAN_VIEW_SCHOOL = 'can_view_school'
    CAN_ADD_SCHOOL = 'can_add_school'
    CAN_UPDATE_SCHOOL = 'can_update_school'
    CAN_DELETE_SCHOOL = 'can_delete_school'

    SCHOOL_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_SCHOOL, 'Can View School'),
        (CAN_ADD_SCHOOL, 'Can Add a School'),
        (CAN_UPDATE_SCHOOL, 'Can Update a School'),
        (CAN_DELETE_SCHOOL, 'Can Delete a School'),
    )

    CAN_VIEW_UPLOADED_CSV = 'can_view_uploaded_csv'
    CAN_IMPORT_CSV = 'can_import_csv'
    CAN_DELETE_CSV = 'can_delete_csv'

    CSV_FILE_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_UPLOADED_CSV, 'Can View Uploaded CSV'),
        (CAN_IMPORT_CSV, 'Can Import a CSV'),
        (CAN_DELETE_CSV, 'Can Delete a CSV'),
    )

    CAN_VIEW_BACKGROUND_TASK = 'can_view_background_task'
    CAN_ADD_BACKGROUND_TASK = 'can_add_background_task'
    CAN_UPDATE_BACKGROUND_TASK = 'can_update_background_task'
    CAN_DELETE_BACKGROUND_TASK = 'can_delete_background_task'

    BACKGROUND_TASK_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_BACKGROUND_TASK, 'Can View Background Task'),
        (CAN_ADD_BACKGROUND_TASK, 'Can Add a Background Task'),
        (CAN_UPDATE_BACKGROUND_TASK, 'Can Update a Background Task'),
        (CAN_DELETE_BACKGROUND_TASK, 'Can Delete a Background Task'),
    )

    CAN_VIEW_CONTACT_MESSAGE = 'can_view_contact_message'
    CAN_UPDATE_CONTACT_MESSAGE = 'can_update_contact_message'
    CAN_DELETE_CONTACT_MESSAGE = 'can_delete_contact_message'

    CONTACT_MESSAGE_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_CONTACT_MESSAGE, 'Can View Contact Message'),
        (CAN_UPDATE_CONTACT_MESSAGE, 'Can Update a Contact Message'),
        (CAN_DELETE_CONTACT_MESSAGE, 'Can Delete a Contact Message'),
    )

    CAN_VIEW_RECENT_ACTIONS = 'can_view_recent_actions'

    RECENT_ACTIONS_MANAGEMENT_PERMISSION_CHOICES = (
        (CAN_VIEW_RECENT_ACTIONS, 'Can View Recent Actions'),
    )

    CAN_VIEW_NOTIFICATION = 'can_view_notification'
    CAN_CREATE_NOTIFICATION = 'can_create_notification'
    CAN_DELETE_NOTIFICATION = 'can_delete_notification'

    NOTIFICATION_PERMISSION_CHOICES = (
        (CAN_VIEW_NOTIFICATION, 'Can View Notification'),
        (CAN_CREATE_NOTIFICATION, 'Can Create Notification'),
        (CAN_DELETE_NOTIFICATION, 'Can Delete Notification'),
    )

    PERMISSION_CHOICES = (
        USER_MANAGEMENT_PERMISSION_CHOICES
        + DATA_LAYER_MANAGEMENT_PERMISSION_CHOICES
        + SCHOOL_MASTER_DATA_MANAGEMENT_PERMISSION_CHOICES
        + API_KEY_MANAGEMENT_PERMISSION_CHOICES
        + COUNTRY_MANAGEMENT_PERMISSION_CHOICES
        + SCHOOL_MANAGEMENT_PERMISSION_CHOICES
        + CSV_FILE_MANAGEMENT_PERMISSION_CHOICES
        + BACKGROUND_TASK_MANAGEMENT_PERMISSION_CHOICES
        + CONTACT_MESSAGE_MANAGEMENT_PERMISSION_CHOICES
        + RECENT_ACTIONS_MANAGEMENT_PERMISSION_CHOICES
        + NOTIFICATION_PERMISSION_CHOICES
    )

    role = models.ForeignKey(Role, related_name='permissions', on_delete=models.DO_NOTHING)
    slug = models.CharField(
        max_length=100,
        choices=PERMISSION_CHOICES,
        db_index=True,
    )

    def perm_dict(self):
        role_perms = {
            perm_slug: True for perm_slug in self.role.permissions.values_list(
                'slug', flat=True
            )
        }
        return role_perms
