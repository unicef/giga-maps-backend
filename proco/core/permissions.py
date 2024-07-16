from datetime import datetime, timedelta

from rest_framework import exceptions as rest_exceptions
from rest_framework import permissions

from proco.custom_auth.models import Role, RolePermission
from proco.utils.dates import to_datetime


class ProcoBasePermission(permissions.BasePermission):
    method = None
    permission_name = None

    def get_allowed_super_roles(self, request, view):
        return (Role.SYSTEM_ROLE_NAME_ADMIN,)

    def _is_super_allowed(self, request, view):
        allowed_super_roles = self.get_allowed_super_roles(request, view)
        user_roles = (request.user.get_roles().name,)
        roles = set(user_roles) & set(allowed_super_roles)
        return roles

    def check_permission(self, request, view):
        user_permissions = request.user.permissions
        return user_permissions.get(self.permission_name, False)

    def has_permission(self, request, view):
        if request.method != self.method:
            return True

        if request.user.is_anonymous:
            return False

        if request.user.is_staff or request.user.is_superuser:
            return True

        is_super_allowed = self._is_super_allowed(request, view)
        if is_super_allowed:
            return True

        return self.check_permission(request, view)


class IsRequestUserSameAsQueryParam(permissions.BasePermission):
    """
    IsRequestUserSameAsQueryParam
        Permission class to check user_id in url path and requesting user.id is same.
    """

    def has_permission(self, request, view):
        """
        has_permission
            Method to check the permission
        :Request Type: GET,PUT,POST,DELETE
        :returns:  bool: True if it has permission

        * Check if user_id in url path and requesting user.id is same
        """
        if view.kwargs.get('user_id'):
            return str(request.user.id) == view.kwargs.get('user_id')

        return True


class IsUserAuthenticated(permissions.BasePermission):
    """
    IsUserAuthenticated
       This permission checks if the user is authenticated to proco system.
    """

    def has_permission(self, request, view):
        if (
            not request.user or
            request.user.is_anonymous or
            request.user.deleted is not None or
            not request.user.is_active
        ):
            raise rest_exceptions.AuthenticationFailed()

        login_date = to_datetime(request.user.last_login, default=datetime.now())
        login_expiration_date = datetime.now() - timedelta(days=5)
        if login_date < login_expiration_date:
            e = rest_exceptions.AuthenticationFailed()
            raise e

        return True


class CanViewUsers(ProcoBasePermission):
    """
    CanViewUsers
        It create permission to check if current logged-in user has permission to view user/users.
    Inherits: permissions.BasePermission
    In this permission class we are overriding both methods - has_permission and has_object_permission
    """
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_USER


class CanAddUser(ProcoBasePermission):
    """
    CanAddUser
        It create permission to check if current logged-in user has permission to add a user.
    Inherits: permissions.BasePermission
    In this permission class we are overriding both methods - has_permission and has_object_permission
    """
    method = 'POST'
    permission_name = RolePermission.CAN_ADD_USER


class CanDeleteUser(ProcoBasePermission):
    """
    CanDeleteUser
        It create permission to check if current logged-in user has permission to delete a user.
    Inherits: permissions.BasePermission
    In this permission class we are overriding both methods - has_permission only.
    """
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_USER


class CanUpdateUser(ProcoBasePermission):
    """
    CanUpdateUser
        It create permission to check if current logged-in user has permission to update a user.
    Inherits: permissions.BasePermission
    In this permission class we are overriding both methods - has_permission only.
    """
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_USER

    def has_permission(self, request, view):
        if str(request.user.id) == view.kwargs.get('pk'):
            return True

        return super(CanUpdateUser, self).has_permission(request, view)


class IsUserEnabled(permissions.BasePermission):
    """
    IsUserEnabled
        This permission checks if a user is enabled or not.
    """

    def has_permission(self, request, view):
        """
        has_permission
            Method to check the permission
        :returns:  bool: True if it has permission
        * If it is enabled, return True, else return False
        """
        # check if user is active and deleted or not
        if request.user.deleted is not None or not request.user.is_active:
            return False
        return True


class IsSuperUserEnabledAuthenticated(ProcoBasePermission):
    """
    IsSuperUserEnabledAuthenticated
        This permission checks if a user is superuser or staff.
    """

    def has_permission(self, request, view):
        if (
            not request.user or
            request.user.is_anonymous or
            request.user.deleted is not None or
            not request.user.is_active
        ):
            raise rest_exceptions.AuthenticationFailed()

        login_date = to_datetime(request.user.last_login, default=datetime.now())
        login_expiration_date = datetime.now() - timedelta(days=5)
        if login_date < login_expiration_date:
            raise rest_exceptions.AuthenticationFailed()

        if request.user.is_staff or request.user.is_superuser:
            return True

        is_super_allowed = self._is_super_allowed(request, view)
        if is_super_allowed:
            return True

        return False


class CanViewAllRoles(ProcoBasePermission):
    """
    CanViewAllRoles
        This permission is to check if current logged-in user has permission to view All Roles.
        Inherits: ProcoBasePermission
    """
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_ALL_ROLES


class CanCreateRole(ProcoBasePermission):
    """
    CanCreateRole
        This permission is to check if current logged-in user has permission to create role.
        Inherits: ProcoBasePermission
    """
    method = 'POST'
    permission_name = RolePermission.CAN_CREATE_ROLE_CONFIGURATIONS


class CanUpdateRole(ProcoBasePermission):
    """
    CanUpdateRole
        This permission is to check if current logged-in user has permission to update role.
        Inherits: ProcoBasePermission
    """
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_ROLE_CONFIGURATIONS


class CanDeleteRole(ProcoBasePermission):
    """
    CanDeleteRole
        This permission is to check if current logged-in user has permission to delete role.
        Inherits: ProcoBasePermission
    """
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_ROLE_CONFIGURATIONS


class CanUpdateUserRole(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_USER_ROLE


class CanApproveRejectAPIKeyorAPIKeyExtension(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_APPROVE_REJECT_API_KEY


class CanDeleteAPIKey(ProcoBasePermission):
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_API_KEY


class CanViewMessages(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_NOTIFICATION


class CanSendMessages(ProcoBasePermission):
    """
    CanSendMessages
        This permission is to check if current logged-in user has permission to send notification.
        Inherits: ProcoBasePermission
    """
    method = 'POST'
    permission_name = RolePermission.CAN_CREATE_NOTIFICATION


class CanDeleteMessages(ProcoBasePermission):
    """
    CanDeleteMessages
        This permission is to check if current logged-in user has permission to delete notification.
        Inherits: ProcoBasePermission
    """
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_NOTIFICATION


class CanCleanCache(ProcoBasePermission):
    method = 'GET'
    permission_name = None

    def has_permission(self, request, view):
        if request.method != self.method:
            return True

        is_super_allowed = self._is_super_allowed(request, view)
        if is_super_allowed:
            return True

        if request.user.is_staff or request.user.is_superuser:
            return True

        return False


class CanViewDataLayer(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_DATA_LAYER


class CanAddDataLayer(ProcoBasePermission):
    method = 'POST'
    permission_name = RolePermission.CAN_ADD_DATA_LAYER


class CanDeleteDataLayer(ProcoBasePermission):
    method = 'DELETE'
    permission_name = None

    def has_permission(self, request, view):
        if request.method != self.method:
            return True

        is_super_allowed = self._is_super_allowed(request, view)
        if is_super_allowed:
            return True

        if request.user.is_staff or request.user.is_superuser:
            return True

        return False


class CanUpdateDataLayer(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_DATA_LAYER


class CanPublishDataLayer(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_PUBLISH_DATA_LAYER


class CanUpdateSchool(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_SCHOOL


class CanDeleteSchool(ProcoBasePermission):
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_SCHOOL


class CanAddSchool(ProcoBasePermission):
    method = 'POST'
    permission_name = RolePermission.CAN_ADD_SCHOOL


class CanViewSchool(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_SCHOOL


class CanImportCSV(ProcoBasePermission):
    method = 'POST'
    permission_name = RolePermission.CAN_IMPORT_CSV


class CanDeleteCSV(ProcoBasePermission):
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_CSV


class CanViewCSV(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_UPLOADED_CSV


class CanUpdateCountry(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_COUNTRY


class CanAddCountry(ProcoBasePermission):
    method = 'POST'
    permission_name = RolePermission.CAN_ADD_COUNTRY


class CanDeleteCountry(ProcoBasePermission):
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_COUNTRY


class CanViewCountry(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_COUNTRY


class CanPreviewDataLayer(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_PREVIEW_DATA_LAYER


class CanViewSchoolMasterData(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_SCHOOL_MASTER_DATA


class CanDeleteSchoolMasterData(ProcoBasePermission):
    method = 'DELETE'
    permission_name = RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA


class CanUpdateSchoolMasterData(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA


class CanPublishSchoolMasterData(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA


class CanViewBackgroundTask(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_BACKGROUND_TASK


class CanAddBackgroundTask(ProcoBasePermission):
    method = 'POST'
    permission_name = RolePermission.CAN_ADD_BACKGROUND_TASK


class CanUpdateBackgroundTask(ProcoBasePermission):
    method = 'PUT'
    permission_name = RolePermission.CAN_UPDATE_BACKGROUND_TASK


class CanDeleteBackgroundTask(ProcoBasePermission):
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_BACKGROUND_TASK


class CanViewRecentAction(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_RECENT_ACTIONS


class CanViewContactMessage(ProcoBasePermission):
    method = 'GET'
    permission_name = RolePermission.CAN_VIEW_CONTACT_MESSAGE


class CanDeleteContactMessage(ProcoBasePermission):
    method = 'DELETE'
    permission_name = RolePermission.CAN_DELETE_CONTACT_MESSAGE
