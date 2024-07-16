from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.filters import SearchFilter

from proco.core import permissions as core_permissions
from proco.core.viewsets import BaseModelViewSet
from proco.custom_auth import exceptions as auth_exceptions
from proco.custom_auth import models as auth_models
from proco.custom_auth import serializers
from proco.utils.filters import NullsAlwaysLastOrderingFilter


class BaseUserViewSet(BaseModelViewSet):
    """
    BaseUserViewSet
        This class combines the logic of CRUD operations for users. Only permitted users can
        perform respective operations.
    Inherits: BaseModelViewSet
    """
    model = auth_models.ApplicationUser

    permission_classes = (
        core_permissions.IsRequestUserSameAsQueryParam,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    apply_query_pagination = True


class UserViewSet(BaseUserViewSet):
    """
    UserViewSet
        This class combines the logic of CRUD operations for users. Only permitted users can
        perform respective operations.
    Inherits: BaseUserViewSet
    """

    serializer_class = serializers.UserListSerializer
    action_serializers = {
        'create': serializers.CreateUserSerializer,
        'partial_update': serializers.UpdateUserSerializer,
    }

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
        SearchFilter,
    )

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.IsRequestUserSameAsQueryParam,
        core_permissions.CanViewUsers,
        core_permissions.CanAddUser,
        core_permissions.CanUpdateUser,
        core_permissions.CanDeleteUser,
    )

    ordering_fields = ('first_name', 'last_name', 'username', 'email', 'last_modified_at',)
    filterset_fields = {
        'first_name': ['iexact', 'in', 'icontains', 'contains'],
        'last_name': ['iexact', 'in', 'icontains', 'contains'],
        'username': ['iexact', 'in', 'icontains', 'contains'],
        'email': ['iexact', 'in', 'icontains', 'contains'],
    }

    search_fields = ('first_name', 'last_name', 'email')


class UserDetailsViewSet(BaseUserViewSet):
    """
    UserDetailsViewSet
        This class combines the logic of CRUD operations for self user details.

        Inherits: BaseUserViewSet
    """
    serializer_class = serializers.UserListSerializer
    action_serializers = {
        'create': serializers.CreateUserSerializer,
        'partial_update': serializers.UpdateUserSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.IsRequestUserSameAsQueryParam,
    )

    def retrieve(self, request, *args, **kwargs):
        self.kwargs['pk'] = request.user.id
        return super().retrieve(request, *args, **kwargs)


class RoleViewSet(BaseModelViewSet):
    """
    RoleViewSet
        This class is used to list all roles.
        Inherits: BaseModelViewSet
    """
    model = auth_models.Role
    serializer_class = serializers.RoleSerializer

    action_serializers = {
        'create': serializers.CreateRoleSerializer,
        'partial_update': serializers.UpdateRoleSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewAllRoles,
        core_permissions.CanCreateRole,
        core_permissions.CanUpdateRole,
        core_permissions.CanDeleteRole,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_fields = ('name', 'category', 'last_modified_at',)
    apply_query_pagination = True

    filterset_fields = {
        'name': ['iexact', 'in', 'icontains'],
        'category': ['iexact', 'in'],
    }

    def perform_destroy(self, instance):
        """
        perform_destroy
        :param instance: Role
        :return:
        """
        if instance.category == auth_models.Role.ROLE_CATEGORY_SYSTEM:
            # SYSTEM role can not be deleted
            raise auth_exceptions.InvalidSystemRoleDeleteError(role=instance.name)

        role_user_relationship = auth_models.UserRoleRelationship.objects.filter(role=instance).exists()
        if role_user_relationship:
            # If custom role linked with user/s, then it can not be deleted
            raise auth_exceptions.InvalidRoleDeleteError(
                role=instance.name,
                message_kwargs={'role': instance.name},
            )
        # Perform soft deletion
        auth_models.RolePermission.objects.filter(role=instance.id).delete()
        super().perform_destroy(instance)
