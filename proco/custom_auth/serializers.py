import re
import logging

from django.db import transaction
from rest_flex_fields.serializers import FlexFieldsModelSerializer
from rest_framework import serializers

from proco.core import utils as core_utilities
from proco.custom_auth import exceptions as auth_exceptions
from proco.custom_auth import models as auth_models
from proco.custom_auth import utils as auth_utilities
from proco.custom_auth.config import app_config as auth_config

logger = logging.getLogger('gigamaps.' + __name__)


class RoleSerializer(serializers.ModelSerializer):
    """
    RoleSerializer
        Serializer to list all roles.
    """

    class Meta:
        model = auth_models.Role
        read_only_fields = fields = (
            'id',
            'name',
            'category',
            'description',
            'created',
            'last_modified_at',
            'permission_slugs',
        )


class RolePermissionSerializer(serializers.ModelSerializer):
    """
    RolePermissionSerializer
    """

    class Meta:
        model = auth_models.RolePermission
        fields = (
            'id',
            'role',
            'slug',
        )


class BaseRoleCRUDSerializer(serializers.ModelSerializer):
    """
    BaseRoleCRUDSerializer
        This Serializer is used in CRUD validation
    """

    permission_slugs = serializers.ListSerializer(
        write_only=True,
        required=True,
        child=serializers.ChoiceField(auth_models.RolePermission.PERMISSION_CHOICES),
    )

    def get_user_role(self):
        request_user = core_utilities.get_current_user(context=self.context)
        if request_user:
            return request_user.get_roles()
        return None

    def validate_name(self, name):
        if re.match(r'[a-zA-Z0-9-\' _()]*$', name):
            return name
        raise auth_exceptions.InvalidRoleNameError()

    def _validate_custom_role_count_error(self):
        max_role_count = auth_config.custom_role_count_limit

        custom_role = auth_models.Role.objects.filter(category='custom')

        if len(custom_role) >= max_role_count:
            message_kwargs = {'limit': max_role_count}
            raise auth_exceptions.InvalidCustomRoleCountError(message_kwargs=message_kwargs)
        return True

    def _validate_unique_role_name(self, data):
        system_roles = dict(auth_models.Role.SYSTEM_ROLE_NAME_CHOICES).values()
        role_names = [role.lower() for role in system_roles]
        message_kwargs = {'role': data['name']}
        if data['name'].lower() in role_names:
            raise auth_exceptions.DuplicateRoleNameError(message_kwargs=message_kwargs)
        elif auth_models.Role.objects.filter(name=data['name']).exists():
            raise auth_exceptions.DuplicateRoleNameError(message_kwargs=message_kwargs)
        return True

    def validate(self, data):
        # Incase if user is doing a role copy
        reference_role_name = self.initial_data.get('reference_role')
        if not core_utilities.is_blank_string(reference_role_name):
            # INFO: Copy can only be created of a System role
            reference_role = auth_models.Role.objects.filter(
                name=reference_role_name,
                category=auth_models.Role.ROLE_CATEGORY_SYSTEM,
            ).first()
            if not reference_role:
                raise auth_exceptions.InvalidRoleDataError()

            # If reference role is provided then copy all the permissions as well
            data['permissions'] = reference_role.permissions

        self._validate_custom_role_count_error()
        self._validate_unique_role_name(data)
        return data

    def create(self, validated_data):
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            # set created_by and last_modified_by value
            if request_user is not None:
                validated_data['created_by'] = validated_data.get('created_by') or request_user
                validated_data['last_modified_by'] = validated_data.get(
                    'last_modified_by',
                ) or request_user
            instance = super().create(validated_data)
            return instance


class CreateRoleSerializer(BaseRoleCRUDSerializer):
    """
    CreateRoleSerializer
        Serializer to create role.
    """

    class Meta:
        model = auth_models.Role

        fields = (
            'id',
            'name',
            'category',
            'description',
            'permission_slugs',
        )

    def to_internal_value(self, data):
        """
        to_internal_value
            Add fields in initial data for Role instance.
        :param data:
        :return:
        """
        if not data:
            data = {}
        else:
            data['category'] = auth_models.Role.ROLE_CATEGORY_CUSTOM
        return super().to_internal_value(data)

    def create(self, validated_data):
        """
        create
            This method is used to create Role
        :param validated_data:
        :return:
        """
        permission_slugs = validated_data.pop('permission_slugs', [])
        role = super().create(validated_data)

        if len(permission_slugs) > 0:
            request_user = core_utilities.get_current_user(context=self.context)

            role_permissions = auth_utilities.create_role_permissions_data(request_user, permission_slugs, role)
            auth_models.RolePermission.objects.bulk_create(role_permissions)

        return role


class UpdateRoleSerializer(BaseRoleCRUDSerializer):
    """
    UpdateRoleSerializer
        Serializer to update role.
    """

    class Meta:
        model = auth_models.Role
        read_only_fields = (
            'id',
            'category',
        )

        fields = read_only_fields + (
            'name',
            'description',
            'permission_slugs',
        )

    def _validate_unique_role_name(self, data):
        if data.get('name') and data.get('name') != self.instance.name:
            system_roles = dict(auth_models.Role.SYSTEM_ROLE_NAME_CHOICES).values()
            role_names = [role.lower() for role in system_roles]
            message_kwargs = {'role': data['name']}
            if data['name'].lower() in role_names:
                raise auth_exceptions.DuplicateRoleNameError(message_kwargs=message_kwargs)
            elif auth_models.Role.objects.filter(name=data['name']).exists():
                raise auth_exceptions.DuplicateRoleNameError(message_kwargs=message_kwargs)
        return True

    def update(self, instance, validated_data):
        """
        update
            This method is used to update Role
        :param instance:
        :param validated_data:
        :return:
        """

        permission_slugs = validated_data.pop('permission_slugs', [])
        with transaction.atomic():
            instance = super().update(instance, validated_data)

            auth_models.RolePermission.objects.filter(role=instance.id).delete()
            if len(permission_slugs) > 0:
                request_user = core_utilities.get_current_user(context=self.context)
                role_permissions = auth_utilities.create_role_permissions_data(request_user, permission_slugs,
                                                                               instance)
                auth_models.RolePermission.objects.bulk_create(role_permissions)
        return instance


class BaseUserSerializer(serializers.ModelSerializer):
    """
    BaseUserSerializer
        Common fields and functionality for create and update User
    """
    first_name = serializers.CharField(
        required=True,
        min_length=1,
        max_length=20,
    )
    last_name = serializers.CharField(
        required=True,
        min_length=1,
        max_length=20,
    )
    email = serializers.EmailField(required=True)

    class Meta:
        model = auth_models.ApplicationUser
        fields = (
            'id',
            'first_name',
            'last_name',
            'username',
            'email',
            'role',
            'permissions',
            'last_login',
        )
        read_only_fields = (
            'id',
            'last_login',
            'user_name',
        )

    def validate_email(self, email):
        email_lower = email.lower()
        if auth_models.ApplicationUser.objects.filter(email=email_lower).exists():
            e = auth_exceptions.EmailAlreadyExistsError()
            logger.error(e.message)
            logger.debug('Details: {0}'.format(email_lower))
            raise e
        logger.info('Email validated.')
        logger.debug('Details: {0}'.format(email_lower))
        return email_lower

    def get_role_fields(self):
        request = self.context.get('request')
        query_params = request and request.method == 'GET' and request.query_params
        fields = query_params and query_params.get('fields', '').split(',')
        fields = fields or []
        role_fields = []
        # check for all query fields and extract role fields if present in query fields.
        for field in fields:
            field_list = field.split('.')
            if len(field_list) == 2 and field.split('.')[0] == 'role':
                role_fields.append(field.split('.')[1])

        # when query filter not present, assume all fields
        if not role_fields:
            role_fields = list(RoleSerializer.Meta.fields)
        return role_fields

    def to_representation(self, user):
        user_role = user.get_roles()
        role_serializer = RoleSerializer(instance=user_role)
        setattr(user, 'role', role_serializer.data)
        return super().to_representation(user)


class UserRoleRelationshipSerializer(serializers.ModelSerializer):
    """
    UserRoleRelationshipSerializer
        Serializer to update User Role.
    """

    class Meta:
        model = auth_models.UserRoleRelationship
        required_field = fields = (
            'user',
            'role',
        )

    def validate_role(self, role):
        """
            Method to validate new role of the user.
        """
        if role and self.instance:
            # In case of updating the user role relationship, the new role should not be same
            # as that of the existing role.
            if self.instance.role.id != role.id:
                return role
            raise auth_exceptions.InvalidRoleError()

        return role

    def update(self, instance, validated_data):
        with transaction.atomic():
            instance = super().update(instance, validated_data)
        return instance


class UserListSerializer(BaseUserSerializer):
    """
    UserListSerializer
        Serializer used to list and retrieve user details.
    """

    class Meta(BaseUserSerializer.Meta):
        new_fields = (
            BaseUserSerializer.Meta.fields +
            ('is_active', 'is_superuser', 'user_name',))

        fields = read_only_fields = new_fields


class CreateUserSerializer(BaseUserSerializer):
    """
    CreateUserSerializer
        Serializer defines how to validate User object which are going to save into database.
    """
    role = serializers.PrimaryKeyRelatedField(queryset=auth_models.Role.objects.all())

    class Meta(BaseUserSerializer.Meta):
        extra_kwargs = {
            'id': {'read_only': True},
        }
        fields = BaseUserSerializer.Meta.fields + (
            'date_joined',
        )

    def create(self, validated_data):
        """
        create
            This method is used to create User
        :param validated_data:
        :return:
        """
        role = validated_data.pop('role')
        user = super().create(validated_data)
        user_role_data = {'role': role.id, 'user': user.id}
        user_role_relationships = UserRoleRelationshipSerializer(
            data=user_role_data,
            context=self.context,
        )
        user_role_relationships.is_valid(raise_exception=True)
        user_role_relationships.save()
        return user

    def to_internal_value(self, data):
        """
        to_internal_value

        :param data:
        :return:
        """
        if not data:
            data = {}
        else:
            name_as_list = list(filter(lambda d: not core_utilities.is_blank_string(d),
                                       data.pop('name', '').split(' ')))

            data['first_name'] = 'Unknown'
            data['last_name'] = 'Unknown'
            if len(name_as_list) > 0:
                first_name = ' '.join(name_as_list[:-1] if len(name_as_list) > 1 else [name_as_list[0]])
                last_name = first_name if core_utilities.is_blank_string(name_as_list[-1]) else name_as_list[-1]
                data['first_name'] = first_name
                data['last_name'] = last_name

            emails = data.pop('emails', [])
            if len(emails) > 0:
                data['email'] = emails[-1]
                data['username'] = emails[-1].lower()

        return super().to_internal_value(data)


class UpdateUserSerializer(BaseUserSerializer):
    """
    UpdateUserSerializer
        Serializer used to update user entity of database.
    """

    role = serializers.PrimaryKeyRelatedField(queryset=auth_models.Role.objects.all())

    class Meta(BaseUserSerializer.Meta):
        read_only_fields = (
            'id',
            'username',
            'email',
        )

        fields = read_only_fields + (
            'first_name',
            'last_name',
            'is_active',
            'role',
        )

    def validate_role(self, new_role):
        """
            Method to validate new role of the user.
        """
        if new_role and self.instance:
            # In case of updating the user role relationship, the new role should not be same
            # as that of the existing role.
            user_existing_role = self.instance.get_roles()
            if user_existing_role and user_existing_role.id == new_role.id:
                raise auth_exceptions.InvalidRoleError()

        return new_role

    def update(self, instance, validated_data):
        role = validated_data.pop('role', None)
        request_user = core_utilities.get_current_user(context=self.context)
        validated_data['last_modified_by'] = validated_data.get(
            'last_modified_by',
        ) or request_user

        with transaction.atomic():
            instance = super().update(instance, validated_data)

            if role:
                existing_user_role_relationship = instance.roles.first()
                user_role_data = {'role': role.id, 'user': instance.id}
                user_role_relationships = UserRoleRelationshipSerializer(
                    data=user_role_data,
                    context=self.context,
                )
                user_role_relationships.is_valid(raise_exception=True)
                user_role_relationships.save()

                # If role change detected
                if existing_user_role_relationship and existing_user_role_relationship.role.id != role.id:
                    existing_user_role_relationship.delete()
        return instance


class ExpandUserSerializer(FlexFieldsModelSerializer):
    """
    ExpandUserSerializer
        This serializer is used for expandable feature.
    """

    class Meta:
        model = auth_models.ApplicationUser
        fields = (
            'id',
            'first_name',
            'last_name',
            'username',
            'email',
            'user_name',
        )
