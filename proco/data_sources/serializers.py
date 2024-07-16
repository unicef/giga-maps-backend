from django.db import transaction
from rest_flex_fields.serializers import FlexFieldsModelSerializer
from rest_framework import serializers

from proco.core import utils as core_utilities
from proco.custom_auth import models as auth_models
from proco.custom_auth.serializers import ExpandUserSerializer
from proco.data_sources import exceptions as source_exceptions
from proco.data_sources import models as sources_models
from proco.data_sources import tasks as sources_tasks
from proco.schools.serializers import ExpandSchoolSerializer, ExpandCountrySerializer


class ListSchoolMasterDataSerializer(FlexFieldsModelSerializer):
    """
    SchoolMasterDataListSerializer
        Serializer to list all Rows.
    """

    modified_fields = serializers.SerializerMethodField()
    status_verbose = serializers.SerializerMethodField()

    class Meta:
        model = sources_models.SchoolMasterData
        read_only_fields = fields = (
            'id',
            'created',
            'modified',
            'school_id_giga',
            'school_id_govt',
            'school_name',
            'admin1',
            'admin1_id_giga',
            'admin2',
            'admin2_id_giga',
            'latitude',
            'longitude',
            'education_level',
            'school_establishment_year',
            'num_students',
            'num_teachers',
            'num_classrooms',
            'num_latrines',
            'num_computers',
            'water_availability',
            'electricity_availability',
            'connectivity_govt',
            'connectivity_type_govt',
            'cellular_coverage_availability',
            'cellular_coverage_type',
            'connectivity',
            'connectivity_RT',
            'connectivity_RT_datasource',
            # 'connectivity_static',
            'version',
            'status',
            'status_verbose',
            'modified_by',
            'school',
            'country',
            'modified_fields',
        )

        expandable_fields = {
            'school': (ExpandSchoolSerializer, {'source': 'school'}),
            'modified_by': (ExpandUserSerializer, {'source': 'modified_by'}),
            'published_by': (ExpandUserSerializer, {'source': 'published_by'}),
            'country': (ExpandCountrySerializer, {'source': 'country'}),
        }

    def get_status_verbose(self, row):
        return dict(sources_models.SchoolMasterData.ROW_STATUS_CHOICES).get(row.status)

    def get_modified_fields(self, row):
        if row.status == sources_models.SchoolMasterData.ROW_STATUS_DELETED:
            return {}
        updated_values = {}

        published_row_for_same_school = sources_models.SchoolMasterData.objects.filter(
            status=sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED,
            school_id_giga=row.school_id_giga,
        ).first()

        if published_row_for_same_school:
            check_fields = [
                'school_id_govt',
                'school_name',
                'admin1_id_giga',
                'admin2_id_giga',
                'latitude',
                'longitude',
                'education_level',
                'school_establishment_year',
                'num_students',
                'num_teachers',
                'num_classrooms',
                'num_latrines',
                'num_computers',
                'water_availability',
                'electricity_availability',
                'connectivity_govt',
                'connectivity_type_govt',
                'cellular_coverage_availability',
                'cellular_coverage_type',
                'connectivity',
                'connectivity_RT',
                'connectivity_RT_datasource',
                # 'connectivity_static',
            ]

            for key in check_fields:
                if getattr(row, key) != getattr(published_row_for_same_school, key):
                    updated_values[key] = {
                        'old': getattr(published_row_for_same_school, key),
                        'new': getattr(row, key)
                    }
            return updated_values

        if row.school:
            if str(row.school_name).lower() != str(row.school.name).lower():
                updated_values['school_name'] = {
                    'old': row.school.name,
                    'new': row.school_name
                }

            old_external_id = None \
                if core_utilities.is_blank_string(row.school.external_id) else str(row.school.external_id).lower()
            new_external_id = None \
                if core_utilities.is_blank_string(row.school_id_govt) else str(row.school_id_govt).lower()

            if old_external_id != new_external_id:
                updated_values['school_id_govt'] = {
                    'old': row.school.external_id,
                    'new': row.school_id_govt
                }

            if core_utilities.convert_to_float(row.longitude, orig=True) != row.school.geopoint.x:
                updated_values['longitude'] = {
                    'old': row.school.geopoint.x,
                    'new': row.longitude
                }

            if core_utilities.convert_to_float(row.latitude, orig=True) != row.school.geopoint.y:
                updated_values['latitude'] = {
                    'old': row.school.geopoint.y,
                    'new': row.latitude
                }

            old_admin1_id = None
            if row.school.admin1:
                old_admin1_id = str(row.school.admin1.giga_id_admin).lower()
            new_admin1_id = None \
                if core_utilities.is_blank_string(row.admin1_id_giga) else str(row.admin1_id_giga).lower()

            if old_admin1_id != new_admin1_id:
                updated_values['admin1_id_giga'] = {
                    'old': row.school.admin1.giga_id_admin if row.school.admin1 else None,
                    'new': row.admin1_id_giga
                }

            old_admin2_id = None
            if row.school.admin2:
                old_admin2_id = str(row.school.admin2.giga_id_admin).lower()
            new_admin2_id = None \
                if core_utilities.is_blank_string(row.admin2_id_giga) else str(row.admin2_id_giga).lower()

            if old_admin2_id != new_admin2_id:
                updated_values['admin2_id_giga'] = {
                    'old': row.school.admin2.giga_id_admin if row.school.admin2 else None,
                    'new': row.admin2_id_giga
                }

            old_education_level = None \
                if core_utilities.is_blank_string(row.school.education_level) else str(
                row.school.education_level).lower()
            new_education_level = None \
                if core_utilities.is_blank_string(row.education_level) else str(row.education_level).lower()

            if old_education_level != new_education_level:
                updated_values['education_level'] = {
                    'old': row.school.education_level,
                    'new': row.education_level
                }

        return updated_values


class UpdateListSerializer(serializers.ListSerializer):

    def update(self, instances, validated_data):
        instance_hash = {index: instance for index, instance in enumerate(instances)}
        if len(instance_hash) > 0:
            result = [
                self.child.update(instance_hash[index], attrs)
                for index, attrs in enumerate(validated_data)
            ]
            return result
        raise source_exceptions.ZeroSchoolMasterDataRowError()


class UpdateSchoolMasterDataSerializer(serializers.ModelSerializer):
    """
    UpdateSchoolMasterDataSerializer
        Serializer to update School MAster data row.
    """

    class Meta:
        model = sources_models.SchoolMasterData
        read_only_fields = (
            'id',
            'created',
            'school_id_giga',
        )

        fields = read_only_fields + (
            'modified',
            'school_id_govt',
            'school_name',
            'admin1',
            'admin1_id_giga',
            'admin2',
            'admin2_id_giga',
            'latitude',
            'longitude',
            'education_level',
            'school_establishment_year',
            'num_students',
            'num_teachers',
            'num_classrooms',
            'num_latrines',
            'num_computers',
            'water_availability',
            'electricity_availability',
            'connectivity_govt',
            'connectivity_type_govt',
            'cellular_coverage_availability',
            'cellular_coverage_type',
            'connectivity',
            'connectivity_RT',
            'connectivity_RT_datasource',
            # 'connectivity_static',
            'status',
        )

        list_serializer_class = UpdateListSerializer

    def _validate_status(self, instance, validated_data):
        request_user = core_utilities.get_current_user(context=self.context)
        user_permissions = request_user.permissions

        is_publisher = user_permissions.get(auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA, False)
        is_editor_only = (not user_permissions.get(auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA, False) and
                          user_permissions.get(auth_models.RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA, False))

        status = validated_data.get('status', None)

        if not status:
            if is_publisher:
                status = sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED
            elif is_editor_only:
                status = sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT
            else:
                message_kwargs = {
                    'old': instance.status,
                    'new': status,
                }
                raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

        message_kwargs = {
            'old': instance.status,
            'new': status,
        }

        if status in (sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED,
                      sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
                      sources_models.SchoolMasterData.ROW_STATUS_DELETED,
                      sources_models.SchoolMasterData.ROW_STATUS_DISCARDED):
            # Publish/Deleted/Discarded status can not be set from this API
            raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

        elif status == sources_models.SchoolMasterData.ROW_STATUS_DRAFT:
            if is_publisher:
                if instance.status not in [
                    sources_models.SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                    sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
                ]:
                    # Only Publisher can move back the row to DRAFT
                    raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

            elif instance.status in [
                sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
                sources_models.SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
            ]:
                # Can not be moved back to draft by other than Publisher
                raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

        elif status == sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT:
            if is_publisher:
                # Published can not use this status
                raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)
            if instance.status not in [sources_models.SchoolMasterData.ROW_STATUS_DRAFT,
                                       sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT, ]:
                # Only Draft row can be put into this status
                raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

        elif status == sources_models.SchoolMasterData.ROW_STATUS_DRAFT_LOCKED:
            if is_publisher:
                # Published can not use this status
                raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)
            if is_editor_only:
                if instance.status in [sources_models.SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                                       sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED, ]:
                    # Already picked by publisher or forwarded to publisher
                    raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

        elif status == sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED:
            if not is_publisher:
                # Only publisher can use this status
                raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

            if instance.status == sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT:
                raise source_exceptions.InvalidSchoolMasterDataRowStatusAtUpdateError(message_kwargs=message_kwargs)

        return status

    def update(self, instance, validated_data):
        """
        update
            This method is used to update School MAster data row
        :param instance:
        :param validated_data:
        :return:
        """
        validated_data['status'] = self._validate_status(instance, validated_data)
        validated_data['modified'] = core_utilities.get_current_datetime_object()
        with transaction.atomic():
            request_user = core_utilities.get_current_user(context=self.context)
            if request_user is not None:
                validated_data['modified_by'] = request_user

            instance = super().update(instance, validated_data)

        return instance


class PublishSchoolMasterDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = sources_models.SchoolMasterData
        read_only_fields = (
            'id',
            'school_id_giga',
            'school_name',
        )

        fields = read_only_fields + (
            'published_by',
            'published_at',
            'status',
        )

        list_serializer_class = UpdateListSerializer

    def _validate_status(self, instance, validated_data):
        if instance.status in [
            sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
            sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED,
            sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,

        ]:
            message_kwargs = {
                'old': instance.status,
                'new': sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED,
            }
            raise source_exceptions.InvalidSchoolMasterDataRowStatusError(message_kwargs=message_kwargs)

    def delete_all_related_rows(self, instance):
        print('Deleting all the row for same school giga id: {0}'.format(instance.school_id_giga))
        sources_models.SchoolMasterData.objects.filter(
            school_id_giga=instance.school_id_giga,
        ).exclude(
            pk=instance.id,
        ).delete()

    def update_all_related_models(self, instance):
        if instance.status == sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED:
            sources_tasks.handle_published_school_master_data_row(published_row=instance)
        else:
            sources_tasks.handle_deleted_school_master_data_row(deleted_row=instance)

    def update(self, instance, validated_data):
        """
        update
            This method is used to update Data Layer
        :param instance:
        :param validated_data:
        :return:
        """
        if instance.status == sources_models.SchoolMasterData.ROW_STATUS_DELETED:
            validated_data['status'] = sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED
        else:
            validated_data['status'] = sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED
        self._validate_status(instance, validated_data)
        validated_data['published_at'] = core_utilities.get_current_datetime_object()

        request_user = core_utilities.get_current_user(context=self.context)
        if request_user is not None:
            validated_data['published_by'] = request_user

        instance = super().update(instance, validated_data)

        self.delete_all_related_rows(instance)
        self.update_all_related_models(instance)

        return instance
