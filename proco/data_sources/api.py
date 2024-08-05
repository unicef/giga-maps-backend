from django.core.exceptions import ValidationError
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import status
from rest_framework import status as rest_status
from rest_framework.filters import SearchFilter
from rest_framework.response import Response
from rest_framework.views import APIView

from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.custom_auth import models as auth_models
from proco.data_sources import serializers
from proco.data_sources import tasks as sources_tasks
from proco.data_sources.models import SchoolMasterData, QoSData, DailyCheckAppMeasurementData
from proco.locations.models import Country
from proco.utils.filters import NullsAlwaysLastOrderingFilter


class StaticAndLiveDataLoaderViewSet(APIView):
    permission_classes = (
        core_permissions.IsSuperUserEnabledAuthenticated,
    )

    def get(self, request, *args, **kwargs):
        source_type = self.request.query_params.get('data_source', 'static')
        iso3_format = self.request.query_params.get('iso3_format', None)

        if source_type == 'static':
            sources_tasks.update_static_data.delay(country_iso3_format=iso3_format)
        elif source_type == 'live':
            sources_tasks.update_live_data.delay(today=True)
        else:
            return Response(data={'message': 'Invalid "data_source" selected. Available options are static/live.'},
                            status=rest_status.HTTP_502_BAD_GATEWAY)

        return Response(data={'message': 'Data load started. Source tables will be updated in a few minutes.'})


class SchoolMasterLoaderViewSet(APIView):
    permission_classes = (
        core_permissions.IsSuperUserEnabledAuthenticated,
    )

    def get(self, request, *args, **kwargs):
        iso3_format = self.request.query_params.get('iso3_format', None)
        sources_tasks.load_data_from_school_master_apis(country_iso3_format=iso3_format)
        sources_tasks.cleanup_school_master_rows()
        if iso3_format:
            countries_ids = list(Country.objects.filter(iso3_format=iso3_format).values_list('id', flat=True))
            sources_tasks.handle_published_school_master_data_row(country_ids=countries_ids)
        else:
            sources_tasks.handle_published_school_master_data_row()
        sources_tasks.handle_deleted_school_master_data_row()
        sources_tasks.email_reminder_to_editor_and_publisher_for_review_waiting_records()

        return Response(data={'success': True})


class QoSLoaderViewSet(APIView):
    permission_classes = (
        core_permissions.IsSuperUserEnabledAuthenticated,
    )

    def get(self, request, *args, **kwargs):
        sources_tasks.load_data_from_qos_apis()

        date = QoSData.objects.all().values_list('date', flat=True).order_by('-date').first()

        countries_ids = QoSData.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
            'country_id')

        for country_id in countries_ids:
            sources_tasks.finalize_previous_day_data(None, country_id, date)

        return Response(data={'success': True})


class DailyCheckAppLoaderViewSet(APIView):
    permission_classes = (
        core_permissions.IsSuperUserEnabledAuthenticated,
    )

    def get(self, request, *args, **kwargs):
        sources_tasks.load_data_from_daily_check_app_api()

        countries_ids = Country.objects.filter(
            code__in=list(DailyCheckAppMeasurementData.objects.values_list(
                'country_code', flat=True).order_by(
                'country_code').distinct(
                'country_code')),
        ).values_list('id', flat=True)

        today_date = core_utilities.get_current_datetime_object().date()
        for country_id in countries_ids:
            sources_tasks.finalize_previous_day_data(None, country_id, today_date)
        return Response(data={'success': True})


class SchoolMasterDataViewSet(BaseModelViewSet):
    """
    SchoolMasterDataViewSet
        This class is used to list all School Master Data Rows.
        Inherits: BaseModelViewSet
    """
    model = SchoolMasterData
    serializer_class = serializers.ListSchoolMasterDataSerializer

    action_serializers = {
        'partial_update': serializers.UpdateSchoolMasterDataSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewSchoolMasterData,
        core_permissions.CanUpdateSchoolMasterData,
        core_permissions.CanDeleteSchoolMasterData,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
        SearchFilter,
    )

    ordering_fields = ('school_name', 'status', 'modified', 'created', 'country_id')
    apply_query_pagination = True

    filterset_fields = {
        'school_name': ['iexact', 'in', 'exact'],
        'status': ['iexact', 'exact', 'in'],
        'id': ['exact', 'in'],
        'country_id': ['exact', 'in'],
    }

    search_fields = ('school_name', 'school_id_giga', 'school_id_govt')

    permit_list_expands = ['school', 'modified_by', 'published_by', 'country']

    def apply_queryset_filters(self, queryset):
        """ Perform the action on DRAFT or UPDATED_IN_DRAFT rows """
        request_user = self.request.user

        queryset = queryset.exclude(
            status__in=[
                SchoolMasterData.ROW_STATUS_PUBLISHED,
                SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
                SchoolMasterData.ROW_STATUS_DISCARDED,
            ],
            is_read=True,
        )

        user_permissions = request_user.permissions

        is_publisher = user_permissions.get(auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA, False)
        is_editor_only = (
            not user_permissions.get(auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA, False) and
            user_permissions.get(auth_models.RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA, False))

        if is_publisher:
            # To publisher show only LOCK or UPDATED in LOCKED status rows
            queryset = queryset.filter(
                status__in=[
                    SchoolMasterData.ROW_STATUS_DRAFT,
                    SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                    SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
                    SchoolMasterData.ROW_STATUS_DELETED,
                ],
            )
        elif is_editor_only:
            # To editor show only DRAFT or UPDATED in DRAFT status rows
            queryset = queryset.filter(
                status__in=[
                    SchoolMasterData.ROW_STATUS_DRAFT,
                    SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
                ],
            )
        return super().apply_queryset_filters(queryset)

    def get_queryset(self, ids=None):
        """
        Return active records
        :return queryset:
        """
        if ids:
            queryset = self.model.objects.filter(id__in=ids, )
        else:
            queryset = self.model.objects.all()
        return self.apply_queryset_filters(queryset)

    def update(self, request, *args, **kwargs):
        if 'pk' in kwargs:
            return super().update(request, *args, **kwargs)

        data = request.data
        if len(data) != 0:
            try:
                ids = validate_ids(data)
            except KeyError as ex:
                return Response(['Required key {} missing in the request body'.format(ex)],
                                status=status.HTTP_400_BAD_REQUEST)
            except Exception as ex:
                return Response(ex, status=status.HTTP_400_BAD_REQUEST)

            instances = self.get_queryset(ids=ids)
            if isinstance(data, dict):
                data = [data, ]
            serializer = self.get_serializer(instances, data=data, partial=False, many=True, )
            serializer.is_valid(raise_exception=True)

            self.perform_update(serializer)
            return Response(serializer.data)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        if 'pk' in kwargs:
            data = [{'id': kwargs['pk']}]
        else:
            data = request.data

        if len(data) != 0:
            try:
                ids = validate_ids(data)
            except KeyError as ex:
                return Response(['Required key {} missing in the request body'.format(ex)],
                                status=status.HTTP_400_BAD_REQUEST)
            except Exception as ex:
                return Response(ex, status=status.HTTP_400_BAD_REQUEST)

            # Only DELETED rows can be discarded by only Publisher
            deleted_rows_qs = self.model.objects.filter(
                id__in=ids,
                status=SchoolMasterData.ROW_STATUS_DELETED,
                is_read=False,
            )
            if deleted_rows_qs.exists():
                deleted_rows_qs.update(
                    status=SchoolMasterData.ROW_STATUS_DISCARDED,
                    is_read=True,
                    modified=core_utilities.get_current_datetime_object(),
                    modified_by=self.request.user,
                )
                return Response(status=status.HTTP_204_NO_CONTENT)
        return Response({}, status=status.HTTP_400_BAD_REQUEST)


def validate_ids(data, field='id', unique=True):
    if isinstance(data, list):
        id_list = [int(x[field]) for x in data]

        if unique and len(id_list) != len(set(id_list)):
            raise ValidationError('Multiple updates to a single {} found'.format(field))

        ids_in_db = list(SchoolMasterData.objects.filter(pk__in=id_list).values_list(field, flat=True))
        if len(id_list) != len(ids_in_db):
            raise ValidationError('{0} value missing in database: {1}'.format(field, set(id_list) - set(ids_in_db)))

        return id_list

    return [int(data[field])]


class SchoolMasterDataPublishViewSet(BaseModelViewSet):
    model = SchoolMasterData
    serializer_class = serializers.PublishSchoolMasterDataSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishSchoolMasterData,
    )

    def get_queryset(self, ids=None):
        """
        Return active records
        :return queryset:
        """
        queryset = self.model.objects.all()

        if ids:
            queryset = queryset.filter(id__in=ids, )
        return self.apply_queryset_filters(queryset)

    def update(self, request, *args, **kwargs):
        if 'pk' in kwargs:
            return super().update(request, *args, **kwargs)

        data = request.data
        if len(data) != 0:
            try:
                ids = validate_ids(data)
            except KeyError as ex:
                return Response(['Required key {} missing in the request body'.format(ex)],
                                status=status.HTTP_400_BAD_REQUEST)
            except Exception as ex:
                return Response(ex, status=status.HTTP_400_BAD_REQUEST)

            instances = self.get_queryset(ids=ids)

            serializer = self.get_serializer(instances, data=data, partial=False, many=True, )
            serializer.is_valid(raise_exception=True)

            self.perform_update(serializer)
            return Response(serializer.data)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)


class SchoolMasterDataPublishByCountryViewSet(BaseModelViewSet):
    model = SchoolMasterData

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishSchoolMasterData,
    )

    def get_queryset(self, ids=None):
        """
        Return active records
        :return queryset:
        """
        queryset = self.model.objects.all()

        if ids:
            queryset = queryset.filter(
                country_id__in=ids,
                status__in=[
                    SchoolMasterData.ROW_STATUS_DRAFT,
                    SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                    SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
                    SchoolMasterData.ROW_STATUS_DELETED,
                ],
                is_read=False,
            )
        return self.apply_queryset_filters(queryset)

    def update(self, request, *args, **kwargs):
        data = request.data
        if len(data) != 0:
            try:
                if isinstance(data, list):
                    id_list = [int(x['country_id']) for x in data]

                    if len(id_list) != len(set(id_list)):
                        raise ValidationError('Multiple updates to a single {0} found'.format('country_id'))

                    ids_in_db = list(SchoolMasterData.objects.filter(country_id__in=id_list).values_list(
                        'country_id', flat=True))
                    if len(ids_in_db) == 0:
                        raise ValidationError('Provided "{0}" value missing in SchoolMasterData database '
                                              'table: {1}'.format('country_id', set(id_list)))
                else:
                    id_list = [int(data['country_id'])]
            except KeyError as ex:
                return Response(['Required key {0} missing in the request body'.format(ex)],
                                status=status.HTTP_400_BAD_REQUEST)
            except Exception as ex:
                return Response(ex, status=status.HTTP_400_BAD_REQUEST)

            instances = self.get_queryset(ids=id_list)

            published_instances = instances.exclude(status=SchoolMasterData.ROW_STATUS_DELETED)
            if published_instances.exists():
                country_ids = list(
                    published_instances.values_list('country_id', flat=True).order_by('country_id').distinct(
                        'country_id'))
                # Update the rows to PUBLISHED for Insert/Update
                published_instances.update(
                    status=SchoolMasterData.ROW_STATUS_PUBLISHED,
                    published_by=self.request.user,
                    published_at=core_utilities.get_current_datetime_object(),
                )
                sources_tasks.handle_published_school_master_data_row.delay(country_ids=country_ids)

            deleted_published_instances = instances.filter(status=SchoolMasterData.ROW_STATUS_DELETED)
            if deleted_published_instances.exists():
                country_ids = list(
                    deleted_published_instances.values_list('country_id', flat=True).order_by('country_id').distinct(
                        'country_id'))
                # Update the rows to DELETED_PUBLISHED for DELETED
                deleted_published_instances.update(
                    status=SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
                    published_by=self.request.user,
                    published_at=core_utilities.get_current_datetime_object(),
                )
                sources_tasks.handle_deleted_school_master_data_row.delay(country_ids=country_ids)

            return Response(data={'message': 'Country record publish started. Application will be updated in minutes.'})

        return Response({}, status=status.HTTP_400_BAD_REQUEST)
