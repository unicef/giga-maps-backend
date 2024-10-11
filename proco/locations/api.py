import copy
import logging
import traceback
from collections import OrderedDict

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchFieldDataType
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db.models import Case, Count, IntegerField, Value, When
from django.db.models.functions.text import Lower
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import mixins, permissions
from rest_framework import status
from rest_framework import status as rest_status
from rest_framework import viewsets
from rest_framework.filters import SearchFilter
from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param

from proco.accounts import models as accounts_models
from proco.background.tasks import validate_countries
from proco.core import mixins as core_mixins
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.data_sources.models import SchoolMasterData
from proco.locations.models import Country, CountryAdminMetadata
from proco.locations.search_indexes import SchoolIndex
from proco.locations.serializers import (
    CountryCSVSerializer,
    CountrySerializer,
    CountryStatusSerializer,
    CountryUpdateRetrieveSerializer,
    DetailCountrySerializer,
    ExpandCountryAdminMetadataSerializer,
    ListCountrySerializer,
)
from proco.schools.models import School
from proco.utils.cache import cache_manager
from proco.utils.error_message import delete_succ_mess, error_mess, id_missing_error_mess
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.log import action_log, changed_fields
from proco.utils.mixins import CachedListMixin, CachedRetrieveMixin
from proco.utils.tasks import update_country_related_cache

logger = logging.getLogger('gigamaps.' + __name__)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class CountryViewSet(
    CachedListMixin,
    CachedRetrieveMixin,
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    LIST_CACHE_KEY_PREFIX = 'COUNTRIES_LIST'
    RETRIEVE_CACHE_KEY_PREFIX = 'COUNTRY_INFO'

    pagination_class = None
    queryset = Country.objects.all().select_related('last_weekly_status')
    serializer_class = CountrySerializer
    filter_backends = (
        NullsAlwaysLastOrderingFilter, SearchFilter,
    )
    ordering = ('name',)
    ordering_fields = ('name',)
    search_fields = ('name',)

    def get_serializer_class(self):
        return (
            ListCountrySerializer
            if self.action == 'list'
            else DetailCountrySerializer
        )

    def get_object(self):
        return get_object_or_404(
            self.queryset.annotate(code_lower=Lower('code')),
            code_lower=self.kwargs.get('pk').lower(),
        )

    def get_queryset(self):
        qs = super().get_queryset()
        return qs.defer('geometry', 'geometry_simplified')

    def filter_queryset(self, queryset):
        """
        Given a queryset, filter it with whichever filter backend is in use.

        You are unlikely to want to override this method, although you may need
        to call it either from a list view, or from a custom `get_object`
        method if you want to apply the configured filtering backend to the
        default queryset.
        """
        queryset = super().filter_queryset(queryset)
        has_schools = self.request.query_params.get('has_schools', '').lower()

        if has_schools == 'true':
            queryset = queryset.filter(id__in=list(
                School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
                    'country_id')))
        elif has_schools == 'false':
            queryset = queryset.exclude(id__in=list(
                School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct(
                    'country_id')))

        has_school_master_records = self.request.query_params.get('has_school_master_records', '').lower()
        if has_school_master_records == 'true':
            country_ids = list(SchoolMasterData.objects.exclude(
                status__in=[
                    SchoolMasterData.ROW_STATUS_PUBLISHED,
                    SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
                    SchoolMasterData.ROW_STATUS_DISCARDED,
                ],
                is_read=True,
            ).values_list('country_id', flat=True).order_by('country_id').distinct('country_id'))
            if len(country_ids) > 0:
                queryset = queryset.filter(id__in=country_ids)

        has_api_requests = self.request.query_params.get('has_api_requests', '').lower()
        if has_api_requests == 'true':
            country_ids = list(accounts_models.APIKeyCountryRelationship.objects.all().values_list(
                'country_id', flat=True).order_by('country_id').distinct('country_id'))
            if len(country_ids) > 0:
                queryset = queryset.filter(id__in=country_ids)

        return queryset


class CountryDataViewSet(BaseModelViewSet):
    model = Country
    serializer_class = CountrySerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanAddCountry,
        core_permissions.CanUpdateCountry,
        core_permissions.CanViewCountry,
        core_permissions.CanDeleteCountry,
    )

    filter_backends = (
        NullsAlwaysLastOrderingFilter, SearchFilter, DjangoFilterBackend,
    )
    ordering = ('name',)
    ordering_fields = ('modified', 'created', 'name')
    search_fields = ('id', 'name')
    filterset_fields = {
        'id': ['exact', 'in'],
        'name': ['exact', 'in', 'iexact'],
    }

    def get_serializer_class(self):
        if self.action == 'list':
            ser_class = ListCountrySerializer
        elif self.action in ['create', 'update', 'destroy']:
            ser_class = CountryUpdateRetrieveSerializer
        else:
            ser_class = DetailCountrySerializer
        return (ser_class)

    def get_queryset(self, ids=None):
        """
        Return active records
        :return queryset:
        """
        if ids:
            queryset = self.model.objects.filter(id__in=ids).defer(
                'geometry', 'geometry_simplified',
            )
        else:
            queryset = self.model.objects.all().defer(
                'geometry', 'geometry_simplified',
            )
        return self.apply_queryset_filters(queryset)

    def create(self, request, *args, **kwargs):
        try:
            data = CountryUpdateRetrieveSerializer(data=request.data)
            if data.is_valid(raise_exception=True):
                data.save()
                action_log(request, [data.data], 1, '', self.model, field_name='name')
                update_country_related_cache.delay(data.data.get('code'))
                return Response(data.data)
            return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
        except Country.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def update(self, request, *args, **kwargs):
        if 'pk' in kwargs:
            try:
                country = Country.objects.get(pk=kwargs['pk'])
                copy_request_data = copy.deepcopy(request.data)
                if copy_request_data.get('flag') is None:
                    copy_request_data['flag'] = country.flag

                data = CountryUpdateRetrieveSerializer(instance=country, data=copy_request_data)
                if data.is_valid(raise_exception=True):
                    change_message = changed_fields(country, copy_request_data)
                    action_log(request, [country], 2, change_message, self.model, field_name='name')
                    data.save()
                    update_country_related_cache.delay(data.data.get('code'))
                    return Response(data.data)
                return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            except Country.DoesNotExist:
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
        return Response(data=id_missing_error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def destroy(self, request, *args, **kwargs):
        request_user = core_utilities.get_current_user(request=request)
        if 'pk' in kwargs:
            instance = self.get_object()
            response = super().destroy(request, *args, **kwargs)

            accounts_models.DataLayerCountryRelationship.objects.filter(country=instance).update(
                deleted=core_utilities.get_current_datetime_object(),
                last_modified_by=request_user,
            )
            accounts_models.AdvanceFilterCountryRelationship.objects.filter(country=instance).update(
                deleted=core_utilities.get_current_datetime_object(),
                last_modified_by=request_user,
            )
            accounts_models.APIKeyCountryRelationship.objects.filter(country=instance).update(
                deleted=core_utilities.get_current_datetime_object(),
                last_modified_by=request_user,
            )

            return response

        data = request.data
        if len(data) != 0:
            try:
                ids = validate_ids(data)
            except KeyError as ex:
                return Response(['Required key {0} missing in the request body'.format(ex)],
                                status=status.HTTP_400_BAD_REQUEST)
            except Exception as ex:
                return Response(ex, status=status.HTTP_400_BAD_REQUEST)

            country_qs = Country.objects.filter(id__in=ids)
            action_log(request, country_qs, 3, 'Country deleted', self.model, field_name='name')

            for country in list(country_qs):
                country.delete()

                accounts_models.DataLayerCountryRelationship.objects.filter(country=country).update(
                    deleted=core_utilities.get_current_datetime_object(),
                    last_modified_by=request_user,
                )
                accounts_models.AdvanceFilterCountryRelationship.objects.filter(country=country).update(
                    deleted=core_utilities.get_current_datetime_object(),
                    last_modified_by=request_user,
                )
                accounts_models.APIKeyCountryRelationship.objects.filter(country=country).update(
                    deleted=core_utilities.get_current_datetime_object(),
                    last_modified_by=request_user,
                )

                update_country_related_cache.delay(country.code)
            return Response(delete_succ_mess, status=status.HTTP_200_OK)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)


def validate_ids(data, field='id', unique=True):
    id_list = []
    if isinstance(data, list):
        id_list = [int(x[field]) for x in data]
    elif isinstance(data, dict):
        id_list = [int(x) for x in data[field]]

    if len(id_list) > 0:
        if unique and len(id_list) != len(set(id_list)):
            raise ValidationError('Multiple updates to a single {0} found'.format(field))

        ids_in_db = list(Country.objects.filter(pk__in=id_list).values_list(field, flat=True))
        if len(id_list) != len(ids_in_db):
            raise ValidationError('{0} value missing in database: {1}'.format(field, set(id_list) - set(ids_in_db)))

        return id_list

    return [int(data[field])]


class DownloadCountriesViewSet(BaseModelViewSet, core_mixins.DownloadAPIDataToCSVMixin):
    model = Country
    queryset = Country.objects.all().select_related('last_weekly_status')
    serializer_class = CountryCSVSerializer

    base_auth_permissions = (
        core_permissions.IsUserAuthenticated,
    )

    filter_backends = (
        DjangoFilterBackend,
        # NullsAlwaysLastOrderingFilter,
    )

    ordering_field_names = ['name']
    apply_query_pagination = True

    filterset_fields = {
        'id': ['exact', 'in'],
    }

    def list(self, request, *args, **kwargs):
        if core_utilities.is_export(request, self.action):
            return self.list_export(request, *args, **kwargs)
        else:
            self.perform_pre_checks(request, *args, **kwargs)
            self.serializer_class = CountryStatusSerializer
            return super().list(request, *args, **kwargs)


class CountrySearchStatListAPIView(CachedListMixin, ListAPIView):
    """
    CountrySearchStatListAPIView
        This class is used to list all Countries along with Admin 1 and Admin2 names.
        Inherits: ListAPIView
    """
    model = Country

    base_auth_permissions = (
        permissions.AllowAny,
    )

    LIST_CACHE_KEY_PREFIX = 'GLOBAL_COUNTRY_SEARCH_MAPPING'

    def get_queryset(self):
        queryset = self.model.objects.all()

        qs = queryset.values(
            'id', 'name', 'code', 'last_weekly_status__integration_status',
            'schools__admin1__id', 'schools__admin1__name', 'schools__admin1__description', 'schools__admin1__description_ui_label',
            'schools__admin1__giga_id_admin',
            'schools__admin2__id', 'schools__admin2__name', 'schools__admin2__description', 'schools__admin2__description_ui_label',
            'schools__admin2__giga_id_admin',
        ).annotate(
            school_count=Count('schools__id'),
        ).order_by('name', 'schools__admin1__name', 'schools__admin2__name')

        qs = qs.annotate(
            custom_order=Case(
                When(last_weekly_status__integration_status=3, school_count__gt=0, then=Value(1)),
                When(last_weekly_status__integration_status=2, school_count__gt=0, then=Value(2)),
                When(last_weekly_status__integration_status=1, school_count__gt=0, then=Value(3)),
                When(last_weekly_status__integration_status=0, school_count__gt=0, then=Value(4)),
                When(last_weekly_status__integration_status=5, school_count__gt=0, then=Value(5)),
                When(last_weekly_status__integration_status=4, school_count__gt=0, then=Value(6)),
                default=Value(7),
                output_field=IntegerField(),
            ),
        ).order_by('custom_order', 'name', 'schools__admin1__name', 'schools__admin2__name')

        return qs

    def _format_result(self, qry_data):
        data = OrderedDict()
        for resp_data in qry_data:
            country_id = resp_data.get('id')
            admin1_name = 'Unknown' if core_utilities.is_blank_string(resp_data['schools__admin1__name']) else resp_data[
                'schools__admin1__name']
            admin2_name = resp_data['schools__admin2__name']

            country_data = data.get(country_id, {
                'country_id': country_id,
                'country_name': resp_data.get('name'),
                'country_code': resp_data.get('code'),
                'integration_status': resp_data.get('last_weekly_status__integration_status'),
                'data': OrderedDict(),
            })

            # If admin 1 name exist in response
            admin1_name_data = country_data['data'].get(admin1_name, {
                'admin1_name': admin1_name,
                'admin1_id': resp_data.get('schools__admin1__id'),
                'admin1_description': resp_data.get('schools__admin1__description'),
                'admin1_description_ui_label': resp_data.get('schools__admin1__description_ui_label'),
                'admin1_code': resp_data.get('schools__admin1__giga_id_admin'),
                'data': OrderedDict(),
            })
            if core_utilities.is_blank_string(admin2_name):
                admin1_name_data['school_count'] = resp_data.get('school_count', 0)
            else:
                # If admin 2 name exist in response
                admin2_data = admin1_name_data.get('data')
                admin2_name_data = admin2_data.get(admin2_name, {
                    'type': 'Admin',
                    'admin2_name': admin2_name,
                    'admin2_id': resp_data.get('schools__admin2__id'),
                    'admin2_description': resp_data.get('schools__admin2__description'),
                    'admin2_description_ui_label': resp_data.get('schools__admin2__description_ui_label'),
                    'admin2_code': resp_data.get('schools__admin2__giga_id_admin'),
                    'data': OrderedDict(),
                })
                admin2_name_data['school_count'] = resp_data.get('school_count', 0)
                admin2_data[admin2_name] = admin2_name_data

                admin1_name_data['data'] = admin2_data

            country_data['data'][admin1_name] = admin1_name_data
            data[country_id] = country_data

        for country_id, country_data in data.items():
            country_data['admin1_count'] = len(country_data['data'])

            for _admin1_name, admin1_data in country_data['data'].items():
                admin1_data['admin2_count'] = len(admin1_data.get('data', {}))
            data[country_id] = country_data
        return list(data.values())

    def _get_raw_list_response(self, request, *args, **kwargs):
        cache_key = self.get_list_cache_key()

        queryset = self.get_queryset()
        queryset_data = list(queryset)
        data = self._format_result(queryset_data)

        request_path = remove_query_param(request.get_full_path(), self.CACHE_KEY)
        cache_manager.set(cache_key, data, request_path=request_path)
        return data

    def list(self, request, *args, **kwargs):
        if not self.use_cached_data():
            data = self._get_raw_list_response(request, *args, **kwargs)
        else:
            cache_key = self.get_list_cache_key()
            data = cache_manager.get(cache_key)
            if not data:
                data = self._get_raw_list_response(request, *args, **kwargs)
        return Response(data)


class BaseSearchMixin:
    index_class = None
    filterset_fields = None
    filter_field_type = None

    params = {}

    cognitive_search_settings = settings.AZURE_CONFIG.get('COGNITIVE_SEARCH')
    endpoint = cognitive_search_settings['SEARCH_ENDPOINT']
    credentials = AzureKeyCredential(cognitive_search_settings['SEARCH_API_KEY'])

    def create_search_client(self):
        return SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_class.Meta.index_name,
            credential=self.credentials,
        )

    def create_admin_client(self):
        return SearchIndexClient(endpoint=self.endpoint, credential=self.credentials)

    @property
    def possible_filters(self):
        field_filters = []
        for field_name, filter_list in self.filterset_fields.items():
            for filter_name in filter_list:
                field_filter = '{0}__{1}'.format(field_name, filter_name)
                field_filters.append(field_filter)
        return field_filters

    @property
    def get_filters(self):
        filters = []
        possible_filter_keys = self.possible_filters

        for param_key, param_value in self.params.items():
            param_value = param_value[-1] or 'null'

            if param_key in possible_filter_keys:
                field_name, filter_name = param_key.split('__')[0], param_key.split('__')[1]
                field_type = self.filter_field_type.get(field_name)

                if filter_name == 'exact':
                    if param_value == 'null':
                        filters.append('{0} eq {1}'.format(field_name, param_value))
                    elif field_type in (SearchFieldDataType.Int32, SearchFieldDataType.Int64,
                                        SearchFieldDataType.Double):
                        filters.append('{0} eq {1}'.format(field_name, param_value))
                    else:
                        filters.append("{0} eq '{1}'".format(field_name, param_value))
                elif filter_name == 'in':
                    in_filters = []
                    for val in param_value.split(','):
                        if val == 'null':
                            in_filters.append('{0} eq {1}'.format(field_name, val))
                        elif field_type in (SearchFieldDataType.Int32, SearchFieldDataType.Int64,
                                            SearchFieldDataType.Double):
                            in_filters.append('{0} eq {1}'.format(field_name, val))
                        else:
                            in_filters.append("{0} eq '{1}'".format(field_name, val))
                    filters.append('(' + ' or '.join(in_filters) + ')')

        return ' and '.join(filters) if len(filters) > 0 else None

    def normalize_search_text(self, val):
        """Remove the extra chars from string."""
        import re
        if not isinstance(val, str):
            return val
        wh = '\t\n\r\v\f'
        punctuation = r"""!"#$%&'()*+,./:;<=>?@[\]^`{|}~_""" + wh
        return re.sub(r'[' + re.escape(punctuation) + ']', '', val)

    @property
    def get_search_text(self):
        search_text = self.params.get('q', ['*'])[-1]
        search_text = self.normalize_search_text(search_text)
        search_text = core_utilities.sanitize_str(search_text)
        # Remove multiple spaces with single space
        search_text = ' '.join([
            search_word
            if search_word.endswith('*') else search_word + '*'
            for search_word in search_text.split()
        ])

        if '-' in search_text:
            search_text = '"' + search_text + '"'
        elif ' ' in search_text:
            search_text = search_text.replace(' ', ' AND ')

        return search_text

    @property
    def get_search_fields(self):
        search_fields = self.params.get('search_fields')
        if not search_fields:
            search_fields = self.index_class.Meta.searchable_fields
        else:
            search_fields = [core_utilities.sanitize_str(field_name) for field_name in search_fields[-1].split(',')]
        return search_fields

    @property
    def get_select(self):
        fields = self.params.get('fields')
        if not fields:
            fields = [
                attr
                for attr in dir(SchoolIndex)
                if not callable(getattr(SchoolIndex, attr)) and not attr.startswith('__')
            ]

        return ','.join(fields)

    @property
    def get_top(self):
        return core_utilities.convert_to_int(self.params.get('page_size', ['20'])[-1], default=20)

    @property
    def get_skip(self):
        page_no = core_utilities.convert_to_int(self.params.get('page', ['1'])[-1], default=1)
        if page_no > 0:
            page_no = page_no - 1
        return page_no * self.get_top

    @property
    def get_orderby(self):
        order_by = self.params.get('ordering')
        if not order_by:
            order_by = self.index_class.Meta.ordering
        else:
            order_by = [core_utilities.sanitize_str(field_name) for field_name in order_by[-1].split(',')]

        order_by_list = []
        for order_field in order_by:
            if order_field.startswith('-'):
                order_by_list.append('{0} desc'.format(order_field[1:]))
            else:
                order_by_list.append('{0} asc'.format(order_field))

        return ','.join(order_by_list)

    @property
    def get_count(self):
        return True

    @property
    def get_query_type(self):
        return 'full'

    def index_search(self, request, *args, **kwargs):
        self.params = dict(request.query_params)

        search_client = self.create_search_client()
        logger.debug(
            'Search params: \nsearch_text - {search_text}\ninclude_total_count - {include_total_count}'
            '\norder_by - {order_by}\nsearch_fields - {search_fields}\nselect - {select}'
            '\nskip - {skip}\ntop - {top}\nfilter - {filter}\nquery_type - {query_type}'.format(
                search_text=self.get_search_text,
                include_total_count=self.get_count,
                order_by=self.get_orderby,
                search_fields=self.get_search_fields,
                select=self.get_select,
                skip=self.get_skip,
                top=self.get_top,
                filter=self.get_filters,
                query_type=self.get_query_type,
            ))
        results = search_client.search(
            search_text=self.get_search_text,
            include_total_count=self.get_count,
            order_by=self.get_orderby,
            search_fields=self.get_search_fields,
            select=self.get_select,
            skip=self.get_skip,
            top=self.get_top,
            filter=self.get_filters,
            query_type=self.get_query_type,
        )

        logger.debug('Total Documents Matching Query: {}'.format(results.get_count()))
        return results


class AggregateSearchViewSet(BaseSearchMixin, ListAPIView):
    """
    AggregateSearchViewSet
        Endpoint to use the Cognitive search index.
        Inherits: BaseSearchMixin, ListAPIView
    """
    index_class = SchoolIndex

    base_auth_permissions = (
        permissions.AllowAny,
    )

    filterset_fields = {
        'id': ['exact', 'in'],
        'country_id': ['exact', 'in'],
        'admin1_id': ['exact', 'in'],
        'admin2_id': ['exact', 'in'],
        'admin1_name': ['exact', 'in'],
        'admin2_name': ['exact', 'in'],
    }

    filter_field_type = {
        'id': SearchFieldDataType.Int64,
        'country_id': SearchFieldDataType.Int64,
        'admin1_id': SearchFieldDataType.Int64,
        'admin2_id': SearchFieldDataType.Int64,
    }

    def list(self, request, *args, **kwargs):
        resp_data = OrderedDict()
        data = self.index_search(request, *args, **kwargs)
        counts = data.get_count()
        resp_data['count'] = counts
        resp_data['results'] = list(data)
        return Response(resp_data)


class CountryAdminMetadataViewSet(BaseModelViewSet):
    """
    CountryAdminMetadataViewSet
        This class is used to list all Admin Metadata.
        Inherits: BaseModelViewSet
    """
    model = CountryAdminMetadata
    serializer_class = ExpandCountryAdminMetadataSerializer

    base_auth_permissions = (
        permissions.AllowAny,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_fields = ('country', 'layer_name', 'name', 'description', 'id', 'description_ui_label')
    filterset_fields = {
        'id': ['in', 'exact'],
        'country_id': ['in', 'exact'],
        'layer_name': ['exact', 'in', 'iexact'],
        'name': ['exact', 'in', 'iexact'],
        'description': ['exact', 'in', 'iexact'],
        'description_ui_label': ['exact', 'in', 'iexact'],
        'parent_id': ['in', 'exact'],
    }

    permit_list_expands = ['country', 'parent', 'created_by', 'last_modified_by']


class MarkAsJoinedViewSet(BaseModelViewSet):
    model = Country
    serializer_class = CountrySerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
    )

    def check_access(self, request, queryset, ids):
        access = True if request.user.is_superuser else False
        if not access:
            countries_available = ids
            qs_not_available = queryset.exclude(id__in=countries_available)
            result = (False, qs_not_available) if not qs_not_available.exists() else (True, None)
        else:
            result = True, None
        return result

    def create(self, request, *args, **kwargs):
        if 'pk' in kwargs:
            return super().destroy(request, *args, **kwargs)

        data = request.data
        if len(data) != 0:
            try:
                ids = validate_ids(data)
                queryset = self.model.objects.all()
                access, qs_not_available = self.check_access(request, queryset, ids)
                if not access:
                    message = f'You do not have access to change countries: ' \
                              f'{", ".join(qs_not_available.values_list("name", flat=True))}'
                    return Response({'desc': message}, status=rest_status.HTTP_200_OK)
                else:
                    if request.method == 'POST':
                        task = validate_countries.apply_async(args=[ids], countdown=2)
                        message = 'Countries validation started. Please wait.'
                        return Response({'desc': message, 'task_id': [task.id]}, status=rest_status.HTTP_200_OK)
            except:
                logger.error(traceback.format_exc())
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
