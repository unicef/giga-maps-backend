from rest_flex_fields import FlexFieldsModelViewSet
from rest_framework.response import Response

from proco.core.mixins import ActionSerializerMixin
from proco.core.permissions import IsUserAuthenticated
from proco.core.utils import convert_to_int


class BaseModelViewSet(ActionSerializerMixin, FlexFieldsModelViewSet):
    """
    BaseModelViewSet
        ViewSet to handle CRUD operations on a BaseModel instance.
        Handles: GET,PUT,POST request
        Inherits: `LoggingMixin`, `ActionSerializerMixin`, `viewsets.ModelViewSet`
    """
    filter_backends = ()

    permission_classes = ()
    model = None
    base_auth_permissions = (
        # permissions.IsAuthenticated,
        IsUserAuthenticated,
    )

    http_method_names = ['get', 'post', 'put', 'delete', 'options']

    ordering_field_names = []
    apply_query_pagination = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert self.model is not None, (
            "Attribute 'model' is not defined in {0}.".format(self.__class__.__name__)
        )
        self._filtered_queryset = None

    def get_view_name(self):
        """
        get_view_name
            Method to get view name. View name is calculated as per one of the options:
            1) If 'get_custom_view_name' method is defined
            2) if model has ReportMeta and entity name is specified in ReportMeta
            3) if model is specified, get the verbose model name.
            4) default: use super get_view_name method
        :return: Entity name
        """
        if hasattr(self, 'get_custom_view_name') and callable(self.get_custom_view_name):
            entity_name = self.get_custom_view_name()
        elif self.model:
            report_meta = getattr(self.model, 'ReportMeta', None)
            entity_name = getattr(
                report_meta,
                'entity_name',
                self.model._meta.verbose_name.title(),
            )
        else:
            entity_name = super().get_view_name()
        return entity_name

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        permissions_list = self.base_auth_permissions + self.permission_classes
        return [permission() for permission in permissions_list]

    def get_serializer_context(self):
        """
        Add context from view to serializer context with value, more generic
        :return:
        """
        context = super().get_serializer_context()
        context = self.update_serializer_context(context)
        return context

    def get_queryset(self):
        """
        Return active records
        :return queryset:
        """
        queryset = self.model.objects.all()
        queryset = self.apply_queryset_filters(queryset)
        if self.ordering_field_names and len(self.ordering_field_names) > 0:
            queryset = self.apply_custom_ordering(queryset)
        return queryset

    def apply_queryset_filters(self, queryset):
        """
        Override if applying more complex filters to queryset.
        :param queryset:
        :return queryset:
        """
        return queryset

    def filter_queryset(self, queryset):
        if not hasattr(self, '_filtered_queryset') or self._filtered_queryset is None:
            return super().filter_queryset(queryset)
        return self._filtered_queryset

    def update_serializer_context(self, context):
        """
        placeholder method for child classes to override
        :param context:
        :return context:
        """
        return context

    def pre_check_permissions(self, request):
        """
        pre_check_permissions
            This method is used to perform actions before checking permissions
        :param request:
        :return:
        """
        return True

    def check_permissions(self, request):
        """
        check_permissions
            Checks if object has pre_check_permissions method
            execute that method before permission check.
        :param request:
        :return:
        """
        self.pre_check_permissions(request)
        for permission in self.get_permissions():
            if not permission.has_permission(request, self):
                self.permission_denied(
                    request,
                    message=getattr(permission, 'message', None),
                    code=getattr(permission, 'code', None)
                )

    def apply_custom_ordering(self, queryset):
        """
        Method to apply the custom order by on queryset
        Parameters
        ----------
        queryset

        Returns
        -------

        """
        qry_ordering = self.ordering_field_names
        query_param_ordering = self.request.query_params.get('ordering')
        # Apply the ordering as asked
        if query_param_ordering:
            qry_ordering = query_param_ordering.split(',')
        if len(qry_ordering) > 0:
            queryset = queryset.order_by(*qry_ordering)
        return queryset

    def get_custom_paginated_response(self, queryset):
        """

        Parameters
        ----------
        queryset

        Returns
        -------

        """
        page_size = convert_to_int(self.request.query_params.get('page_size'), default=1000)
        if page_size > 0:
            page = convert_to_int(self.request.query_params.get('page', '1'), default=1)
            offset = (page - 1) * page_size
            qs = queryset[offset:offset + page_size]
            serializer = self.get_serializer(qs, many=True)
            return Response({'count': queryset.count(), 'results': serializer.data, })

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())

        response = None
        if self.apply_query_pagination:
            response = self.get_custom_paginated_response(queryset)

        if not response:
            page = self.paginate_queryset(queryset)
            if page is not None:
                serializer = self.get_serializer(page, many=True)
                response = self.get_paginated_response(serializer.data)
            else:
                serializer = self.get_serializer(queryset, many=True)
                response = Response(serializer.data)
        return response

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
