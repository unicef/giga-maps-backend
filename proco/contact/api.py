from django.core.exceptions import ValidationError
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import permissions
from rest_framework import status
from rest_framework import status as rest_status
from rest_framework.filters import SearchFilter
from rest_framework.generics import CreateAPIView
from rest_framework.response import Response

from proco.contact import serializers as contact_serializers
from proco.contact.models import ContactMessage
from proco.core import permissions as core_permissions
from proco.core.viewsets import BaseModelViewSet
from proco.utils.error_message import delete_succ_mess
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.log import action_log


class ContactAPIView(BaseModelViewSet):
    model = ContactMessage
    serializer_class = contact_serializers.ContactSerializer
    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewContactMessage,
        core_permissions.CanDeleteContactMessage,
    )

    filter_backends = (
        NullsAlwaysLastOrderingFilter, SearchFilter, DjangoFilterBackend,
    )
    ordering_field_names = ['-created']
    apply_query_pagination = True
    search_fields = ('id', 'full_name', 'purpose', 'organisation', 'email', 'category')

    filterset_fields = {
        'id': ['exact', 'in'],
        'full_name': ['exact', 'in'],
        'email': ['exact', 'in'],
        'category': ['exact', 'in'],
    }

    def destroy(self, request, *args, **kwargs):
        try:
            ids = request.data['id']
            if isinstance(ids, list) and len(ids) > 0:
                queryset = self.model.objects.filter(id__in=ids, )
                if queryset.exists():
                    action_log(request, queryset, 3, 'Contact deleted', self.model, 'full_name')
                    queryset.delete()
                    return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
            raise ValidationError('{0} value missing in database: {1}'.format('id', ids))
        except KeyError as ex:
            return Response(['Required key {0} missing in the request body'.format(ex)],
                            status=status.HTTP_400_BAD_REQUEST)
        except Exception as ex:
            return Response(str(ex), status=status.HTTP_400_BAD_REQUEST)


class CreateContactAPIView(CreateAPIView):
    serializer_class = contact_serializers.CreateContactSerializer

    permission_classes = (permissions.AllowAny,)
