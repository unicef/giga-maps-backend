from django.contrib.admin.models import LogEntry
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import mixins, viewsets
from rest_framework import status
from rest_framework import status as rest_status
from rest_framework.filters import SearchFilter
from rest_framework.response import Response

from proco.background.models import BackgroundTask
from proco.background.serializers import BackgroundTaskSerializer, BackgroundTaskHistorySerializer
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.utils.error_message import delete_succ_mess
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.log import action_log


class BackgroundTaskViewSet(BaseModelViewSet):
    model = BackgroundTask
    serializer_class = BackgroundTaskSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewBackgroundTask,
        core_permissions.CanDeleteBackgroundTask,
    )

    filter_backends = (
        NullsAlwaysLastOrderingFilter, SearchFilter, DjangoFilterBackend,
    )
    ordering_field_names = ['-created_at', '-completed_at']
    apply_query_pagination = True

    search_fields = ('task_id', 'log',)
    filterset_fields = {
        'task_id': ['exact', 'in'],
        'log': ['exact', 'in'],
    }

    lookup_field = 'task_id'

    def destroy(self, request, *args, **kwargs):
        try:
            ids = request.data['task_id']
            if isinstance(ids, list) and len(ids) > 0:
                task_qs = self.model.objects.filter(task_id__in=ids, )
                if task_qs.exists():
                    action_log(request, task_qs, 3, 'Background task deleted', self.model, 'name')
                    task_qs.update(
                        deleted=core_utilities.get_current_datetime_object(),
                        deleted_by=core_utilities.get_current_user(request=request),
                    )
                    return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
            raise ValidationError('{0} value missing in database: {1}'.format('task_id', ids))
        except KeyError as ex:
            return Response(['Required key {0} missing in the request body'.format(ex)],
                            status=status.HTTP_400_BAD_REQUEST)
        except Exception as ex:
            return Response(ex, status=status.HTTP_400_BAD_REQUEST)


class BackgroundTaskHistoryViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    model = BackgroundTask
    serializer_class = BackgroundTaskSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
    )

    def list(self, request, *args, **kwargs):
        task_id = self.kwargs.get('task_id')
        ct = ContentType.objects.get_for_model(self.model)
        qs = LogEntry.objects.filter(object_id=task_id, content_type_id=ct.pk)
        serializer = BackgroundTaskHistorySerializer(qs, many=True, context={'request': request})
        return Response(serializer.data)
