import itertools

from rest_framework import status as rest_status
from rest_framework import viewsets
from rest_framework.decorators import permission_classes
from rest_framework.filters import SearchFilter
from rest_framework.response import Response

from proco.about_us.models import AboutUs, SliderImage
from proco.about_us.serializers import AboutUsSerializer, SliderImageSerializer
from proco.core import permissions as core_permissions
from proco.core.viewsets import BaseModelViewSet
from proco.utils.error_message import id_missing_error_mess, delete_succ_mess, \
    error_mess
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.log import action_log, changed_fields, changed_about_us_content_fields


class SlideImageAPIView(BaseModelViewSet):
    model = SliderImage
    serializer_class = SliderImageSerializer
    permission_classes = (
        core_permissions.IsUserAuthenticated,
    )

    filter_backends = (
        NullsAlwaysLastOrderingFilter, SearchFilter,
    )

    def create(self, request, *args, **kwargs):
        try:
            data = SliderImageSerializer(data=request.data, context={'request': request})
            if data.is_valid():
                data.save()
                action_log(request, [data.data], 1, '', self.model, field_name='image')
                return Response(data.data)
            return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
        except SliderImage.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def update(self, request, pk):
        if pk is not None:
            try:
                image_data = SliderImage.objects.get(pk=pk)
                data = SliderImageSerializer(instance=image_data, data=request.data, partial=True,
                                             context={'request': request})
                if data.is_valid():
                    change_message = changed_fields(image_data, request.data)
                    action_log(request, [image_data], 2, change_message, self.model, field_name='image')
                    data.save()
                    return Response(data.data)
                return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            except SliderImage.DoesNotExist:
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
        return Response(data=id_missing_error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def retrieve(self, request, pk):
        if pk is not None:
            try:
                image_data = SliderImage.objects.get(id=pk)
                if image_data:
                    serializer = SliderImageSerializer(image_data, partial=True,
                                                       context={'request': request}, )
                    return Response(serializer.data)
                return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            except SliderImage.DoesNotExist:
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
        return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)

    def destroy(self, request):
        try:
            if len(request.data['id']) > 0:
                if SliderImage.objects.filter(id__in=request.data['id']).exists():
                    image_data = SliderImage.objects.filter(id__in=request.data['id'])
                    if image_data:
                        action_log(request, image_data, 3, "image deleted", self.model,
                                   field_name='title')
                        image_data.delete()
                        return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)
        except:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)


class AboutUsAPIView(viewsets.ViewSet):
    model = AboutUs
    serializer_class = AboutUsSerializer

    filter_backends = (
        NullsAlwaysLastOrderingFilter, SearchFilter,
    )

    def list(self, request, *args, **kwargs):
        # queryset = super(AboutUsAPIView, self).get_queryset()
        try:
            about_us = AboutUs.objects.filter(status=True).values()
            list_data = []
            for item in about_us:
                if item:
                    serializer = AboutUsSerializer(item, partial=True,
                                                   context={'request': request}, )
                    list_data.append(serializer.data)
                else:
                    return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            return Response(list_data)
        except AboutUs.DoesNotExist:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)

    def retrieve(self, request, *args, **kwargs):
        try:
            about_us = AboutUs.objects.all().values()
            list_data = []
            for item in about_us:
                if item:
                    serializer = AboutUsSerializer(item, partial=True,
                                                   context={'request': request}, )
                    list_data.append(serializer.data)
                else:
                    return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            return Response(list_data)
        except AboutUs.DoesNotExist:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)

    def create(self, request, *args, **kwargs):
        try:
            list_data = []
            for item in request.data:
                data = AboutUsSerializer(data=item, context={'request': request})
                if data.is_valid():
                    data.save()
                    action_log(request, [data.data], 1, '', self.model, field_name='title')
                    list_data.append(data.data)
                else:
                    return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            return Response(list_data)
        except:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def get_permissions(self):
        if self.action in ('update', 'create', 'destroy', ):
            return [core_permissions.IsSuperUserEnabledAuthenticated()]
        return []

    def update(self, request, *args, **kwargs):
        try:
            list_data = []
            change_data = []
            for item in request.data:
                about_us = AboutUs.objects.get(pk=item['id'])
                data = AboutUsSerializer(instance=about_us, data=item, partial=True,
                                         context={'request': request})

                if data.is_valid():
                    change_message = changed_about_us_content_fields(about_us, item)
                    if len(change_message) > 0:
                        change_data.append(change_message)
                    data.save()
                    list_data.append(data.data)
                else:
                    return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            if len(change_data) > 0:
                change_data = list(itertools.chain(*change_data))
                change_data = list(set(change_data))
                remove_item = ["created", "modified"]
                for field in remove_item:
                    if field in change_data:
                        change_data.remove(field)
                action_log(request, [about_us], 2, change_data, self.model, field_name='title')
            return Response(list_data)
        except AboutUs.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def destroy(self, request, *args, **kwargs):
        try:
            if len(request.data['id']) > 0:
                if AboutUs.objects.filter(id__in=request.data['id']).exists():
                    about_us = AboutUs.objects.filter(id__in=request.data['id'])
                    if about_us:
                        action_log(request, about_us, 3, "About Us deleted", self.model,
                                   field_name='title')
                        about_us.delete()
                        return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)
        except:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
