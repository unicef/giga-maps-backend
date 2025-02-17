from django.urls import resolve, reverse

from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate


class APITestCaseMixin(object):
    def forced_auth_req(self, method, url, user=None, data=None, request_format='json', **kwargs):
        """
        Function that allows api methods to be called with forced authentication

        If `user` parameter not provided, then `self.user` will be used

        If `view` parameter is provided, then the `view` function
        will be called directly, otherwise `url` will be resolved
        """
        factory = APIRequestFactory()

        data = data or {}
        req_to_call = getattr(factory, method)
        request = req_to_call(url, data, format=request_format, **kwargs)
        request.session = {}

        if user:
            force_authenticate(request, user=user)

        if 'view' in kwargs:
            view = kwargs.pop('view')
            if 'view_info' in kwargs:
                view_info = kwargs.pop('view_info')
                response = view_info(request, *view.args, **view.kwargs)
            else:
                response = view(request)
        else:
            view_info = resolve(url)
            view = view_info.func
            response = view(request, *view_info.args, **view_info.kwargs)

        if hasattr(response, 'render'):
            response.render()

        return response


class TestAPIViewSetMixin(APITestCaseMixin):
    base_view = ''

    def get_list_args(self):
        return []

    def get_detail_args(self, instance):
        return self.get_list_args() + [instance.pk]

    def get_action_url(self, action, instance=None):
        url_args = self.get_detail_args(instance) if instance else self.get_list_args()
        return reverse('{0}-{1}'.format(self.base_view, action), args=url_args)

    def get_list_url(self):
        return self.get_action_url('list')

    def get_detail_url(self, instance):
        return self.get_action_url('detail', instance=instance)

    def make_request_to_viewset(self, user, method='get', instance=None, action=None, data=None, **kwargs):
        if action:
            url = self.get_action_url(action, instance=instance)
        else:
            url = self.get_detail_url(instance) if instance else self.get_list_url()

        data = data or {}

        return self.forced_auth_req(method, url, user=user, data=data, **kwargs)

    def make_list_request(self, user, **kwargs):
        return self.make_request_to_viewset(user, **kwargs)

    def make_detail_request(self, user, instance, **kwargs):
        return self.make_request_to_viewset(user, instance=instance, **kwargs)

    def _test_list(self, user, expected_objects=None, expected_status=status.HTTP_200_OK, data=None, **kwargs):
        response = self.make_list_request(user, data=data, **kwargs)
        self.assertEqual(response.status_code, expected_status)

        if response.data:
            results = response.data['results'] if 'results' in response.data else response.data

        if expected_status == status.HTTP_200_OK:
            self.assertListEqual(
                sorted(obj['id'] for obj in results),
                sorted(obj.id for obj in (expected_objects or [])),
            )

        return response

    def _test_create(self, user, data, expected_status=status.HTTP_201_CREATED, errors=None, **kwargs):
        response = self.make_list_request(user, method='post', data=data, **kwargs)

        self.assertEqual(response.status_code, expected_status)

        if errors:
            self.assertListEqual(list(response.data['errors'].keys()), errors)

        return response

    def _test_retrieve(self, user, instance, expected_status=status.HTTP_200_OK, errors=None):
        response = self.make_detail_request(user, instance)

        self.assertEqual(response.status_code, expected_status)

        if errors:
            self.assertListEqual(list(response.data['errors'].keys()), errors)

        return response

    def _test_update(self, user, instance, data, expected_status=status.HTTP_200_OK, errors=None):
        response = self.make_detail_request(user, instance=instance, method='patch', data=data)

        self.assertEqual(response.status_code, expected_status)

        if errors:
            self.assertListEqual(list(response.data['errors'].keys()), errors)

        return response

    def _test_destroy(self, user, instance, expected_status=status.HTTP_204_NO_CONTENT):
        response = self.make_detail_request(user, instance, method='delete')

        self.assertEqual(response.status_code, expected_status)

        return response
