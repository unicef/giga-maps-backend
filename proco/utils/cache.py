from functools import wraps

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from django.utils.cache import add_never_cache_headers, patch_cache_control

from proco.utils.tasks import update_cached_value


class SoftCacheManager(object):
    CACHE_PREFIX = 'SOFT_CACHE'

    def get(self, key):
        value = cache.get('{0}_{1}'.format(self.CACHE_PREFIX, key), None)

        if value:
            if (
                (value['expired_at'] and value['expired_at'] < timezone.now().timestamp())
                or value.get('invalidated', True)
            ) and value.get('request_path', None):
                update_cached_value.delay(url=value['request_path'])
            return value['value']

    def _invalidate(self, key):
        value = cache.get(key, None)
        if value:
            value['invalidated'] = True
            cache.set(key, value, None)

    def invalidate_many(self, keys, hard=False):
        for key in keys:
            self.invalidate(key, hard=False)

    def invalidate(self, key='*', hard=False):
        if hard:
            if isinstance(key, str):
                keys = cache.keys('{0}_{1}'.format(self.CACHE_PREFIX, key))
                for key in keys:
                    cache.delete(key)
            elif isinstance(key, (list, tuple)):
                for k in key:
                    cache.delete(k)
        else:
            if isinstance(key, str):
                keys = cache.keys('{0}_{1}'.format(self.CACHE_PREFIX, key))
                for key in keys:
                    self._invalidate(key)
            elif isinstance(key, (list, tuple)):
                self.invalidate_many(key)

    def set(self, key, value, request_path=None, soft_timeout=settings.CACHES['default']['TIMEOUT']):
        cache.set('{0}_{1}'.format(self.CACHE_PREFIX, key), {
            'value': value,
            'invalidated': False,
            'request_path': request_path,
            'expired_at': (timezone.now().timestamp() + soft_timeout) if soft_timeout else None,
        }, None)


cache_manager = SoftCacheManager()

no_expiry_cache_manager = SoftCacheManager()
no_expiry_cache_manager.CACHE_PREFIX = settings.NO_EXPIRY_CACHE_PREFIX


def custom_cache_control(**kwargs):
    def _cache_controller(viewfunc):
        @wraps(viewfunc)
        def _cache_controlled(request, *args, **kw):
            response = viewfunc(request, *args, **kw)
            if 'cache_status_codes' in kwargs:
                if response.status_code in kwargs['cache_status_codes']:
                    patch_cache_control(response, **kwargs)
                else:
                    add_never_cache_headers(response)
            else:
                patch_cache_control(response, **kwargs)
            return response
        return _cache_controlled
    return _cache_controller
