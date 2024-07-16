from django.contrib.auth.models import UserManager

from proco.core.managers import BaseManager


class CustomUserManager(UserManager, BaseManager):
    def get_by_natural_key(self, username):
        return self.get(username=username.lower())
