from django.contrib.admin.models import LogEntry
from rest_framework import serializers

from proco.background.models import BackgroundTask
from proco.core.utils import is_blank_string


class BackgroundTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = BackgroundTask
        fields = '__all__'


class BackgroundTaskHistorySerializer(serializers.ModelSerializer):
    username = serializers.SerializerMethodField()

    class Meta:
        model = LogEntry
        fields = ('action_time', 'user', 'change_message', 'username',)

    def get_username(self, instance):
        user_name = None
        if instance.user:
            user_name = instance.user.first_name
            if not is_blank_string(instance.user.last_name):
                user_name += ' ' + instance.user.last_name
        return user_name
