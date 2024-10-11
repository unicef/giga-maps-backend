from rest_framework import serializers

from proco.contact.models import ContactMessage
from proco.core.utils import is_blank_string


class ContactSerializer(serializers.ModelSerializer):
    full_name = serializers.SerializerMethodField()
    purpose = serializers.SerializerMethodField()

    class Meta:
        model = ContactMessage
        fields = '__all__'

    def get_full_name(self, instance):
        if is_blank_string(instance.email):
            return instance.full_name
        return instance.full_name + ' (' + instance.email + ')'

    def get_purpose(self, instance):
        return dict(ContactMessage.CATEGORY_CHOICES).get(instance.category) + ': ' + instance.purpose
