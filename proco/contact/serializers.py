from rest_framework import serializers

from proco.contact.models import ContactMessage


class ContactSerializer(serializers.ModelSerializer):
    class Meta:
        model = ContactMessage
        fields = '__all__'
