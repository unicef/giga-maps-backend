from rest_framework import serializers

from proco.about_us.models import AboutUs, SliderImage


class SliderImageSerializer(serializers.ModelSerializer):
    class Meta:
        model = SliderImage
        fields = "__all__"


class AboutUsSerializer(serializers.ModelSerializer):
    text = serializers.JSONField()
    cta = serializers.JSONField()
    content = serializers.JSONField()

    class Meta:
        model = AboutUs
        fields = "__all__"
