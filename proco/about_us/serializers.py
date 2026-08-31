from django.core.validators import FileExtensionValidator

from rest_framework import serializers

from proco.about_us.config import app_config
from proco.about_us.models import AboutUs, SliderImage


class SliderImageSerializer(serializers.ModelSerializer):
    # Declared explicitly so the field maps to `serializers.FileField` instead of the
    # `serializers.ImageField` that `fields = "__all__"` would infer. The kwargs mirror what the
    # implicit mapping produced, so required/allow_null/max_length behaviour is unchanged.
    image = serializers.FileField(
        max_length=100,
        required=False,
        allow_null=True,
        validators=[
            FileExtensionValidator(allowed_extensions=app_config.slide_media_allowed_extensions),
        ],
    )

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
