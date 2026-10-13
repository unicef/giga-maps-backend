from django.contrib.postgres.fields import JSONField
from django.core.validators import FileExtensionValidator
from django.db import models

from proco.about_us.config import app_config
from proco.locations.utils import get_random_name_image


class AboutUs(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    style = models.TextField(blank=True, null=True)
    text = JSONField(default=list)
    image = models.CharField(max_length=255, blank=True, null=True)
    type = models.CharField(max_length=255, blank=True, null=True)
    status = models.BooleanField(default=False)
    order = models.IntegerField(blank=True, null=True)
    cta = JSONField(default=dict)
    content = JSONField(default=list)

    objects = models.Manager()

    def __str__(self):
        return self.type


class SliderImage(models.Model):
    name = models.CharField(max_length=200, null=True)
    # FileField rather than ImageField: the landing page sections carry short videos as well as
    # stills, and ImageField rejects them at Pillow verification. The field name stays `image`
    # because the About Us payload exposes it as `image` on every section.
    image = models.FileField(
        upload_to=get_random_name_image,
        blank=True,
        null=True,
        validators=[
            FileExtensionValidator(allowed_extensions=app_config.slide_media_allowed_extensions),
        ],
    )

    objects = models.Manager()

    def __str__(self):
        return self.name
