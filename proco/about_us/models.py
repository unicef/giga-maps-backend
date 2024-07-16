from django.contrib.postgres.fields import JSONField
from django.db import models
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
    image = models.ImageField(upload_to=get_random_name_image, blank=True, null=True)

    objects = models.Manager()

    def __str__(self):
        return self.name
