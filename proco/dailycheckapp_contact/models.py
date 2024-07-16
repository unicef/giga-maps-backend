from django.db import models

from model_utils.models import TimeStampedModel


class ContactMessage(TimeStampedModel, models.Model):
    firstname = models.CharField(max_length=256)
    lastname = models.CharField(max_length=256)
    school_id = models.CharField(max_length=256)
    email = models.CharField(max_length=256)
    message = models.TextField()

    class Meta:
        db_table = 'dailycheckapp_contact_contactmessages'

    def __str__(self):
        return 'Message from: {0} ({1})'.format(self.firstname, self.created)
