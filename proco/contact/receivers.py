from django.conf import settings
from django.db.models.signals import post_save
from django.dispatch import receiver

from constance import config
from templated_email import send_templated_mail

from proco.contact.models import ContactMessage


@receiver(post_save, sender=ContactMessage)
def send_email_notification(instance, created=False, **kwargs):
    if created and len(config.CONTACT_EMAIL) > 0:
        send_templated_mail(
            '/contact_email', settings.DEFAULT_FROM_EMAIL, config.CONTACT_EMAIL, context={
                'contact_message': instance,
                'project_title': settings.PROJECT_FULL_NAME or settings.PROJECT_SHORT_NAME,
            },
        )
