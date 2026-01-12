from django.db.models.signals import post_save
from django.dispatch import receiver

from proco.connection_statistics.models import EntityWeeklyStatus
from proco.entities.models import HealthMasterStatus

@receiver(post_save, sender=EntityWeeklyStatus)
def update_entity_last_weekly_status(instance, created=False, **kwargs):
    entity = instance.entity
    entity_last_status = entity.last_weekly_status
    if not entity_last_status:
        entity.last_weekly_status = instance
        entity.save()
    elif entity_last_status.date < instance.date:
        entity.last_weekly_status = instance
        entity.save()


@receiver(post_save, sender=HealthMasterStatus)
def update_entity_last_master_status_id(instance, created=False, **kwargs):
    if created:
        entity = instance.entity
        entity_last_status_id = entity.last_master_status_id
        if not entity_last_status_id:
            entity.last_master_status_id = instance.id
            entity.save()
        elif entity_last_status_id < instance.id:
            entity.last_master_status_id = instance.id
            entity.save()
