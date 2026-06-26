from django.db.models.signals import post_save
from django.dispatch import receiver

from proco.connection_statistics.models import EntityWeeklyStatus
from proco.data_sources.models import HealthEntityMasterIntermediateData
from proco.schools.constants import statuses_schema


def get_entity_connectivity_status(entity_weekly_status):
    if entity_weekly_status.connectivity_speed is None:
        return 'unknown'
    return statuses_schema.get_connectivity_status_by_connectivity_speed(
        entity_weekly_status.connectivity_speed
    )


@receiver(post_save, sender=EntityWeeklyStatus)
def update_entity_last_weekly_status(instance, created=False, **kwargs):
    entity = instance.entity
    entity_last_status = entity.last_weekly_status
    update_fields = []

    if not entity_last_status:
        entity.last_weekly_status = instance
        update_fields.append('last_weekly_status')
    elif entity_last_status.date < instance.date:
        entity.last_weekly_status = instance
        update_fields.append('last_weekly_status')

    if entity.last_weekly_status_id == instance.id:
        connectivity_status = get_entity_connectivity_status(instance)
        if entity.connectivity_status != connectivity_status:
            entity.connectivity_status = connectivity_status
            update_fields.append('connectivity_status')

    if update_fields:
        entity.save(update_fields=update_fields)


@receiver(post_save, sender=HealthEntityMasterIntermediateData)
def update_entity_last_master_status_id(instance, created=False, **kwargs):
    if created:
        entity = instance.entity
        if entity:
            entity_last_status_id = entity.last_master_status_id
            if not entity_last_status_id:
                entity.last_master_status_id = instance.id
                entity.save()
            elif entity_last_status_id < instance.id:
                entity.last_master_status_id = instance.id
                entity.save()
