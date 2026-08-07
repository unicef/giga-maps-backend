import os

from django.conf import settings

from celery import Celery
from celery.schedules import crontab

if not settings.configured:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.dev')

app = Celery('proco')

app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
app.conf.timezone = 'UTC'
app.conf.broker_transport_options = {"visibility_timeout": 36000}  # 10h
app.conf.worker_deduplicate_successful_tasks = True
app.conf.redbeat_key_prefix = 'gigamaps:'
app.conf.redbeat_lock_timeout = 36000


@app.on_after_finalize.connect
def finalize_setup(sender, **kwargs):

    app.conf.beat_schedule.update({
        # TODO: Comment out once entity code is deployed with new FE
        # Old cache and rebuild school index task
        'proco.utils.tasks.update_all_cached_values': {
            'task': 'proco.utils.tasks.update_all_cached_values',
            'schedule': crontab(hour=4, minute=45),
            'args': (),
            'kwargs': {'clean_cache': True},
        },
        'proco.utils.tasks.rebuild_school_index': {
            'task': 'proco.utils.tasks.rebuild_school_index',
            'schedule': crontab(hour=2, minute=0),
            'args': (),
        },
        'proco.schools.tasks.update_school_records': {
            'task': 'proco.schools.tasks.update_school_records',
            'schedule': crontab(hour=1, minute=0),
            'args': (),
        },
        'proco.data_sources.tasks.cleanup_school_master_rows': {
            'task': 'proco.data_sources.tasks.cleanup_school_master_rows',
            'schedule': crontab(hour='1,15', minute=40),
            'args': (),
        },
        'proco.data_sources.tasks.update_static_data': {
            'task': 'proco.data_sources.tasks.update_static_data',
            # Executes at 4:00 AM every day
            'schedule': crontab(hour='*/4', minute=47),
            'args': (),
        },
        'proco.data_sources.tasks.update_live_data': {
            'task': 'proco.data_sources.tasks.update_live_data',
            'schedule': crontab(hour='2,8,14,20', minute=10),
            'args': (),
            'kwargs': {'today': True},
        },
        'proco.data_sources.tasks.update_live_data_and_aggregate_yesterday_data': {
            'task': 'proco.data_sources.tasks.update_live_data',
            'schedule': crontab(hour=0, minute=30),
            'args': (),
            'kwargs': {'today': False},
        },
        'proco.data_sources.tasks.update_qos_data_and_aggregate_yesterday_data': {
            'task': 'proco.data_sources.tasks.update_qos_data',
            'schedule': crontab(hour=4, minute=0),
            'args': (),
            'kwargs': {'today': False},
        },
        'proco.utils.tasks.populate_school_registration_data': {
            'task': 'proco.utils.tasks.populate_school_registration_data',
            'schedule': crontab(hour='2,8,14,20', minute=50),
            'args': (),
        },
        'proco.data_sources.tasks.handle_published_school_master_data_row': {
            'task': 'proco.data_sources.tasks.handle_published_school_master_data_row',
            # Executes every 4 hours
            'schedule': crontab(hour='*/4', minute=27),
            'args': (),
        },
        'proco.data_sources.tasks.handle_deleted_school_master_data_row': {
            'task': 'proco.data_sources.tasks.handle_deleted_school_master_data_row',
            # Executes every 4 hours
            'schedule': crontab(hour='*/4', minute=17),
            'args': (),
        },
        'proco.data_sources.tasks.email_reminder_to_editor_and_publisher_for_review_waiting_records': {
            'task': 'proco.data_sources.tasks.email_reminder_to_editor_and_publisher_for_review_waiting_records',
            # Executes once in a day at 8:10 AM
            'schedule': crontab(hour=8, minute=10),
            'args': (),
        },
        'proco.data_sources.tasks.clean_old_live_data': {
            'task': 'proco.data_sources.tasks.clean_old_live_data',
            'schedule': crontab(hour=5, minute=10),
            'args': (),
        },
        'proco.data_sources.tasks.clean_historic_data': {
            'task': 'proco.data_sources.tasks.clean_historic_data',
            'schedule': crontab(day_of_week='0,6', hour=5, minute=20),
            'args': (),
        },
        'proco.giga_meter.tasks.handle_giga_meter_school_master_data_sync': {
            'task': 'proco.giga_meter.tasks.handle_giga_meter_school_master_data_sync',
            # Executes once in a day at 8:30 PM
            'schedule': crontab(hour=20, minute=30),
            'args': (),
        },
        'proco.giga_meter.tasks.fetch_and_aggregate_ping_data': {
            'task': 'proco.giga_meter.tasks.fetch_and_aggregate_ping_data',
            'schedule': crontab(minute=15, hour='9,15,21,23'),
            'args': (),
        },
        'proco.giga_meter.tasks.scheduler_for_backup_giga_meter_connectivity_ping_data': {
            'task': 'proco.giga_meter.tasks.scheduler_for_backup_giga_meter_connectivity_ping_data',
            # Executes once in a day at 9:30 PM
            'schedule': crontab(hour=21, minute=30),
            'args': (),
        },

        # Entity Based Tasks
        'proco.utils.tasks.update_all_entity_cached_values': {
            'task': 'proco.utils.tasks.update_all_entity_cached_values',
            'schedule': crontab(hour=4, minute=45),
            'args': (),
            'kwargs': {'clean_cache': True},
        },
        'proco.utils.tasks.rebuild_unified_index': {
            'task': 'proco.utils.tasks.rebuild_unified_index',
            'schedule': crontab(hour=2, minute=0),
            'args': (),
        },
        'proco.data_sources.tasks.update_entity_static_data': {
            'task': 'proco.data_sources.tasks.update_entity_static_data',
            # Executes at 4:00 AM every day
            'schedule': crontab(hour='*/4', minute=52),
            'args': (),
        },
        'proco.data_sources.tasks.handle_published_entity_master_data_row': {
            'task': 'proco.data_sources.tasks.handle_published_entity_master_data_row',
            # Executes every 4 hours
            'schedule': crontab(hour='*/4', minute=27),
            'args': (),
        },
        'proco.data_sources.tasks.update_entity_live_data_from_giga_meter': {
            'task': 'proco.data_sources.tasks.update_entity_live_data_from_giga_meter',
            # Executes 3 times a day at 10:30 AM, 4:30 PM, 10:30 PM
            'schedule': crontab(minute=30, hour='10,16,22'),
            'args': (),
        },
        'proco.data_sources.tasks.update_entity_qos_data': {
            'task': 'proco.data_sources.tasks.update_entity_qos_data',
            # Executes once a day at 5:00 AM
            'schedule': crontab(hour=5, minute=0),
            'args': (),
            'kwargs': {'today': False},
        },
        'proco.utils.tasks.populate_entity_registration_data': {
            'task': 'proco.utils.tasks.populate_entity_registration_data',
            # Executes 4 times daily (offset from school equivalent by 5 minutes)
            'schedule': crontab(hour='2,8,14,20', minute=55),
            'args': (),
        },
        'proco.utils.tasks.update_entity_records': {
            'task': 'proco.utils.tasks.update_entity_records',
            # Executes twice daily at 1:30 AM and 1:30 PM (offset from school equivalent)
            'schedule': crontab(hour='1,13', minute=30),
            'args': (),
        },
        'proco.utils.tasks.handle_deleted_entity_master_data_row': {
            'task': 'proco.utils.tasks.handle_deleted_entity_master_data_row',
            # Executes every 4 hours at :22
            'schedule': crontab(hour='*/4', minute=22),
            'args': (),
        },
        'proco.data_sources.tasks.cleanup_health_entity_master_rows': {
            'task': 'proco.data_sources.tasks.cleanup_health_entity_master_rows',
            'schedule': crontab(hour='1,15', minute=45),
            'args': (),
        },
    })

