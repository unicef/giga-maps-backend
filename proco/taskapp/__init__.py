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
        'proco.utils.tasks.update_all_cached_values': {
            'task': 'proco.utils.tasks.update_all_cached_values',
            'schedule': crontab(hour=4, minute=0),
            'args': (),
        },
        'proco.utils.tasks.rebuild_school_index': {
            'task': 'proco.utils.tasks.rebuild_school_index',
            'schedule': crontab(hour=2, minute=0),
            'args': (),
        },
        # New
        'proco.schools.tasks.update_school_records': {
            'task': 'proco.schools.tasks.update_school_records',
            'schedule': crontab(hour='1,13', minute=0),
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
        'proco.utils.tasks.populate_school_registration_data': {
            'task': 'proco.utils.tasks.populate_school_registration_data',
            'schedule': crontab(hour=2, minute=40),
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
    })
