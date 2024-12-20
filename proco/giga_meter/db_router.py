from django.conf import settings

from proco.giga_meter.config import app_config


class GigaMeterDBRouter(object):
    """
    A router to control all database operations on models in the public_website application.
    """
    def db_for_read(self, model, **hints):
        """
        Suggest the database that should be used for read operations for objects of type model.
        """
        if model._meta.app_label == app_config.app_name:
            return settings.GIGA_METER_DB_KEY
        return None

    def db_for_write(self, model, **hints):
        """
        Suggest the database that should be used for read operations for objects of type model.
        """
        if model._meta.app_label == app_config.app_name:
            return settings.GIGA_METER_DB_KEY
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Determine if the migration operation is allowed to run on the database with alias db.
        Return True if the operation should run, False if it should not run, or None if the
        router has no opinion.
        . note::
            - This method is called when migrate command is executed.
        """
        if app_label == app_config.app_name:
            if db == settings.GIGA_METER_DB_KEY:
                return True
            return False
        else:
            return db == 'default'
