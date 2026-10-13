from django.apps import AppConfig


class EntitiesConfig(AppConfig):
    name = 'proco.entities'
    verbose_name = 'Entities'

    def ready(self):
        from proco.entities import signals  # NOQA
