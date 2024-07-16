from django.apps import AppConfig


class AboutUsConfig(AppConfig):
    name = 'proco.about_us'
    verbose_name = 'About US'

    def ready(self):
        from proco.about_us import receivers  # NOQA
