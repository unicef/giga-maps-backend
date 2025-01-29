class AppConfig(object):

    @property
    def app_name(self):
        """Name of the app giga_meter"""
        return 'giga_meter'


app_config = AppConfig()
