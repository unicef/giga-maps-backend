class AppConfig(object):

    @property
    def app_name(self):
        """Name of the app daily_check_app"""
        return 'daily_check_app'


app_config = AppConfig()
