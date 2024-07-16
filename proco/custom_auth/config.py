class AppConfig(object):

    @property
    def custom_role_count_limit(self):
        """custom role count limit"""
        return 50


app_config = AppConfig()
