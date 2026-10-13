""" Config file to specify application configurations used in the About Us app"""


class AppConfig(object):

    @property
    def slide_media_allowed_extensions(self):
        """
        slide_media_allowed_extensions
            Returns the file extensions accepted by the slide media uploader. Images and videos
            share the single `image` field on SliderImage, so both are listed here.
        :return: list of str
        """
        return ['png', 'jpg', 'jpeg', 'svg', 'mp4', 'webm']


app_config = AppConfig()
