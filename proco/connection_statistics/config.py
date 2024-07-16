
class AppConfig(object):

    DAILY_CHECK_APP_MLAB_SOURCE = 'DAILY_CHECK_APP_MLAB'
    QOS_SOURCE = 'QOS'
    DAILY_CHECK_APP_MLAB_QOS_SOURCE = 'DAILY_CHECK_APP_MLAB_QOS'
    UNKNOWN_SOURCE = 'UNKNOWN'

    LIVE_DATA_SOURCE_CHOICES = (
        (DAILY_CHECK_APP_MLAB_SOURCE, 'Daily Check App/MLab'),
        (QOS_SOURCE, 'QoS'),
        (DAILY_CHECK_APP_MLAB_QOS_SOURCE, 'Daily Check APP/MLab/QoS'),
        (UNKNOWN_SOURCE, 'Unknown'),
    )

    SCHOOL_MASTER_SOURCE = 'SCHOOL_MASTER'

    STATIC_DATA_SOURCE_CHOICES = (
        (SCHOOL_MASTER_SOURCE, 'School Master Data Source'),
    )


app_config = AppConfig()
