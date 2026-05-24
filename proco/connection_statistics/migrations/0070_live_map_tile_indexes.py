from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ('connection_statistics', '0069_increased_upload_field_size'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rt_reg_date_school
            ON connection_statistics_schoolrealtimeregistration (
                rt_registration_date,
                school_id
            )
            WHERE deleted IS NULL
              AND rt_registered = true;
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS idx_rt_reg_date_school;
            """,
        ),
        migrations.RunSQL(
            sql="""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sds_school_date_src_m
            ON connection_statistics_schooldailystatus (
                school_id,
                date,
                live_data_source
            )
            INCLUDE (
                connectivity_speed,
                connectivity_upload_speed,
                connectivity_latency,
                connectivity_speed_probe,
                connectivity_latency_probe,
                connectivity_speed_mean,
                connectivity_upload_speed_mean,
                roundtrip_time,
                jitter_download
            )
            WHERE deleted IS NULL;
            """,
            reverse_sql="""
            DROP INDEX CONCURRENTLY IF EXISTS idx_sds_school_date_src_m;
            """,
        ),
    ]
