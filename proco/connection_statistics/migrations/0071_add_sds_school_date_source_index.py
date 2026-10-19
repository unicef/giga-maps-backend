# -*- coding: utf-8 -*-

from django.db import migrations


class Migration(migrations.Migration):

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    atomic = False

    dependencies = [
        ('connection_statistics', '0070_auto_20260403_0547'),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS sds_school_date_source_idx '
                'ON connection_statistics_schooldailystatus (school_id, date, live_data_source) '
                'WHERE deleted IS NULL;'
            ),
            reverse_sql='DROP INDEX CONCURRENTLY IF EXISTS sds_school_date_source_idx;',
        ),
    ]
