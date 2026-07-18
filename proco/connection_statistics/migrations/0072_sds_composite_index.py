# P1 (perf): composite partial index driving the live data-layer join/graph predicate
# (school_id, date, live_data_source) WHERE deleted IS NULL. Created CONCURRENTLY (via RunSQL,
# since Django 2.2 has no AddIndexConcurrently) so it does not lock writes on the large
# SchoolDailyStatus table. State-only AddIndex keeps Django's model state in sync.
from django.db import migrations, models


class Migration(migrations.Migration):

    atomic = False

    dependencies = [
        ('connection_statistics', '0071_school_weekly_rollup'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        'CREATE INDEX CONCURRENTLY IF NOT EXISTS sds_school_date_source_idx '
                        'ON connection_statistics_schooldailystatus (school_id, date, live_data_source) '
                        'WHERE deleted IS NULL;'
                    ),
                    reverse_sql='DROP INDEX CONCURRENTLY IF EXISTS sds_school_date_source_idx;',
                ),
            ],
            state_operations=[
                migrations.AddIndex(
                    model_name='schooldailystatus',
                    index=models.Index(
                        condition=models.Q(deleted__isnull=True),
                        fields=['school', 'date', 'live_data_source'],
                        name='sds_school_date_source_idx',
                    ),
                ),
            ],
        ),
    ]
