# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('proco_data_migrations', '0004_drop_tables_for_realtime_dailycheckapp_and_realtime_unicef'),
    ]

    operations = [
        migrations.RunSQL(
            sql='CREATE INDEX IF NOT EXISTS schools_school_school_type_lower_casing ON public.schools_school(lower(school_type))',
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.RunSQL(
            sql='CREATE INDEX IF NOT EXISTS schools_school_education_level_lower_casing ON public.schools_school(lower(education_level))',
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.RunSQL(
            sql='CREATE INDEX IF NOT EXISTS schoolweeklystatus_connectivity_type_lower_casing ON public.connection_statistics_schoolweeklystatus(lower(connectivity_type))',
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]
