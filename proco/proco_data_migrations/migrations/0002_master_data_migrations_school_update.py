# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from datetime import datetime

from django.db import migrations, models
from django.db.models.functions import Lower

from proco.connection_statistics.models import (
    CountryDailyStatus,
    CountryWeeklyStatus,
    RealTimeConnectivity,
    SchoolDailyStatus,
    SchoolWeeklyStatus, )
from proco.schools.models import School


def fill_new_fields(apps, schema_editor):
    School.objects.update(external_id=Lower('external_id'), name_lower=Lower('name'))


def fill_date(apps, schema_editor):
    for obj in SchoolWeeklyStatus.objects.all():
        obj.date = datetime.strptime(f'{obj.year}-W{obj.week}-1', "%Y-W%W-%w")


def migrate_speed_field(apps, schema_editor):
    models_list = [
        CountryWeeklyStatus, SchoolWeeklyStatus, CountryDailyStatus, SchoolDailyStatus, RealTimeConnectivity,
    ]

    for model in models_list:
        model.objects.update(connectivity_speed_in_bits=models.F('connectivity_speed') * (10 ** 6))


def set_last_status(apps, schema_editor):
    School.objects.annotate(
        last_status_id=models.Subquery(
            SchoolWeeklyStatus.objects.filter(
                school=models.OuterRef('id'),
            ).order_by('-year', '-week', '-id')[:1].values('id'),
        ),
    ).update(last_weekly_status_id=models.F('last_status_id'))


class Migration(migrations.Migration):
    dependencies = [
        ('proco_data_migrations', '0001_master_data_migrations_country_updates'),
    ]

    operations = [
        # migrations.RunPython(fill_new_fields, migrations.RunPython.noop),
        # migrations.RunPython(fill_date, migrations.RunPython.noop),
        # migrations.RunPython(migrate_speed_field, migrations.RunPython.noop),
        # migrations.RunPython(set_last_status, migrations.RunPython.noop),
    ]
