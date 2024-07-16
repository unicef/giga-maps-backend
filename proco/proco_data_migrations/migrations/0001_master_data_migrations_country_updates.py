# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
from datetime import datetime, timedelta

from django.db import migrations, models

from proco.connection_statistics.models import CountryWeeklyStatus
from proco.locations.models import Country
from proco.schools.models import School, FileImport


def fill_data_source(apps, schema_editor):
    for file in FileImport.objects.filter(country__isnull=False):
        source = re.search(r'\d+-(.*)-\d+', file.uploaded_file.name.split('/')[-1])

        if source:
            pretty_source = source.group(1).replace('_', ' ')
            if file.country.data_source:
                if pretty_source.lower() not in file.country.data_source.lower():
                    file.country.data_source += f'\n{pretty_source}'
            else:
                file.country.data_source = pretty_source

            file.country.save()


def fill_date(apps, schema_editor):
    for obj in CountryWeeklyStatus.objects.all():
        obj.date = datetime.strptime(f'{obj.year}-W{obj.week}-1', "%Y-W%W-%w")
        fill_schools_with_data_percentage_field(obj)


def get_start_and_end_date_from_calendar_week(year, calendar_week):
    monday = datetime.strptime(f'{year}-{calendar_week}-1', "%Y-%W-%w").date()
    return monday, monday + timedelta(days=6.9)


def fill_date_of_join(apps, schema_editor):
    for country in Country.objects.filter(weekly_status__isnull=False):
        weekly_status = CountryWeeklyStatus.objects.filter(country=country).order_by('country_id', 'year',
                                                                                     'week').first()
        if weekly_status:
            country.date_of_join = weekly_status.date
            country.save()


def fill_schools_with_data_percentage_field(country_weekly):
    week_start, week_end = get_start_and_end_date_from_calendar_week(country_weekly.year, country_weekly.week)

    schools_number = School.objects.filter(created__lte=week_end, country=country_weekly.country).count()

    if schools_number:
        schools_with_data_number = School.objects.filter(
            weekly_status__week=country_weekly.week, country=country_weekly.country,
            weekly_status__year=country_weekly.year,
        ).distinct('id').count()

        country_weekly.schools_with_data_percentage = 1.0 * schools_with_data_number / schools_number
        country_weekly.save()


def set_last_weekly_status(apps, schema_editor):
    Country.objects.annotate(
        last_status_id=models.Subquery(
            CountryWeeklyStatus.objects.filter(
                country=models.OuterRef('id'),
            ).order_by('-year', '-week', '-id')[:1].values('id'),
        ),
    ).update(last_weekly_status_id=models.F('last_status_id'))


class Migration(migrations.Migration):
    dependencies = [
        ('schools', '0022_auto_20230816_0603'),
    ]

    operations = [
        # migrations.RunPython(fill_data_source, migrations.RunPython.noop),
        # migrations.RunPython(fill_date, migrations.RunPython.noop),
        # migrations.RunPython(fill_date_of_join, migrations.RunPython.noop),
        # migrations.RunPython(set_last_weekly_status, migrations.RunPython.noop),
    ]
