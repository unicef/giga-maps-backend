# Generated by Django 2.2.16 on 2020-10-23 08:31

from django.db import migrations, models


def remove_schools_outside_countries(apps, schema_editor):
    School = apps.get_model("schools", "School")
    School.objects.exclude(geopoint__within=models.F('country__geometry')).delete()


class Migration(migrations.Migration):

    dependencies = [
        ('schools', '0011_school_last_weekly_status'),
    ]

    operations = [
        # migrations.RunPython(remove_schools_outside_countries, migrations.RunPython.noop),
    ]
