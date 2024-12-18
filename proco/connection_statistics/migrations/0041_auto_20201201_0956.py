# Generated by Django 2.2.15 on 2020-12-01 09:56

from django.db import migrations


def update_countries_statuses(apps, schema_editor):
    Country = apps.get_model('locations', 'Country')

    for country in Country.objects.all():
        if country.data_source and country.data_source.lower() == 'osm':
            country.last_weekly_status.integration_status = 5
            country.last_weekly_status.save(update_fields=('integration_status',))


class Migration(migrations.Migration):

    dependencies = [
        ('connection_statistics', '0040_auto_20201130_1516'),
        ('locations', '0010_auto_20201120_1407'),
    ]

    operations = [
        #migrations.RunPython(update_countries_statuses, migrations.RunPython.noop),
    ]
