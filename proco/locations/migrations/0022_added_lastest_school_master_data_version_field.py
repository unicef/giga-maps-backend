# Generated by Django 2.2.28 on 2024-12-26 12:23

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('locations', '0021_removed_geometry_simplified_field_from_country'),
    ]

    operations = [
        migrations.AddField(
            model_name='country',
            name='latest_school_master_data_version',
            field=models.PositiveIntegerField(blank=True, default=None, null=True),
        ),
    ]