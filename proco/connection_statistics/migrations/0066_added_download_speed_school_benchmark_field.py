# Generated by Django 2.2.28 on 2024-08-23 12:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('connection_statistics', '0065_added_new_school_master_fields_for_honduras'),
    ]

    operations = [
        migrations.AddField(
            model_name='schoolweeklystatus',
            name='download_speed_benchmark',
            field=models.FloatField(blank=True, default=None, null=True),
        ),
    ]