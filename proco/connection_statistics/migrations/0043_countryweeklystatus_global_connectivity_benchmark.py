# Generated by Django 2.2.19 on 2023-08-25 04:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('connection_statistics', '0042_auto_20220922_1338'),
    ]

    operations = [
        migrations.AddField(
            model_name='countryweeklystatus',
            name='global_schools_connectivity_good',
            field=models.PositiveIntegerField(blank=True, default=0),
        ),
        migrations.AddField(
            model_name='countryweeklystatus',
            name='global_schools_connectivity_moderate',
            field=models.PositiveIntegerField(blank=True, default=0),
        ),
        migrations.AddField(
            model_name='countryweeklystatus',
            name='global_schools_connectivity_no',
            field=models.PositiveIntegerField(blank=True, default=0),
        ),
        migrations.AddField(
            model_name='countryweeklystatus',
            name='global_schools_connectivity_unknown',
            field=models.PositiveIntegerField(blank=True, default=0),
        ),
    ]
