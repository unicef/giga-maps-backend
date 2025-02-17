# Generated by Django 2.2.28 on 2024-04-22 10:59

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('connection_statistics', '0053_latency_field_conversion_from_int_to_float'),
    ]

    operations = [
        migrations.AlterField(
            model_name='countrydailystatus',
            name='roundtrip_time',
            field=models.FloatField(blank=True, default=None, help_text='ms', null=True),
        ),
        migrations.AlterField(
            model_name='countryweeklystatus',
            name='roundtrip_time',
            field=models.FloatField(blank=True, default=None, help_text='ms', null=True),
        ),
        migrations.AlterField(
            model_name='realtimeconnectivity',
            name='roundtrip_time',
            field=models.FloatField(blank=True, default=None, help_text='ms', null=True),
        ),
        migrations.AlterField(
            model_name='schooldailystatus',
            name='roundtrip_time',
            field=models.FloatField(blank=True, default=None, help_text='ms', null=True),
        ),
        migrations.AlterField(
            model_name='schoolweeklystatus',
            name='roundtrip_time',
            field=models.FloatField(blank=True, default=None, help_text='ms', null=True),
        ),
    ]
