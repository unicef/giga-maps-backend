# Generated by Django 2.2.28 on 2024-03-04 13:44

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('about_us', '0002_auto_20240304_1211'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='aboutus',
            name='status',
        ),
    ]
