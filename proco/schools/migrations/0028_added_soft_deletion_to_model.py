# Generated by Django 2.2.28 on 2024-03-11 13:08

from django.db import migrations, models
import proco.core.models


class Migration(migrations.Migration):

    dependencies = [
        ('schools', '0027_increased_school_name_length_to_1000'),
    ]

    operations = [
        migrations.AddField(
            model_name='school',
            name='deleted',
            field=proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True),
        ),
    ]
