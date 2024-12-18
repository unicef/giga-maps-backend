# Generated by Django 2.2.28 on 2024-06-05 07:07

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('background', '0003_added_deleted_bu_field'),
    ]

    operations = [
        migrations.AddConstraint(
            model_name='backgroundtask',
            constraint=models.UniqueConstraint(fields=('name', 'status', 'deleted'),
                                               name='background_task_unique_with_deleted'),
        ),
        migrations.AddConstraint(
            model_name='backgroundtask',
            constraint=models.UniqueConstraint(condition=models.Q(deleted=None), fields=('name', 'status'),
                                               name='background_task_unique_without_deleted'),
        ),
    ]
