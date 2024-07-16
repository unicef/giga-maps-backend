# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from django.db import migrations

from proco.proco_data_migrations.management_utils.user_role_permissions import \
    populate_user_roles_and_permissions


class Migration(migrations.Migration):

    dependencies = [
        # ('accounts', '0001_initial_account_models'),
        ('proco_data_migrations', '0002_master_data_migrations_school_update'),
    ]

    operations = [
        migrations.RunPython(populate_user_roles_and_permissions),
    ]
