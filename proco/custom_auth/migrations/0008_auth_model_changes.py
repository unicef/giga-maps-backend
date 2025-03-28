# Generated by Django 2.2.19 on 2023-10-26 14:46

from django.conf import settings
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import proco.core.models
import proco.custom_auth.managers
import simple_history.models


class Migration(migrations.Migration):

    dependencies = [
        ('custom_auth', '0007_auto_20210303_0838'),
    ]

    operations = [
        migrations.CreateModel(
            name='Role',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at', proco.core.models.CustomDateTimeField(auto_now=True, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(auto_now_add=True, verbose_name='Created Date')),
                ('name', models.CharField(db_index=True, max_length=255, verbose_name='Role')),
                ('description', models.CharField(blank=True, max_length=255, null=True)),
                ('category', models.CharField(choices=[('system', 'System Role'), ('custom', 'Custom Role')], default='system', max_length=50, verbose_name='Role')),
            ],
            options={
                'ordering': ['last_modified_at'],
                'abstract': False,
            },
        ),
        migrations.AlterModelOptions(
            name='applicationuser',
            options={},
        ),
        migrations.AlterModelManagers(
            name='applicationuser',
            managers=[
                ('objects', proco.custom_auth.managers.CustomUserManager()),
            ],
        ),
        migrations.RemoveField(
            model_name='applicationuser',
            name='groups',
        ),
        migrations.RemoveField(
            model_name='applicationuser',
            name='user_permissions',
        ),
        migrations.AddField(
            model_name='applicationuser',
            name='created',
            field=proco.core.models.CustomDateTimeField(auto_now_add=True, default=django.utils.timezone.now, verbose_name='Created Date'),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='applicationuser',
            name='created_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='created_applicationusers', to=settings.AUTH_USER_MODEL, verbose_name='Created By'),
        ),
        migrations.AddField(
            model_name='applicationuser',
            name='deleted',
            field=proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True),
        ),
        migrations.AddField(
            model_name='applicationuser',
            name='last_modified_at',
            field=proco.core.models.CustomDateTimeField(auto_now=True, verbose_name='Last Updated Date'),
        ),
        migrations.AddField(
            model_name='applicationuser',
            name='last_modified_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='updated_applicationusers', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By'),
        ),
        migrations.AlterField(
            model_name='applicationuser',
            name='countries_available',
            field=models.ManyToManyField(blank=True, help_text='Countries to which the user has access and the ability to manage them.', related_name='countries_available', to='locations.Country', verbose_name='Countries Available'),
        ),
        migrations.AlterField(
            model_name='applicationuser',
            name='date_joined',
            field=models.DateTimeField(default=django.utils.timezone.now, verbose_name='Date Joined'),
        ),
        migrations.AlterField(
            model_name='applicationuser',
            name='first_name',
            field=models.CharField(max_length=100, verbose_name='First Name'),
        ),
        migrations.AlterField(
            model_name='applicationuser',
            name='is_active',
            field=models.BooleanField(default=True, verbose_name='Active'),
        ),
        migrations.AlterField(
            model_name='applicationuser',
            name='is_superuser',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='applicationuser',
            name='last_name',
            field=models.CharField(max_length=100, verbose_name='Last Name'),
        ),
        migrations.CreateModel(
            name='UserRoleRelationship',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at', proco.core.models.CustomDateTimeField(auto_now=True, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(auto_now_add=True, verbose_name='Created Date')),
                ('created_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='created_userrolerelationships', to=settings.AUTH_USER_MODEL, verbose_name='Created By')),
                ('last_modified_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='updated_userrolerelationships', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By')),
                ('role', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, related_name='role_users', to='custom_auth.Role')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, related_name='roles', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'ordering': ['last_modified_at'],
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='RolePermission',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at', proco.core.models.CustomDateTimeField(auto_now=True, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(auto_now_add=True, verbose_name='Created Date')),
                ('slug', models.CharField(choices=[('can_view_country', 'Can View All Countries'), ('can_add_country', 'Can Add a Country'), ('can_update_country', 'Can Update a Country'), ('can_delete_country', 'Can Delete a Country'), ('can_view_user', 'Can View User'), ('can_add_user', 'Can Add User'), ('can_view_all_roles', 'Can View All Roles'), ('can_update_user_role', 'Can Update User Role'), ('can_view_role_configurations', 'Can View Role Configurations'), ('can_create_role_configurations', 'Can Create Role Configurations'), ('can_update_role_configurations', 'Can Update Role Configurations'), ('can_delete_role_configurations', 'Can Delete Role Configurations'), ('can_access_users_tab', 'Can Access User Management Tab'), ('can_delete_user', 'Can Delete a User'), ('can_update_user', 'Can Update a User'), ('can_add_user_relation', 'Can Add a New User Relation'), ('can_delete_user_relation', 'Can Delete a New User Relation'), ('can_update_user_relation', 'Can Add a New User Relation'), ('can_view_app_config', 'Can View App Configurations'), ('can_update_app_config', 'Can Update App Configurations'), ('can_view_notification', 'Can View Notification'), ('can_create_notification', 'Can Create Notification'), ('can_delete_notification', 'Can Delete Notification')], db_index=True, max_length=100)),
                ('created_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='created_rolepermissions', to=settings.AUTH_USER_MODEL, verbose_name='Created By')),
                ('last_modified_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='updated_rolepermissions', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By')),
                ('role', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, related_name='permissions', to='custom_auth.Role')),
            ],
            options={
                'ordering': ['last_modified_at'],
                'abstract': False,
            },
        ),
        migrations.AddField(
            model_name='role',
            name='created_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='created_roles', to=settings.AUTH_USER_MODEL, verbose_name='Created By'),
        ),
        migrations.AddField(
            model_name='role',
            name='last_modified_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='updated_roles', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By'),
        ),
        migrations.CreateModel(
            name='HistoricalUserRoleRelationship',
            fields=[
                ('id', models.IntegerField(auto_created=True, blank=True, db_index=True, verbose_name='ID')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Created Date')),
                ('history_id', models.AutoField(primary_key=True, serialize=False)),
                ('history_date', models.DateTimeField()),
                ('history_change_reason', models.CharField(max_length=100, null=True)),
                ('history_type', models.CharField(choices=[('+', 'Created'), ('~', 'Changed'), ('-', 'Deleted')], max_length=1)),
                ('created_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Created By')),
                ('history_user', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('last_modified_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By')),
                ('role', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to='custom_auth.Role')),
                ('user', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'historical user role relationship',
                'ordering': ('-history_date', '-history_id'),
                'get_latest_by': 'history_date',
            },
            bases=(simple_history.models.HistoricalChanges, models.Model),
        ),
        migrations.CreateModel(
            name='HistoricalRolePermission',
            fields=[
                ('id', models.IntegerField(auto_created=True, blank=True, db_index=True, verbose_name='ID')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Created Date')),
                ('slug', models.CharField(choices=[('can_view_country', 'Can View All Countries'), ('can_add_country', 'Can Add a Country'), ('can_update_country', 'Can Update a Country'), ('can_delete_country', 'Can Delete a Country'), ('can_view_user', 'Can View User'), ('can_add_user', 'Can Add User'), ('can_view_all_roles', 'Can View All Roles'), ('can_update_user_role', 'Can Update User Role'), ('can_view_role_configurations', 'Can View Role Configurations'), ('can_create_role_configurations', 'Can Create Role Configurations'), ('can_update_role_configurations', 'Can Update Role Configurations'), ('can_delete_role_configurations', 'Can Delete Role Configurations'), ('can_access_users_tab', 'Can Access User Management Tab'), ('can_delete_user', 'Can Delete a User'), ('can_update_user', 'Can Update a User'), ('can_add_user_relation', 'Can Add a New User Relation'), ('can_delete_user_relation', 'Can Delete a New User Relation'), ('can_update_user_relation', 'Can Add a New User Relation'), ('can_view_app_config', 'Can View App Configurations'), ('can_update_app_config', 'Can Update App Configurations'), ('can_view_notification', 'Can View Notification'), ('can_create_notification', 'Can Create Notification'), ('can_delete_notification', 'Can Delete Notification')], db_index=True, max_length=100)),
                ('history_id', models.AutoField(primary_key=True, serialize=False)),
                ('history_date', models.DateTimeField()),
                ('history_change_reason', models.CharField(max_length=100, null=True)),
                ('history_type', models.CharField(choices=[('+', 'Created'), ('~', 'Changed'), ('-', 'Deleted')], max_length=1)),
                ('created_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Created By')),
                ('history_user', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('last_modified_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By')),
                ('role', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to='custom_auth.Role')),
            ],
            options={
                'verbose_name': 'historical role permission',
                'ordering': ('-history_date', '-history_id'),
                'get_latest_by': 'history_date',
            },
            bases=(simple_history.models.HistoricalChanges, models.Model),
        ),
        migrations.CreateModel(
            name='HistoricalRole',
            fields=[
                ('id', models.IntegerField(auto_created=True, blank=True, db_index=True, verbose_name='ID')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Created Date')),
                ('name', models.CharField(db_index=True, max_length=255, verbose_name='Role')),
                ('description', models.CharField(blank=True, max_length=255, null=True)),
                ('category', models.CharField(choices=[('system', 'System Role'), ('custom', 'Custom Role')], default='system', max_length=50, verbose_name='Role')),
                ('history_id', models.AutoField(primary_key=True, serialize=False)),
                ('history_date', models.DateTimeField()),
                ('history_change_reason', models.CharField(max_length=100, null=True)),
                ('history_type', models.CharField(choices=[('+', 'Created'), ('~', 'Changed'), ('-', 'Deleted')], max_length=1)),
                ('created_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Created By')),
                ('history_user', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('last_modified_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By')),
            ],
            options={
                'verbose_name': 'historical role',
                'ordering': ('-history_date', '-history_id'),
                'get_latest_by': 'history_date',
            },
            bases=(simple_history.models.HistoricalChanges, models.Model),
        ),
        migrations.CreateModel(
            name='HistoricalApplicationUser',
            fields=[
                ('id', models.IntegerField(auto_created=True, blank=True, db_index=True, verbose_name='ID')),
                ('last_login', models.DateTimeField(blank=True, null=True, verbose_name='last login')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(blank=True, editable=False, verbose_name='Created Date')),
                ('email', models.EmailField(blank=True, db_index=True, max_length=254, null=True, verbose_name='email address')),
                ('first_name', models.CharField(max_length=100, verbose_name='First Name')),
                ('last_name', models.CharField(max_length=100, verbose_name='Last Name')),
                ('date_joined', models.DateTimeField(default=django.utils.timezone.now, verbose_name='Date Joined')),
                ('is_active', models.BooleanField(default=True, verbose_name='Active')),
                ('is_superuser', models.BooleanField(default=False)),
                ('username', models.CharField(db_index=True, error_messages={'unique': 'A user with that username already exists.'}, help_text='Required. 30 characters or fewer. Letters, digits and @/./+/-/_ only.', max_length=30, validators=[django.core.validators.RegexValidator('^[\\w.@+-]+$', 'Enter a valid username. This value may contain only letters, numbers and @/./+/-/_ characters.')], verbose_name='username')),
                ('is_staff', models.BooleanField(default=False, help_text='Designates whether the user can log into this admin site.', verbose_name='staff status')),
                ('history_id', models.AutoField(primary_key=True, serialize=False)),
                ('history_date', models.DateTimeField()),
                ('history_change_reason', models.CharField(max_length=100, null=True)),
                ('history_type', models.CharField(choices=[('+', 'Created'), ('~', 'Changed'), ('-', 'Deleted')], max_length=1)),
                ('created_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Created By')),
                ('history_user', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('last_modified_by', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to=settings.AUTH_USER_MODEL, verbose_name='Last Updated By')),
            ],
            options={
                'verbose_name': 'historical application user',
                'ordering': ('-history_date', '-history_id'),
                'get_latest_by': 'history_date',
            },
            bases=(simple_history.models.HistoricalChanges, models.Model),
        ),
    ]
