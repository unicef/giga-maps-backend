# Generated by Django 2.2.28 on 2024-12-26 12:20

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import proco.core.models


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('schools', '0032_removed_location_id_field_from_school_model'),
    ]

    operations = [
        migrations.CreateModel(
            name='SchoolMasterStatus',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
                ('last_modified_at',
                 proco.core.models.CustomDateTimeField(auto_now=True, verbose_name='Last Updated Date')),
                ('created', proco.core.models.CustomDateTimeField(auto_now_add=True, verbose_name='Created Date')),
                ('download_speed_benchmark', models.FloatField(blank=True, default=None, null=True)),
                ('download_speed_contracted', models.FloatField(blank=True, default=None, null=True)),
                ('num_computers_desired', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('electricity_type', models.CharField(blank=True, max_length=255, null=True)),
                ('num_adm_personnel', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_classrooms', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_latrines', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_computers', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students_girls', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students_boys', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students_other', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers_female', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers_male', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('fiber_node_distance', models.FloatField(blank=True, default=None, null=True)),
                ('microwave_node_distance', models.FloatField(blank=True, default=None, null=True)),
                ('schools_within_1km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('schools_within_2km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('schools_within_3km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('pop_within_1km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('pop_within_2km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('pop_within_3km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('connectivity_govt_collection_year',
                 models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('num_tablets', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_robotic_equipment', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('building_id_govt', models.CharField(blank=True, max_length=255, null=True)),
                ('num_schools_per_building', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('establishment_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('water_availability', models.NullBooleanField(default=None)),
                ('electricity_availability', models.NullBooleanField(default=None)),
                ('computer_lab', models.NullBooleanField(default=None)),
                ('teachers_trained', models.NullBooleanField(default=None)),
                ('sustainable_business_model', models.NullBooleanField(default=None)),
                ('computer_availability', models.NullBooleanField(default=None)),
                ('device_availability', models.NullBooleanField(default=None)),
                ('disputed_region', models.BooleanField(default=False)),
                ('connectivity_govt', models.NullBooleanField(default=None)),
                ('connectivity_type_govt', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_type', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_type_root', models.CharField(blank=True, max_length=255, null=True)),
                ('nearest_lte_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_umts_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_gsm_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_nr_distance', models.FloatField(blank=True, default=None, null=True)),
                ('data_source', models.CharField(blank=True, max_length=255, null=True)),
                ('data_collection_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('data_collection_modality', models.CharField(blank=True, max_length=255, null=True)),
                ('location_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('connectivity_govt_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('connectivity', models.NullBooleanField(default=None)),
                ('version', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('created_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                                 related_name='created_schoolmasterstatuss',
                                                 to=settings.AUTH_USER_MODEL, verbose_name='Created By')),
                ('last_modified_by',
                 models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                   related_name='updated_schoolmasterstatuss', to=settings.AUTH_USER_MODEL,
                                   verbose_name='Last Updated By')),
                ('school',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, related_name='master_status',
                                   to='schools.School', verbose_name='School Master Sync')),
            ],
            options={
                'ordering': ('id',),
            },
        ),
        migrations.AddConstraint(
            model_name='schoolmasterstatus',
            constraint=models.UniqueConstraint(fields=('school', 'version', 'deleted'),
                                               name='schools_master_status_unique_with_deleted'),
        ),
        migrations.AddConstraint(
            model_name='schoolmasterstatus',
            constraint=models.UniqueConstraint(condition=models.Q(deleted=None), fields=('school', 'version'),
                                               name='schools_master_status_unique_without_deleted'),
        ),
    ]