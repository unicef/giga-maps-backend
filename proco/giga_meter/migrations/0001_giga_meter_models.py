# Generated by Django 2.2.28 on 2024-12-20 11:06

from django.db import migrations, models
import django.utils.timezone
import model_utils.fields
import proco.core.models
import timezone_field.fields


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='GigaMeter_Country',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255)),
                ('code', models.CharField(max_length=32)),
                ('iso3_format', models.CharField(blank=True, max_length=32, null=True)),
                ('latest_school_master_data_version', models.PositiveIntegerField(blank=True, default=None, null=True)),
            ],
            options={
                'db_table': 'country',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='GigaMeter_School',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False, verbose_name='created')),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False, verbose_name='modified')),
                ('name', models.CharField(default='Name unknown', max_length=1000)),
                ('country_code', models.CharField(max_length=32)),
                ('timezone', timezone_field.fields.TimeZoneField(blank=True, null=True)),
                ('geopoint', models.CharField(blank=True, max_length=1000, null=True, verbose_name='Point')),
                ('gps_confidence', models.FloatField(blank=True, null=True)),
                ('altitude', models.PositiveIntegerField(blank=True, default=0)),
                ('address', models.CharField(blank=True, max_length=255)),
                ('postal_code', models.CharField(blank=True, max_length=128)),
                ('email', models.EmailField(blank=True, default=None, max_length=128, null=True)),
                ('education_level', models.CharField(blank=True, max_length=255)),
                ('environment', models.CharField(blank=True, max_length=64)),
                ('school_type', models.CharField(blank=True, db_index=True, max_length=64)),
                ('admin_1_name', models.CharField(blank=True, max_length=100)),
                ('admin_2_name', models.CharField(blank=True, max_length=100)),
                ('admin_3_name', models.CharField(blank=True, max_length=100)),
                ('admin_4_name', models.CharField(blank=True, max_length=100)),
                ('external_id', models.CharField(blank=True, db_index=True, max_length=50)),
                ('name_lower', models.CharField(blank=True, db_index=True, editable=False, max_length=1000)),
                ('giga_id_school', models.CharField(blank=True, db_index=True, max_length=50)),
                ('education_level_regional', models.CharField(blank=True, max_length=6400007)),
                ('deleted', proco.core.models.CustomDateTimeField(blank=True, db_index=True, null=True)),
            ],
            options={
                'db_table': 'school',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='GigaMeter_SchoolMasterData',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False, verbose_name='created')),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False, verbose_name='modified')),
                ('school_id_giga', models.CharField(db_index=True, max_length=50)),
                ('school_id_govt', models.CharField(blank=True, db_index=True, max_length=255, null=True)),
                ('school_name', models.CharField(default='Name unknown', max_length=1000)),
                ('admin1', models.CharField(blank=True, max_length=255, null=True)),
                ('admin1_id_giga', models.CharField(blank=True, max_length=50, null=True)),
                ('admin2', models.CharField(blank=True, max_length=255, null=True)),
                ('admin2_id_giga', models.CharField(blank=True, max_length=50, null=True)),
                ('latitude', models.FloatField(blank=True, default=None, null=True)),
                ('longitude', models.FloatField(blank=True, default=None, null=True)),
                ('education_level', models.CharField(blank=True, max_length=255, null=True)),
                ('school_area_type', models.CharField(blank=True, max_length=255, null=True)),
                ('school_funding_type', models.CharField(blank=True, max_length=255, null=True)),
                ('school_establishment_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('download_speed_contracted', models.FloatField(blank=True, default=None, null=True)),
                ('num_computers_desired', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('electricity_type', models.CharField(blank=True, max_length=255, null=True)),
                ('num_adm_personnel', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_classrooms', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_latrines', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('water_availability', models.CharField(blank=True, max_length=255, null=True)),
                ('electricity_availability', models.CharField(blank=True, max_length=255, null=True)),
                ('computer_lab', models.CharField(blank=True, max_length=255, null=True)),
                ('num_computers', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('connectivity_govt', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_type_govt', models.CharField(blank=True, max_length=255, null=True)),
                ('cellular_coverage_availability', models.CharField(blank=True, max_length=255, null=True)),
                ('cellular_coverage_type', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_type', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_type_root', models.CharField(blank=True, max_length=255, null=True)),
                ('fiber_node_distance', models.FloatField(blank=True, default=None, null=True)),
                ('microwave_node_distance', models.FloatField(blank=True, default=None, null=True)),
                ('schools_within_1km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('schools_within_2km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('schools_within_3km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('nearest_LTE_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_UMTS_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_GSM_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_NR_distance', models.FloatField(blank=True, default=None, null=True)),
                ('pop_within_1km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('pop_within_2km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('pop_within_3km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('school_data_source', models.CharField(blank=True, max_length=255, null=True)),
                ('school_data_collection_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('school_data_collection_modality', models.CharField(blank=True, max_length=255, null=True)),
                ('school_location_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('connectivity_govt_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('connectivity_govt_collection_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('disputed_region', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_RT', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_RT_datasource', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_RT_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('download_speed_benchmark', models.FloatField(blank=True, default=None, null=True)),
                ('computer_availability', models.CharField(blank=True, max_length=255, null=True)),
                ('num_students_girls', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students_boys', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students_other', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers_female', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers_male', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('teachers_trained', models.CharField(blank=True, max_length=255, null=True)),
                ('sustainable_business_model', models.CharField(blank=True, max_length=255, null=True)),
                ('device_availability', models.CharField(blank=True, max_length=255, null=True)),
                ('num_tablets', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_robotic_equipment', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('building_id_govt', models.CharField(blank=True, max_length=255, null=True)),
                ('num_schools_per_building', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('connectivity', models.CharField(blank=True, max_length=255, null=True)),
                ('version', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('status', models.CharField(choices=[('PUBLISHED', 'Insert/Update'), ('DELETED', 'Deleted')], db_index=True, default='PUBLISHED', max_length=50)),
            ],
            options={
                'db_table': 'master_sync_intermediate',
                'ordering': ['created'],
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='GigaMeter_SchoolStatic',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False, verbose_name='created')),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False, verbose_name='modified')),
                ('latitude', models.FloatField(blank=True, default=None, null=True)),
                ('longitude', models.FloatField(blank=True, default=None, null=True)),
                ('admin1_id_giga', models.CharField(blank=True, max_length=50, null=True)),
                ('admin2_id_giga', models.CharField(blank=True, max_length=50, null=True)),
                ('school_establishment_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('download_speed_contracted', models.FloatField(blank=True, default=None, null=True)),
                ('num_computers_desired', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('electricity_type', models.CharField(blank=True, max_length=255, null=True)),
                ('num_adm_personnel', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_classrooms', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_latrines', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('water_availability', models.NullBooleanField(default=None)),
                ('electricity_availability', models.NullBooleanField(default=None)),
                ('computer_lab', models.NullBooleanField(default=None)),
                ('num_computers', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('connectivity_govt', models.NullBooleanField(default=None)),
                ('connectivity_type_govt', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_type', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_type_root', models.CharField(blank=True, max_length=255, null=True)),
                ('cellular_coverage_availability', models.NullBooleanField(default=None)),
                ('cellular_coverage_type', models.CharField(blank=True, max_length=255, null=True)),
                ('fiber_node_distance', models.FloatField(blank=True, default=None, null=True)),
                ('microwave_node_distance', models.FloatField(blank=True, default=None, null=True)),
                ('schools_within_1km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('schools_within_2km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('schools_within_3km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('nearest_lte_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_umts_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_gsm_distance', models.FloatField(blank=True, default=None, null=True)),
                ('nearest_nr_distance', models.FloatField(blank=True, default=None, null=True)),
                ('pop_within_1km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('pop_within_2km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('pop_within_3km', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('school_data_source', models.CharField(blank=True, max_length=255, null=True)),
                ('school_data_collection_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('school_data_collection_modality', models.CharField(blank=True, max_length=255, null=True)),
                ('school_location_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('connectivity_govt_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('connectivity_govt_collection_year', models.PositiveSmallIntegerField(blank=True, default=None, null=True)),
                ('disputed_region', models.BooleanField(default=False)),
                ('connectivity_rt', models.NullBooleanField(default=None)),
                ('connectivity_rt_datasource', models.CharField(blank=True, max_length=255, null=True)),
                ('connectivity_rt_ingestion_timestamp', proco.core.models.CustomDateTimeField(blank=True, null=True)),
                ('connectivity', models.NullBooleanField(default=None)),
                ('download_speed_benchmark', models.FloatField(blank=True, default=None, null=True)),
                ('computer_availability', models.NullBooleanField(default=None)),
                ('num_students_girls', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students_boys', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_students_other', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers_female', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_teachers_male', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('teachers_trained', models.NullBooleanField(default=None)),
                ('sustainable_business_model', models.NullBooleanField(default=None)),
                ('device_availability', models.NullBooleanField(default=None)),
                ('num_tablets', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('num_robotic_equipment', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('building_id_govt', models.CharField(blank=True, max_length=255, null=True)),
                ('num_schools_per_building', models.PositiveIntegerField(blank=True, default=None, null=True)),
                ('version', models.PositiveIntegerField(blank=True, default=None, null=True)),
            ],
            options={
                'db_table': 'master_sync_school_static',
                'managed': False,
            },
        ),
    ]