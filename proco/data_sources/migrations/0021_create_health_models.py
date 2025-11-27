from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import model_utils.fields
import proco.core.models
import simple_history.models


class Migration(migrations.Migration):

    atomic = False

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('locations', '0022_added_disclaimer_field_on_country'),
        ('entities', '0002_auto_20251126_1255'),
        ('data_sources', '0020_added_kenya_qos_fields'),
    ]

    operations = [
        migrations.CreateModel(
            name='HealthEntityMasterData',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('pulled_at', proco.core.models.CustomDateTimeField(blank=True, null=True, verbose_name='Pulled at Date')),
                ('latitude', models.FloatField(blank=True, null=True, default=None)),
                ('longitude', models.FloatField(blank=True, null=True, default=None)),
                ('facility_name', models.CharField(default='Name unknown', max_length=1000)),
                ('health_id_giga', models.CharField(db_index=True, max_length=50)),
                ('facility_level', models.CharField(max_length=20)),
                ('licensing_status', models.CharField(max_length=20)),
                ('facility_hours', models.CharField(max_length=30)),
                ('hmis_system', models.CharField(max_length=10)),
                ('facility_id_govt', models.CharField(max_length=50)),

                ('created', model_utils.fields.AutoCreatedField(default=django.utils.timezone.now, editable=False)),
                ('modified', model_utils.fields.AutoLastModifiedField(default=django.utils.timezone.now, editable=False)),

                ('country', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, to='locations.Country', related_name='health_entity_master_rows')),
                ('entity', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, to='entities.Entity', related_name='health_entity', verbose_name='Health Entity')),
                ('modified_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, to=settings.AUTH_USER_MODEL, related_name='updated_healthentitymasterdatas')),
                ('published_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING, to=settings.AUTH_USER_MODEL, related_name='source_published_healthentitymasterdatas')),
            ]
        ),

        migrations.CreateModel(
            name='HistoricalHealthEntityMasterData',
            fields=[
                ('id', models.IntegerField(auto_created=True, blank=True, db_index=True, verbose_name='ID')),
                ('pulled_at', proco.core.models.CustomDateTimeField(blank=True, null=True, verbose_name='Pulled at Date')),
                ('facility_name', models.CharField(default='Name unknown', max_length=1000)),
                ('health_id_giga', models.CharField(db_index=True, max_length=50)),
                ('history_id', models.AutoField(primary_key=True, serialize=False)),
                ('history_date', models.DateTimeField()),
                ('history_change_reason', models.CharField(max_length=100, null=True)),
                ('history_type', models.CharField(choices=[('+', 'Created'), ('~', 'Changed'), ('-', 'Deleted')], max_length=1)),
                ('history_user', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'ordering': ('-history_date', '-history_id'),
                'get_latest_by': 'history_date',
            },
            bases=(simple_history.models.HistoricalChanges, models.Model)
        )
    ]
