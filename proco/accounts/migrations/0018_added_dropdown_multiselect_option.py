# Generated by Django 2.2.28 on 2024-08-02 10:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0017_updated_advance_filter_model_fields'),
    ]

    operations = [
        migrations.AlterField(
            model_name='advancefilter',
            name='status',
            field=models.CharField(choices=[('DRAFT', 'In Draft'), ('PUBLISHED', 'Activated'), ('DISABLED', 'De-activated')], db_index=True, default='DRAFT', max_length=20),
        ),
        migrations.AlterField(
            model_name='advancefilter',
            name='type',
            field=models.CharField(choices=[('DROPDOWN', 'Dropdown'), ('DROPDOWN_MULTISELECT', 'Dropdown with multi select'), ('RANGE', 'Range'), ('INPUT', 'Input'), ('BOOLEAN', 'Boolean')], db_index=True, max_length=10),
        ),
        migrations.AlterField(
            model_name='historicaladvancefilter',
            name='status',
            field=models.CharField(choices=[('DRAFT', 'In Draft'), ('PUBLISHED', 'Activated'), ('DISABLED', 'De-activated')], db_index=True, default='DRAFT', max_length=20),
        ),
        migrations.AlterField(
            model_name='historicaladvancefilter',
            name='type',
            field=models.CharField(choices=[('DROPDOWN', 'Dropdown'), ('DROPDOWN_MULTISELECT', 'Dropdown with multi select'), ('RANGE', 'Range'), ('INPUT', 'Input'), ('BOOLEAN', 'Boolean')], db_index=True, max_length=10),
        ),
    ]