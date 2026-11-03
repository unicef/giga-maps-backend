# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from django.db import migrations

LIVE_DATA_SOURCE_TYPES = ('DAILY_CHECK_APP', 'QOS')


def build_daily_speed_column(name, alias, value_label):
    return {
        'name': name,
        'type': 'int',
        'unit': 'bps',
        'is_parameter': True,
        'alias': alias,
        'base_benchmark': 1000000,
        'display_unit': 'Mbps',
        'supported_functions': [
            {
                'name': 'avg',
                'verbose': 'Avg',
                'description': 'Average of the daily {0} values'.format(value_label),
                'sql': 'AVG({col_name})',
            },
            {
                'name': 'median|50',
                'verbose': '50th Percentile',
                'description': 'Median of the daily {0} values'.format(value_label),
                'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})',
            },
        ],
    }


# Frozen copies of the column entries in load_system_data_layers, so this migration does not change if the
# command does. Inserted in this order right after connectivity_speed.
DAILY_SPEED_COLUMNS = [
    build_daily_speed_column('connectivity_speed_p90', 'Download Speed P90', 'P90'),
    build_daily_speed_column('connectivity_speed_median', 'Download Speed Median', 'median'),
]


def add_daily_speed_columns(apps, schema_editor):
    """
    add_daily_speed_columns
        Add the daily P90 and median download speed columns to the live data sources, right after
        connectivity_speed. Columns a data source already has are left as they are.
    """
    data_source_model = apps.get_model('accounts', 'DataSource')
    data_sources = data_source_model.objects.filter(
        data_source_type__in=LIVE_DATA_SOURCE_TYPES,
        deleted__isnull=True,
    )

    for data_source in data_sources:
        columns = list(data_source.column_config or [])
        changed = False

        for new_column in DAILY_SPEED_COLUMNS:
            column_names = [column.get('name') for column in columns]
            if new_column['name'] in column_names:
                continue

            # after connectivity_speed and any daily speed column already placed there
            position = len(columns)
            if 'connectivity_speed' in column_names:
                position = column_names.index('connectivity_speed') + 1
                while position < len(columns) and columns[position].get('name') in [
                    column['name'] for column in DAILY_SPEED_COLUMNS
                ]:
                    position += 1
            columns.insert(position, new_column)
            changed = True

        if changed:
            data_source_model.objects.filter(id=data_source.id).update(column_config=columns)


def remove_daily_speed_columns(apps, schema_editor):
    """
    remove_daily_speed_columns
        Reverse of add_daily_speed_columns: drop the daily P90 and median download speed columns from the
        live data sources.
    """
    data_source_model = apps.get_model('accounts', 'DataSource')
    data_sources = data_source_model.objects.filter(data_source_type__in=LIVE_DATA_SOURCE_TYPES)
    new_column_names = [column['name'] for column in DAILY_SPEED_COLUMNS]

    for data_source in data_sources:
        columns = list(data_source.column_config or [])
        kept_columns = [column for column in columns if column.get('name') not in new_column_names]
        if len(kept_columns) != len(columns):
            data_source_model.objects.filter(id=data_source.id).update(column_config=kept_columns)


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0023_auto_20260817_1249'),
        ('proco_data_migrations', '0005_lower_case_indexes_for_filters'),
    ]

    operations = [
        migrations.RunPython(add_daily_speed_columns, remove_daily_speed_columns),
    ]
