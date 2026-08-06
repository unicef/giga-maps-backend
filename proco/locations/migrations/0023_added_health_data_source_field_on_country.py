from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('locations', '0022_added_disclaimer_field_on_country'),
    ]

    operations = [
        migrations.AddField(
            model_name='country',
            name='health_data_source',
            field=models.TextField(blank=True, default='', max_length=500),
        ),
    ]
