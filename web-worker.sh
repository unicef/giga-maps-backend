#!/usr/bin/env bash
set -ex

# export environment variables to make them available in ssh session
for var in $(compgen -e); do
    echo "export $var=${!var}" >> /etc/profile
done

eval $(printenv | awk -F= '{print "export " "\""$1"\"""=""\""$2"\"" }' >> /etc/profile)

echo "Starting SSH ..."
service ssh start

pipenv run python manage.py migrate
pipenv run python manage.py collectstatic --noinput
pipenv run gunicorn config.wsgi:application -b 0.0.0.0:8000 -w 8 --timeout=300


# pipenv run python manage.py load_api_data --api-file /code/proco/core/resources/all_apis.tsv
# pipenv run python manage.py load_api_data --api-file /code/proco/core/resources/all_apis-dev.tsv
# pipenv run python manage.py index_rebuild_schools --delete_index --create_index --clean_index --update_index
# pipenv run python manage.py populate_school_new_fields -start_school_id=1 -end_school_id=1000000
# pipenv run python manage.py populate_school_new_fields -country_id=5
# pipenv run python manage.py populate_school_registration_data
# pipenv run python manage.py load_iso3_format_code_for_countries --country-file /code/proco/core/resources/locations_country_processed.csv
# pipenv run python manage.py update_system_role_permissions
# pipenv run python manage.py load_system_data_layers --delete_data_sources --update_data_sources --update_data_layers


# pipenv run python manage.py create_admin_user -email='sharora@unicef.org' -first_name='Shilpa' -last_name='Arora'
# pipenv run python manage.py create_admin_user -email='nulu.tejaswini@nagarro.com' -first_name='Nulu' -last_name='Tejaswini'
# pipenv run python manage.py create_admin_user -email='vikash.kumar05@nagarro.com' -first_name='Vikash' -last_name='Admin'
# pipenv run python manage.py create_admin_user -email='nitesh02@nagarro.com' -first_name='Nitesh' -last_name='Admin'
# pipenv run python manage.py create_admin_user -email='rushikesh.pawar@nagarro.com' -first_name='Rushikesh' -last_name='Admin'
# pipenv run python manage.py create_admin_user -email='rahul.avhad@nagarro.com' -first_name='Rahul' -last_name='Avhad'


# pipenv run python manage.py load_country_admin_data -af ./proco/core/resources/mapbox_boundaries_metadata_admin_0.csv -at admin0
# pipenv run python manage.py load_country_admin_data -af ./proco/core/resources/mapbox_boundaries_metadata_admin_1.csv -at admin1
# pipenv run python manage.py load_country_admin_data -af ./proco/core/resources/mapbox_boundaries_metadata_admin_2.csv -at admin2

# pipenv run python manage.py populate_admin_id_fields_to_schools -start_school_id=1 -end_school_id=1000000 -at=admin1
# pipenv run python manage.py populate_admin_id_fields_to_schools -start_school_id=1 -end_school_id=1000000 -at=admin2


# pipenv run python manage.py populate_admin_ui_labels -at=admin1
# pipenv run python manage.py populate_admin_ui_labels -at=admin2

# pipenv run python manage.py load_about_us_content --load_about_us_content

# pipenv run python manage.py data_alteration_through_sql --update_brasil_live_data_source_name -end_school_id=1000000
# pipenv run python manage.py data_alteration_through_sql --update_non_brasil_live_data_source_name


# pipenv run python manage.py populate_active_data_layer_for_countries --reset
# pipenv run python manage.py load_api_data --api-file /code/proco/core/resources/all_apis-dev.tsv
# pipenv run python manage.py load_system_data_layers --update_data_sources

# pipenv run python manage.py create_api_key_with_write_access -user='pcdc_user_with_write_api_key@nagarro.com' -api_code='DAILY_CHECK_APP' -reason='For Post/Delete API Control over DailyCheckApp documentation' -valid_till='31-12-2099' --force_user -first_name='PCDC' -last_name='User'
# pipenv run python manage.py create_api_key_with_write_access -user='aditya.acharya@nagarro.com' -api_code='DAILY_CHECK_APP' -reason='For Post/Delete API Control over DailyCheckApp documentation' -valid_till='31-12-2099' --force_user -first_name='Aditya' -last_name='Acharya'

# pipenv run python manage.py data_loss_recovery_for_pcdc --check_missing_dates -start_date='01-04-2024' -end_date='30-04-2024'
# pipenv run python manage.py data_loss_recovery_for_pcdc -pull_data_date='22-04-2024' --pull_data


# To sync the QoS missed data for last 30 days at max:
#   Step 1: Check missing version list:
#       For all QoS countries: pipenv run python manage.py data_loss_recovery_for_qos --check_missing_versions
#       For 1 country: pipenv run python manage.py data_loss_recovery_for_qos --check_missing_versions -country_code='MNG'
#   Step 2: Sync the QoS API data to QoSData table based on version. Only works for single country
#       pipenv run python manage.py data_loss_recovery_for_qos --pull_data -country_code='MNG' -pull_start_version=11 -pull_end_version=20
#   Step 3: Update the proco tables with new aggregation
#       pipenv run python manage.py data_loss_recovery_for_qos --aggregate -country_code='MNG' -aggregate_start_version=11 -aggregate_end_version=20


# pipenv run python manage.py create_api_key_with_write_access -user='pcdc_user_with_write_api_key66@nagarro.com' -api_code='DAILY_CHECK_APP' -reason='API Key to GET the PCDC measurement data, Post/Delete API Control over DailyCheckApp documentation' -valid_till='31-12-2099' --force_user -first_name='PCDC' -last_name='User' --inactive_email
# pipenv run python manage.py load_system_data_layers --update_data_layers_code
# pipenv run python manage.py update_system_role_permissions
# pipenv run python manage.py data_cleanup --clean_duplicate_school_gigs_ids

# pipenv run python manage.py create_admin_user -email='pcdc_user_with_write_api_key5@nagarro.com' -first_name='PCDC' -last_name='User' --inactive_email

# pipenv run python manage.py update_system_role_permissions
# pipenv run python manage.py load_column_configurations --update_configurations
# pipenv run python manage.py populate_active_filters_for_countries --reset

# pipenv run python manage.py data_cleanup --cleanup_active_download_layer -country_id=144

# pipenv run python manage.py load_column_configurations --update_configurations
# pipenv run python manage.py load_system_data_layers --update_data_sources

# pipenv run python manage.py data_cleanup --populate_school_lowercase_fields -country_id=222 -start_school_id=3030276 -end_school_id=3030287
