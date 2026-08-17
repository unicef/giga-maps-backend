import logging

import requests
from django.conf import settings

logger = logging.getLogger('gigamaps.' + __name__)

def is_statistic_enabled(value):
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ('true', '1', 'yes')
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == 1
    return False


STATISTICS_KEYS = (
    'num_students',
    'num_teachers',
    'num_classroom',
    'num_latrines',
    'running_water',
    'electricity_availability',
    'computer_lab',
    'num_computers',
    'connectivity',
    'connectivity_status',
    'connectivity_type',
    'connectivity_speed',
    'connectivity_latency',
    'coverage_availability',
    'coverage_type',
    'connectivity_govt',
    'computer_availability',
    'num_students_girls',
    'num_students_boys',
    'num_students_other',
    'num_teachers_female',
    'num_teachers_male',
    'teachers_trained',
    'sustainable_business_model',
    'device_availability',
    'num_tablets',
    'num_robotic_equipment',
)


def get_nocodb_table_id():
    environment = (settings.APP_ENVIRONMENT or '').upper()
    if environment in ('PROD', 'PRODUCTION'):
        return settings.NOCODB_TABLE_ID_PRODUCTION or settings.NOCODB_TABLE_ID
    return settings.NOCODB_TABLE_ID


def fetch_country_configs():
    api_url = settings.NOCODB_API_URL
    api_token = settings.NOCODB_API_TOKEN
    table_id = get_nocodb_table_id()

    if not all([api_url, api_token, table_id]):
        logger.warning('NocoDB country config is not configured.')
        return []

    url = '{0}/tables/{1}/records'.format(api_url.rstrip('/'), table_id)
    headers = {'xc-token': api_token}
    records = []
    offset = 0
    limit = 100

    while True:
        response = requests.get(
            url,
            headers=headers,
            params={'limit': limit, 'offset': offset},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        records.extend(payload.get('list', []))

        page_info = payload.get('pageInfo', {})
        if page_info.get('isLastPage', True):
            break
        offset += limit

    config = []
    for record in records:
        country_code_raw = record.get('Country Code')
        try:
            country_code = int(country_code_raw)
        except (TypeError, ValueError):
            logger.warning('Skipping country config row with invalid Country Code: %s', country_code_raw)
            continue

        enabled_statistics = [
            key for key in STATISTICS_KEYS
            if is_statistic_enabled(record.get(key))
        ]
        config.append({
            'countryCode': country_code,
            'enabledStatistics': enabled_statistics,
        })

    return config
