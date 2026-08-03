import hashlib
import json
import logging

from proco.accounts import models as accounts_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import SchoolRealTimeWeeklyMetric


logger = logging.getLogger('gigamaps.' + __name__)


SUPPORTED_AGG_FUNCTIONS = {
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_AVG,
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MIN,
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MAX,
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_SUM,
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MEDIAN_50,
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MEDIAN_90,
}


def get_live_data_sources_from_relationships(data_source_relationships):
    live_data_sources = [statistics_configs.UNKNOWN_SOURCE]

    for relationship in data_source_relationships:
        source_type = relationship.data_source.data_source_type
        if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
            live_data_sources.append(statistics_configs.QOS_SOURCE)
        elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
            live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

    return sorted(set(live_data_sources))


def get_metric_name(data_source_column):
    if isinstance(data_source_column, dict):
        return data_source_column.get('name')
    return None


def get_agg_function_name(data_source_column_function):
    if not isinstance(data_source_column_function, dict) or len(data_source_column_function) == 0:
        return SchoolRealTimeWeeklyMetric.AGG_FUNCTION_AVG
    return str(
        data_source_column_function.get('name') or SchoolRealTimeWeeklyMetric.AGG_FUNCTION_AVG
    ).lower()


def get_agg_function_sql(data_source_column_function):
    if not isinstance(data_source_column_function, dict) or len(data_source_column_function) == 0:
        return ''
    return str(data_source_column_function.get('sql', ''))


def get_live_data_sources_key(live_data_sources):
    return ','.join(sorted(set([str(source) for source in live_data_sources])))


def get_data_layer_country_ids(data_layer):
    country_ids = list(data_layer.applicable_countries or [])
    if len(country_ids) > 0:
        return country_ids

    return list(data_layer.active_countries.filter(
        deleted__isnull=True,
        is_applicable=True,
    ).values_list('country_id', flat=True))


def build_live_weekly_config_payload(metric_name, agg_function, live_data_sources, agg_function_sql=''):
    """
    Build the stable aggregate identity.

    Country/layer/date are intentionally excluded. They define where the aggregate
    should be populated, not how the value is calculated.
    """
    return {
        'metric_name': metric_name,
        'agg_function': agg_function,
        'agg_function_sql': agg_function_sql,
        'live_data_sources': sorted(set([str(source) for source in live_data_sources])),
    }


def build_live_weekly_config_hash(metric_name, agg_function, live_data_sources, agg_function_sql=''):
    payload = build_live_weekly_config_payload(
        metric_name,
        agg_function,
        live_data_sources,
        agg_function_sql=agg_function_sql,
    )
    payload_json = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload_json.encode('utf-8')).hexdigest()


def build_live_weekly_requirement(data_layer, data_source_relationships=None):
    if data_source_relationships is None:
        data_source_relationships = list(data_layer.data_sources.all().select_related('data_source'))
    else:
        data_source_relationships = list(data_source_relationships)

    if len(data_source_relationships) == 0:
        logger.warning('No data source relationships found for live data layer id=%s', data_layer.id)
        return None

    parameter_relationship = data_source_relationships[0]
    metric_name = get_metric_name(parameter_relationship.data_source_column)
    agg_function = get_agg_function_name(parameter_relationship.data_source_column_function)
    agg_function_sql = get_agg_function_sql(parameter_relationship.data_source_column_function)
    live_data_sources = get_live_data_sources_from_relationships(data_source_relationships)

    if not metric_name:
        logger.warning('No metric name found for live data layer id=%s', data_layer.id)
        return None

    if agg_function not in SUPPORTED_AGG_FUNCTIONS:
        logger.warning(
            'Unsupported aggregate function "%s" for live data layer id=%s. Skipping requirement.',
            agg_function,
            data_layer.id,
        )
        return None

    config_hash = build_live_weekly_config_hash(
        metric_name,
        agg_function,
        live_data_sources,
        agg_function_sql=agg_function_sql,
    )

    return {
        'data_layer_id': data_layer.id,
        'data_layer_code': data_layer.code,
        'country_ids': get_data_layer_country_ids(data_layer),
        'metric_name': metric_name,
        'agg_function': agg_function,
        'agg_function_sql': agg_function_sql,
        'live_data_sources': live_data_sources,
        'live_data_sources_key': get_live_data_sources_key(live_data_sources),
        'config_hash': config_hash,
    }


def get_active_published_live_layer_requirements(layer_ids=None, country_ids=None):
    queryset = accounts_models.DataLayer.objects.filter(
        type=accounts_models.DataLayer.LAYER_TYPE_LIVE,
        status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
    ).prefetch_related('data_sources__data_source')

    if layer_ids:
        queryset = queryset.filter(id__in=layer_ids)

    requirements = []
    country_ids_filter = set([str(country_id) for country_id in country_ids or []])

    for data_layer in queryset:
        requirement = build_live_weekly_requirement(data_layer, data_layer.data_sources.all())
        if requirement is None:
            continue

        requirement_country_ids = set([str(country_id) for country_id in requirement['country_ids']])
        if country_ids_filter:
            filtered_country_ids = sorted(requirement_country_ids & country_ids_filter)
            if len(filtered_country_ids) == 0:
                continue
            requirement['country_ids'] = filtered_country_ids

        requirements.append(requirement)

    return requirements


def get_active_published_live_aggregate_requirements(layer_ids=None, country_ids=None):
    layer_requirements = get_active_published_live_layer_requirements(
        layer_ids=layer_ids,
        country_ids=country_ids,
    )
    requirements_by_hash = {}

    for layer_requirement in layer_requirements:
        config_hash = layer_requirement['config_hash']

        if config_hash not in requirements_by_hash:
            requirements_by_hash[config_hash] = {
                'config_hash': config_hash,
                'metric_name': layer_requirement['metric_name'],
                'agg_function': layer_requirement['agg_function'],
                'agg_function_sql': layer_requirement['agg_function_sql'],
                'live_data_sources': layer_requirement['live_data_sources'],
                'live_data_sources_key': layer_requirement['live_data_sources_key'],
                'country_ids': set(),
                'data_layer_ids': [],
            }

        requirements_by_hash[config_hash]['country_ids'].update(
            [str(country_id) for country_id in layer_requirement['country_ids']]
        )
        requirements_by_hash[config_hash]['data_layer_ids'].append(layer_requirement['data_layer_id'])

    requirements = []
    for requirement in requirements_by_hash.values():
        requirement['country_ids'] = sorted(requirement['country_ids'])
        requirement['data_layer_ids'] = sorted(set(requirement['data_layer_ids']))
        requirements.append(requirement)

    return sorted(requirements, key=lambda item: item['config_hash'])
