from proco.accounts import models as accounts_models


def static_coverage_layer_data():
    master_data_source = accounts_models.DataSource.objects.filter(
        data_source_type=accounts_models.DataSource.DATA_SOURCE_TYPE_SCHOOL_MASTER,
    ).first()
    return {
        "icon": "icon",
        "code": "CELLULAR COVERAGE",
        "name": "Cellular Coverage",
        "description": "Mobile coverage in the area",
        "version": "V 1.0",
        "type": "STATIC",
        "category": "COVERAGE",
        "applicable_countries": [],
        "legend_configs": {
            "good": {
                "values": [
                    "5G",
                    "4G"
                ],
                "labels": "3G & above"
            },
            "moderate": {
                "values": [
                    "3G",
                    "2G"
                ],
                "labels": "2G"
            },
            "bad": {
                "values": [
                    "no"
                ],
                "labels": "No Coverage"
            },
            "unknown": {
                "values": [],
                "labels": "Unknown"
            }
        },
        "data_sources_list": [master_data_source.id, ],
        "data_source_column": master_data_source.column_config[0],
        "data_source_column_function": {},
    }


def live_download_layer_data_pcdc():
    pcdc_data_source = accounts_models.DataSource.objects.filter(
        data_source_type=accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP,
    ).first()

    return {
        "icon": "icon",
        "code": "DOWNLOAD PCDC",
        "name": "Download - PCDC",
        "description": "pcdc download speed",
        "type": "LIVE",
        "category": "CONNECTIVITY",
        "applicable_countries": [],
        "global_benchmark": {
            "value": "20000000",
            "unit": "bps",
            "convert_unit": "mbps"
        },
        "legend_configs": {
            "good": {
                "values": [],
                "labels": "Good"
            },
            "moderate": {
                "values": [],
                "labels": "Moderate"
            },
            "bad": {
                "values": [],
                "labels": "Bad"
            },
            "unknown": {
                "values": [],
                "labels": "Unknown"
            }
        },
        "is_reverse": False,
        "data_sources_list": [pcdc_data_source.id,],
        "data_source_column": pcdc_data_source.column_config[0],
        "data_source_column_function": {},
        "benchmark_metadata": {
            "benchmark_value": "20000000",
            "benchmark_unit": "bps",
            "base_benchmark": "1000000",
            "parameter_column_unit": "bps",
            "round_unit_value": "{val} / (1000 * 1000)"
        },
    }
