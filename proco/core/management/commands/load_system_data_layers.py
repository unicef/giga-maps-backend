import sys

from django.core.management.base import BaseCommand
from django.db import transaction

from proco.accounts import models as accounts_models
from proco.core.utils import get_current_datetime_object, normalize_str


data_source_json = [
    {
        'name': 'Daily Check APP and MLab',
        'description': 'Daily Check APP and MLab',
        'version': 'V1.0',
        'data_source_type': 'DAILY_CHECK_APP',
        'request_config': {},
        'column_config': [
            {
                'name': 'connectivity_speed',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Download Speed',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            }, {
                'name': 'connectivity_upload_speed',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Upload Speed',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            }, {
                'name': 'connectivity_latency',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Latency',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            }
        ],
        'status': 'PUBLISHED'
    },
    {
        'name': 'QoS',
        'description': 'QoS',
        'version': 'V1.0',
        'data_source_type': 'QOS',
        'request_config': {},
        'column_config': [
            {
                'name': 'connectivity_speed',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Download Speed',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_upload_speed',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Upload Speed',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_latency',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Latency',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_speed_probe',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Download Speed Probe',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_upload_speed_probe',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Upload Speed Probe',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_latency_probe',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Latency Probe',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_speed_mean',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Download Speed Mean',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_upload_speed_mean',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Upload Speed Mean',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'roundtrip_time',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Roundtrip Time',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'jitter_download',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Jitter Download',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'jitter_upload',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Jitter Upload',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'rtt_packet_loss_pct',
                'type': 'int',
                'unit': '',
                'is_parameter': True,
                'alias': 'RTT Packet Loss',
                'base_benchmark': 1,
                'display_unit': '%',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    }
                ]
            },
            {
                'name': 'speed_download_max',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Speed Download Max',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'speed_upload_max',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Speed Upload Max',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'pe_ingress',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'PE Ingress',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'pe_egress',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'PE Egress',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'inbound_traffic_sum',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Inbound Traffic Sum',
                'base_benchmark': 1000000,
                'display_unit': 'MB',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'outbound_traffic_sum',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Outbound Traffic Sum',
                'base_benchmark': 1000000,
                'display_unit': 'MB',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'latency_min',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Latency Min',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'latency_mean',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Latency Mean',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'latency_max',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Latency Max',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            ######################################
            {
                'name': 'signal_mean',
                'type': 'int',
                'unit': 'dbm',
                'is_parameter': True,
                'alias': 'Signal Mean',
                'base_benchmark': 1000000,
                'display_unit': 'dBM',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'signal_max',
                'type': 'int',
                'unit': 'dbm',
                'is_parameter': True,
                'alias': 'Signal Max',
                'base_benchmark': 1000000,
                'display_unit': 'dBM',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'is_connected_all',
                'type': 'int',
                'unit': None,
                'is_parameter': True,
                'alias': 'Is Connected All',
                'base_benchmark': 1000000,
                'display_unit': None,
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'is_connected_true',
                'type': 'int',
                'unit': None,
                'is_parameter': True,
                'alias': 'Is Connected True',
                'base_benchmark': 1000000,
                'display_unit': None,
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': '',
                        'sql': 'AVG({col_name})',
                        'eval': ''
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'sum',
                        'verbose': 'Sum',
                        'description': 'Addition of all values',
                        'sql': 'SUM({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': '',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },

        ],
        'status': 'PUBLISHED'
    },
    {
        'name': 'School Master',
        'description': 'School Master',
        'version': 'V1.0',
        'data_source_type': 'SCHOOL_MASTER',
        'request_config': {},
        'column_config': [
            {
                'name': 'coverage_type',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Coverage Type (coverage_type)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate'],
            },
            {
                'name': 'connectivity_type',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Connectivity Type (connectivity_type)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate'],
            },
            {
                'name': 'fiber_node_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Fiber Node Distance (fiber_node_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'microwave_node_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Microwave Node Distance (microwave_node_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'nearest_nr_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Nearest NR Distance (nearest_nr_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'nearest_lte_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Nearest LTE Distance (nearest_lte_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'nearest_umts_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Nearest UMTS Distance (nearest_umts_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'nearest_gsm_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Nearest GSM Distance (nearest_gsm_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'computer_availability',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Computer Availability (computer_availability)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate'],
            },
            {
                'name': 'num_computers',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Computers (num_computers)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_students_girls',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Girl Students (num_students_girls)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_students_boys',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Boy Students (num_students_boys)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_students_other',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Other Students (num_students_other)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_teachers_female',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Female Teachers (num_teachers_female)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_teachers_male',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Male Teachers (num_teachers_male)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'teachers_trained',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Trained Teachers (teachers_trained)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate'],
            },
            {
                'name': 'sustainable_business_model',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Sustainable Business Model (sustainable_business_model)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate'],
            },
            {
                'name': 'device_availability',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Device Availability (device_availability)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate'],
            },
            {
                'name': 'num_tablets',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Tablets (num_tablets)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_robotic_equipment',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Robotic Equipment (num_robotic_equipment)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'building_id_govt',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Building Govt ID (building_id_govt)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_schools_per_building',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Schools per Building (num_schools_per_building)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'connectivity_status',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Connectivity Status (connectivity_status)',
                'unit': '',
                'display_unit': '',
                'table_name': 'schools_school',
                'count_labels': ['good', 'moderate', 'bad', 'unknown'],
            },
            {
                'name': 'education_level_govt',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Education Level Govt (education_level_govt)',
                'unit': '',
                'display_unit': '',
                'table_name': 'schools_school',
                'count_labels': ['good', 'moderate', 'bad', 'unknown'],
            },
            {
                'name': 'education_level_govt_lower',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Education Level Govt (education_level_govt_lower)',
                'unit': '',
                'display_unit': '',
                'table_name': 'schools_school',
                'count_labels': ['good', 'moderate', 'bad', 'unknown'],
            },
        ],
        'status': 'PUBLISHED'
    }
]
download_and_coverage_data_layer_json = [
    {
        'code': 'DEFAULT_DOWNLOAD',
        'name': 'Download',
        'icon': """<svg id="icon" xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32"><defs><style>.cls-1 {fill: none;}</style></defs><path d="M26,24v4H6V24H4v4H4a2,2,0,0,0,2,2H26a2,2,0,0,0,2-2h0V24Z"/><polygon points="26 14 24.59 12.59 17 20.17 17 2 15 2 15 20.17 7.41 12.59 6 14 16 24 26 14"/><g id="_Transparent_Rectangle_" data-name="&lt;Transparent Rectangle&gt;"><rect class="cls-1" width="32" height="32"/></g></svg>""",
        'description': 'System Download Layer',
        'version': 'V 1.0',
        'type': 'LIVE',
        'category': 'CONNECTIVITY',
        'applicable_countries': [],
        'global_benchmark': {'value': '20000000', 'unit': 'bps', 'convert_unit': 'mbps'},
        'legend_configs': [],
        'is_reverse': False,
        'status': 'PUBLISHED',
        'data_sources': [
            {
                'name': 'Daily Check APP and MLab',
                'data_source_type': 'DAILY_CHECK_APP',
                'data_source_column': {
                    'name': 'connectivity_speed',
                    'type': 'int',
                    'unit': 'bps',
                    'is_parameter': True,
                    'alias': 'Download Speed',
                    'base_benchmark': 1000000,
                    'display_unit': 'Mbps',
                }
            }, {
                'name': 'QoS',
                'data_source_type': 'QOS',
                'data_source_column': {
                    'name': 'connectivity_speed',
                    'type': 'int',
                    'unit': 'bps',
                    'is_parameter': True,
                    'alias': 'Download Speed',
                    'base_benchmark': 1000000,
                    'display_unit': 'Mbps',
                }
            }
        ]
    },
    {
        'code': 'DEFAULT_COVERAGE',
        'name': 'Coverage data',
        'icon': """<svg id="icon" xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32"><defs><style>.cls-1 {fill: none;}</style></defs><path d="M10.57,30l.9333-2h8.9928l.9333,2h2.2072L17,15.7778V11H15v4.7778L8.3631,30ZM16,18.3647,17.6965,22h-3.393ZM13.37,24h5.26l.9333,2H12.4369Z" transform="translate(0 0)"/><path d="M10.7832,9.3325a7.0007,7.0007,0,0,1,10.4341,0l-1.49,1.334a5,5,0,0,0-7.4537,0Z" transform="translate(0 0)"/><path d="M7.1992,6.3994a11.0019,11.0019,0,0,1,17.6006,0L23.2,7.6a9.0009,9.0009,0,0,0-14.4014,0Z" transform="translate(0 0)"/><rect id="_Transparent_Rectangle_" data-name="&lt;Transparent Rectangle&gt;" class="cls-1" width="32" height="32"/></svg>""",
        'description': 'Mobile coverage in the area',
        'version': 'V 1.0',
        'type': 'STATIC',
        'category': 'COVERAGE',
        'applicable_countries': [],
        'global_benchmark': {},
        'legend_configs': {
            'good': {
                'values': ['5G', '4G'],
                'labels': '5G/4G'
            },
            'moderate': {
                'values': ['3G', '2G'],
                'labels': '3G/2G'
            },
            'bad': {
                'values': ['no'],
                'labels': 'No Coverage'
            },
            'unknown': {
                'values': [],
                'labels': 'Unknown'
            }
        },
        'is_reverse': False,
        'status': 'PUBLISHED',
        'data_sources': [
            {
                'name': 'School Master',
                'data_source_type': 'SCHOOL_MASTER',
                'data_source_column': {
                    'name': 'coverage_type',
                    'type': 'str',
                    'is_parameter': True,
                    'alias': 'Coverage Type',
                    'unit': '',
                    'display_unit': '',
                    'count_labels': ['good', 'moderate']
                }
            }
        ]
    }
]


def load_data_sources_data():
    for row_data in data_source_json:
        try:
            instance, created = accounts_models.DataSource.objects.update_or_create(
                name=row_data['name'],
                data_source_type=row_data['data_source_type'],
                defaults={
                    'description': row_data['description'],
                    'request_config': row_data['request_config'],
                    'column_config': row_data['column_config'],
                    'version': row_data['version'],
                    'status': row_data['status'],
                    'last_modified_at': get_current_datetime_object(),
                },
            )
            if created:
                sys.stdout.write('\nNew Data Source created: {}'.format(instance.__dict__))
            else:
                sys.stdout.write('\nExisting Data Source updated: {}'.format(instance.__dict__))
        except:
            pass


def load_system_data_layers_data():
    for row_data in download_and_coverage_data_layer_json:
        try:
            layer_instance, created = accounts_models.DataLayer.objects.update_or_create(
                name=row_data['name'],
                type=row_data['type'],
                defaults={
                    'icon': row_data['icon'],
                    'description': row_data['description'],
                    'version': row_data['version'],
                    'category': row_data['category'],
                    'applicable_countries': row_data['applicable_countries'],
                    'global_benchmark': row_data['global_benchmark'],
                    'legend_configs': row_data['legend_configs'],
                    'is_reverse': row_data['is_reverse'],
                    'status': row_data['status'],
                    'last_modified_at': get_current_datetime_object(),
                },
            )
            if created:
                sys.stdout.write('\nNew Data Layers created: {}'.format(layer_instance.__dict__))
            else:
                sys.stdout.write('\nExisting Data Layers updated: {}'.format(layer_instance.__dict__))

            layer_id = layer_instance.id
            for data_source in row_data['data_sources']:
                source_id = accounts_models.DataSource.objects.filter(
                    name=data_source['name'],
                    data_source_type=data_source['data_source_type'],
                    deleted__isnull=True,
                ).first()

                relationship_instance, created = accounts_models.DataLayerDataSourceRelationship.objects.update_or_create(
                    data_layer_id=layer_id,
                    data_source=source_id,
                    defaults={
                        'data_source_column': data_source['data_source_column'],
                        'last_modified_at': get_current_datetime_object(),
                    },
                )
                if created:
                    sys.stdout.write(
                        '\nNew Data Source/Data Layer relationship created: {}'.format(relationship_instance.__dict__))
                else:
                    sys.stdout.write(
                        '\nExisting Source/Data Layer relationship updated: {}'.format(relationship_instance.__dict__))
        except:
            pass


def populate_data_layer_codes():
    for data_layer_instance in accounts_models.DataLayer.objects.all_records():
        if data_layer_instance.code == 'UNKNOWN':
            possible_code = normalize_str(str(data_layer_instance.name)).upper()
            count = 1
            while accounts_models.DataLayer.objects.all_records().filter(code=possible_code).exists():
                possible_code = possible_code + '_' + str(count)
            data_layer_instance.code = possible_code
            data_layer_instance.save(update_fields=('code',))


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--delete_data_sources', action='store_true', dest='delete_data_sources', default=False,
            help='If provided, already created data sources will be deleted from configured endpoint.'
        )

        parser.add_argument(
            '--update_data_sources', action='store_true', dest='update_data_sources', default=False,
            help='If provided, already created data sources data will be updated again.'
        )

        parser.add_argument(
            '--delete_data_layers', action='store_true', dest='delete_data_layers', default=False,
            help='If provided, already created system data layers will be deleted from configured endpoint.'
        )

        parser.add_argument(
            '--update_data_layers', action='store_true', dest='update_data_layers', default=False,
            help='If provided, already created Download/Coverage data layers will be updated again.'
        )

        parser.add_argument(
            '--update_data_layers_code', action='store_true', dest='update_data_layers_code', default=False,
            help='If provided, already created data layers will be updated with code picked from name field.'
        )

    def handle(self, **options):
        sys.stdout.write('\nLoading APIs data....')

        with transaction.atomic():
            if options.get('delete_data_sources', False):
                accounts_models.DataLayerDataSourceRelationship.objects.all().update(
                    deleted=get_current_datetime_object())
                accounts_models.DataLayer.objects.all().update(deleted=get_current_datetime_object())
                accounts_models.DataSource.objects.all().update(deleted=get_current_datetime_object())

        with transaction.atomic():
            if options.get('update_data_sources', False):
                load_data_sources_data()

        with transaction.atomic():
            if options.get('delete_data_layers', False):
                accounts_models.DataLayerDataSourceRelationship.objects.filter(
                    data_layer__in=list(
                        accounts_models.DataLayer.objects.filter(created_by__isnull=True).values_list('id', flat=True))
                ).update(deleted=get_current_datetime_object())

                accounts_models.DataLayer.objects.filter(created_by__isnull=True).update(
                    deleted=get_current_datetime_object())

        if options.get('update_data_layers', False):
            load_system_data_layers_data()

        if options.get('update_data_layers_code', False):
            populate_data_layer_codes()

        sys.stdout.write('\nData loaded successfully!\n')
