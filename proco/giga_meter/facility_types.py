"""
Per-facility-type configuration for the Giga Meter master-data sync pipeline
(proco/giga_meter/pipeline.py).

The pipeline shape - load from Delta Sharing, stage in an intermediate table,
promote PUBLISHED rows into an entity + versioned static snapshot, soft-delete
DELETED rows, track a per-country watermark - is shared across facility types.
Only the schema (which columns exist, how intermediate values map onto the
entity/static rows) differs. That per-type knowledge lives here, in a
FacilityTypeConfig, so adding a new facility type is "write a config", not
"copy tasks.py".

`school` is intentionally NOT migrated onto this yet. Its promote step layers
extra transform logic on top of the shared shape (a `Point()` geopoint built
from two source fields, an `environment` lookup table, a computed
`name_lower`), and it's the live production pipeline. Migrating it is a
deliberate follow-up once this engine has proven out on `health` - not
something to fold in blind alongside a from-scratch config format.
"""
import hashlib
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from proco.core import utils as core_utilities
from proco.core.config import app_config as core_configs
from proco.data_sources import utils as data_sources_utilities
from proco.giga_meter import models as giga_meter_models


# --- Field resolver helpers -------------------------------------------------
# Each resolver has the signature (row, ctx) -> value, where `row` is the
# pandas-Series-turned-model-instance-ish intermediate row and `ctx` is
# whatever `FacilityTypeConfig.build_context(row)` returned for this row.

def copy_field(attr):
    return lambda row, ctx: getattr(row, attr)


def copy_bool_field(attr):
    def _resolve(row, ctx):
        value = getattr(row, attr)
        if core_utilities.is_blank_string(value):
            return None
        return str(value).lower() in core_configs.true_choices
    return _resolve


def from_context(key):
    return lambda row, ctx: ctx[key]


def constant(value):
    return lambda row, ctx: value


@dataclass
class FieldSpec:
    target: str
    resolve: Callable[[object, dict], object]


@dataclass
class FacilityTypeConfig:
    key: str
    label: str

    entity_model: type
    intermediate_model: type
    static_model: type

    id_field: str  # giga id attribute name, shared by intermediate + entity rows
    # Per-type frame cleanup on load (fillna/lowercase govt-id, normalize display name),
    # e.g. data_sources_utilities.normalize_health_master_data_frame. Each facility type
    # owns its own normalize function, same as school always has - not a shared/generic one.
    normalize_frame: Callable[[object], object] = None

    # row -> update_or_create/filter lookup kwargs, used for both promote and soft-delete
    entity_lookup: Callable[[object], Dict] = None
    static_parent_fk_field: str = ''  # kwarg name on the static model pointing back at the entity
    last_static_fk_field: str = ''    # field on the entity model pointing at its latest static row

    build_context: Callable[[object], Dict] = lambda row: {}
    required_fields: Tuple[str, ...] = ()

    entity_fields: List[FieldSpec] = field(default_factory=list)
    static_fields: List[FieldSpec] = field(default_factory=list)

    data_source_config_key: str = ''
    # Optional: another DATA_SOURCE_CONFIG block to fall back to for any credential
    # left unset on this type's own block (e.g. health riding on school's Delta
    # Sharing endpoint/token until it gets its own). Leave unset for types that
    # don't want that fallback.
    fallback_data_source_config_key: Optional[str] = None


# --- health -----------------------------------------------------------------

def _health_signature(row, country_code):
    """Deterministic signature until product defines official feed algorithm."""
    parts = [
        row.health_id_giga or '',
        row.facility_name or '',
        str(row.latitude if row.latitude is not None else ''),
        str(row.longitude if row.longitude is not None else ''),
        country_code or '',
        row.facility_data_source or '',
    ]
    return hashlib.sha256('|'.join(parts).encode('utf-8')).hexdigest()


def _health_context(row):
    country_code = row.country.code if row.country else None
    facility_data_source = (
        row.facility_data_source
        if not core_utilities.is_blank_string(row.facility_data_source)
        else 'health-master'
    )
    return {
        'country_code': country_code,
        'facility_data_source': facility_data_source,
        'signature': _health_signature(row, country_code),
    }


# Same source attr -> same target key, raw copy, on both the entity and the static snapshot.
_HEALTH_SHARED_RAW_COPY_FIELDS = (
    'facility_type_govt', 'facility_ownership_govt', 'facility_level', 'facility_accessibility',
    'health_service_provider', 'facility_data_collection_year',
    'admin1_id_giga', 'admin2_id_giga', 'area_type_govt',
    'distance_to_closest_settlement', 'distance_to_country_boundary',
    'connectivity_type', 'connectivity_govt_collection_year', 'connectivity_catchment_coverage',
    'electricity_type', 'electricity_availability_hours', 'power_backup_system',
    'hmis_system', 'hmis_system_use', 'ers_system', 'ers_system_use',
    'num_staff', 'num_community_health_workers', 'num_community_health_workers_within_5km',
    'pop_est_govt', 'pop_est_hf', 'pop_within_1km', 'pop_within_3km', 'pop_within_5km', 'pop_within_10km',
)

# Kept as raw text on the live entity (mirrors the `health` table); cast to a
# real boolean on the `master_sync_health_static` snapshot.
_HEALTH_TEXT_ON_ENTITY_BOOL_ON_STATIC_FIELDS = (
    'connectivity', 'connectivity_govt', 'electricity_availability',
    'computer_availability', 'device_availability', 'tablets_availability',
)

HEALTH_CONFIG = FacilityTypeConfig(
    key='health',
    label='Health',
    entity_model=giga_meter_models.GigaMeter_Health,
    intermediate_model=giga_meter_models.GigaMeter_HealthMasterData,
    static_model=giga_meter_models.GigaMeter_HealthStatic,
    id_field='health_id_giga',
    normalize_frame=data_sources_utilities.normalize_health_master_data_frame,
    entity_lookup=lambda row: {'health_id_giga': row.health_id_giga},
    static_parent_fk_field='health',
    last_static_fk_field='last_health_static',
    build_context=_health_context,
    required_fields=('latitude', 'longitude'),
    data_source_config_key='HEALTH_MASTER',
    fallback_data_source_config_key='SCHOOL_MASTER',
    entity_fields=[
        FieldSpec('facility_name', lambda row, ctx: row.facility_name or 'Name unknown'),
        FieldSpec('facility_data_source', from_context('facility_data_source')),
        FieldSpec('signature', from_context('signature')),
        FieldSpec('latitude', copy_field('latitude')),
        FieldSpec('longitude', copy_field('longitude')),
        FieldSpec('is_facility_open', copy_bool_field('is_facility_open')),
        FieldSpec('admin1', copy_field('admin1')),
        FieldSpec('admin2', copy_field('admin2')),
        *(FieldSpec(f, copy_field(f)) for f in _HEALTH_SHARED_RAW_COPY_FIELDS),
        *(FieldSpec(f, copy_field(f)) for f in _HEALTH_TEXT_ON_ENTITY_BOOL_ON_STATIC_FIELDS),
        FieldSpec('country_code', from_context('country_code')),
        FieldSpec('deleted', constant(None)),
    ],
    static_fields=[
        FieldSpec('latitude', copy_field('latitude')),
        FieldSpec('longitude', copy_field('longitude')),
        FieldSpec('facility_data_source', from_context('facility_data_source')),
        FieldSpec('is_facility_open', copy_bool_field('is_facility_open')),
        *(FieldSpec(f, copy_field(f)) for f in _HEALTH_SHARED_RAW_COPY_FIELDS),
        *(FieldSpec(f, copy_bool_field(f)) for f in _HEALTH_TEXT_ON_ENTITY_BOOL_ON_STATIC_FIELDS),
    ],
)

FACILITY_TYPES = {
    HEALTH_CONFIG.key: HEALTH_CONFIG,
}
