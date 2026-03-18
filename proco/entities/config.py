"""
Entity Type Configuration Registry

This module provides a centralized configuration system for entity types.
The primary source of truth is the EntityType database model.
Usage:
    from proco.entities.config import get_entity_type_config, get_all_active_types

    # Get config for a specific type
    config = get_entity_type_config('health')

    # Get all active types
    types = get_all_active_types()
"""

import logging
from django.apps import apps
from django.core.exceptions import FieldDoesNotExist

from proco.connection_statistics.models import EntityWeeklyStatus
from proco.entities.models import EntityType, Entity

logger = logging.getLogger('gigamaps.' + __name__)

_PARAMETER_FIELD_CACHE = {}

def get_entity_type_config(code):
    """
    Get an EntityType instance by code, with in-process caching.
    Returns None if not found or inactive.
    """
    entity_type = EntityType.get_by_code(code)
    return entity_type


def get_all_active_types():
    """
    Returns a queryset of all active EntityType instances.
    """
    return EntityType.get_all_active()


def get_tile_config_for_type(code):
    """
    Returns tile generation configuration dict for the given entity type code.
    Returns a default config if the type is not found in the registry.
    """
    entity_type = get_entity_type_config(code)
    if entity_type:
        return entity_type.get_tile_config()

def get_detail_model_class(code):
    """
    Returns the entity-specific detail model class for the given entity type code.
    Returns None if not configured.
    """
    entity_type = get_entity_type_config(code)
    if entity_type:
        return entity_type.get_detail_model_class()
    return None


def get_master_data_model_class(code):
    """
    Returns the master data model class for the given entity type code.
    Returns None if not configured.
    """
    entity_type = get_entity_type_config(code)
    if entity_type:
        return entity_type.get_master_data_model_class()
    return None


def is_legacy_type(code):
    """
    Returns True if the given entity type code represents a legacy type
    (e.g. school that maps to the old School model).
    """
    entity_type = get_entity_type_config(code)
    if entity_type:
        return entity_type.is_legacy
    return code == 'school'


def build_parameter_config(entity_type_obj, field_name, entity_code):
    """
    Resolve table + column automatically from model metadata.
    """
    cache_key = (entity_code, field_name)
    if cache_key in _PARAMETER_FIELD_CACHE:
        return _PARAMETER_FIELD_CACHE[cache_key]

    entity_models = []
    detail_model = entity_type_obj.get_detail_model_class()

    if detail_model:
        entity_models.append((detail_model, "ews"))

    entity_models.append((EntityWeeklyStatus, "ews"))

    entity_models.append((Entity, "entities_entity"))

    for model, table_alias in entity_models:
        try:
            field = model._meta.get_field(field_name)

            config = {
                "col_name": field.column,
                "table_name": table_alias,
                "db_table": model._meta.db_table,
                "field_type": field.get_internal_type(),
            }

            _PARAMETER_FIELD_CACHE[cache_key] = config

            return config

        except FieldDoesNotExist:
            continue

    raise FieldDoesNotExist(
        f'Field "{field_name}" not found in any model '
        f'for entity "{entity_code}"'
    )

