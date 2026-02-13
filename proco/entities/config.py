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
from proco.entities.models import EntityType

logger = logging.getLogger('gigamaps.' + __name__)


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