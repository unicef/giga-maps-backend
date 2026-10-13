"""
Entity Factory Pattern

Provides factory methods for creating and resolving entity-specific instances,
serializers, and handling polymorphic behavior. All resolution is driven by
the EntityType database registry.

Usage:
    from proco.entities.factories import EntityFactory

    # Get the detail model class for a health entity
    model_class = EntityFactory.get_detail_model_class('health')

    # Create an entity with its type-specific detail record
    entity, detail = EntityFactory.create_entity('health', country=country, name='Clinic A', ...)

    # Resolve tile config
    tile_config = EntityFactory.get_tile_config('health')
"""

import logging
from proco.entities.models import Entity, EntityType

from proco.entities.config import (
    get_entity_type_config,
    get_all_active_types,
    get_tile_config_for_type,
    is_legacy_type,
)

logger = logging.getLogger('gigamaps.' + __name__)


class EntityFactory:
    """
    Central factory for entity type operations.
    Delegates to the EntityType registry for all model/config resolution.
    """

    @staticmethod
    def get_entity_type(code):
        """
        Returns the EntityType instance for the given code.
        Returns None if not found or inactive.
        """
        return get_entity_type_config(code)

    @staticmethod
    def get_detail_model_class(code):
        """
        Returns the entity-specific detail model class (e.g. HealthEntity, SchoolEntity).
        Returns None if not configured.
        """
        entity_type = get_entity_type_config(code)
        if entity_type:
            return entity_type.get_detail_model_class()
        return None

    @staticmethod
    def get_master_data_model_class(code):
        """
        Returns the master data model class for the given entity type.
        Returns None if not configured.
        """
        entity_type = get_entity_type_config(code)
        if entity_type:
            return entity_type.get_master_data_model_class()
        return None

    @staticmethod
    def get_tile_config(code):
        """
        Returns tile generation configuration dict for the given entity type.
        """
        return get_tile_config_for_type(code)

    @staticmethod
    def get_all_active_types():
        """
        Returns queryset of all active EntityType instances.
        """
        return get_all_active_types()

    @staticmethod
    def is_legacy(code):
        """
        Returns True if the entity type uses the legacy School model.
        """
        return is_legacy_type(code)

    @staticmethod
    def create_entity(entity_type_code, detail_kwargs=None, **entity_kwargs):
        """
        Create an Entity instance and optionally its type-specific detail record.

        Args:
            entity_type_code: The entity type code (e.g. 'health', 'school')
            detail_kwargs: Dict of fields for the type-specific detail model (optional)
            **entity_kwargs: Fields for the Entity model

        Returns:
            tuple: (entity_instance, detail_instance or None)

        Raises:
            ValueError: If entity_type_code is not found in the registry
        """

        entity_type = EntityType.get_by_code(entity_type_code)
        if not entity_type:
            raise ValueError(f'Unknown or inactive entity type: {entity_type_code}')

        # Create the base Entity
        entity = Entity(
            entity_type=entity_type,
            **entity_kwargs,
        )
        entity.save()

        # Create the type-specific detail record if detail model is configured
        detail_instance = None
        if detail_kwargs is not None:
            detail_model_class = entity_type.get_detail_model_class()
            if detail_model_class:
                detail_instance = detail_model_class(entity=entity, **detail_kwargs)
                detail_instance.save()
            else:
                logger.warning(
                    'No detail model configured for entity type "%s", skipping detail creation',
                    entity_type_code,
                )
        return entity, detail_instance

    @staticmethod
    def get_detail_instance(entity):
        """
        Given an Entity instance, returns its type-specific detail instance.
        Delegates to Entity.get_entity_specific_model() which uses the registry.
        """
        return entity.get_entity_specific_model()

    @staticmethod
    def get_entity_type_choices():
        """
        Returns entity type choices suitable for CharField choices parameter.
        """
        return EntityType.get_choices()
