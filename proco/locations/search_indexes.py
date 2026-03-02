from azure.search.documents.indexes.models import (
    SearchFieldDataType,
    SimpleField,
    SearchableField
)
from django.conf import settings

from proco.entities.models import Entity
from proco.schools.models import School


class SchoolIndex(object):
    school_id = SimpleField(name='school_id', type=SearchFieldDataType.String, key=True)

    id = SimpleField(name='id', type=SearchFieldDataType.Int64, filterable=True)

    name = SearchableField(
        name='name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )
    giga_id_school = SearchableField(
        name='giga_id_school',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )
    external_id = SearchableField(
        name='external_id',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )

    admin1_id = SimpleField(name='admin1_id', type=SearchFieldDataType.Int64, filterable=True)
    admin1_name = SearchableField(
        name='admin1_name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )

    admin2_id = SimpleField(name='admin2_id', type=SearchFieldDataType.Int64, filterable=True)
    admin2_name = SearchableField(
        name='admin2_name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )

    country_id = SimpleField(name='country_id', type=SearchFieldDataType.Int64, filterable=True)
    country_name = SearchableField(
        name='country_name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )
    country_code = SearchableField(
        name='country_code',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
    )

    row_score = SimpleField(name='row_score', type=SearchFieldDataType.Int32, sortable=True)

    class Meta:
        index_name = settings.AZURE_CONFIG.get('COGNITIVE_SEARCH', {}).get('SCHOOL_INDEX_NAME', 'giga-schools')
        model = School

        searchable_fields = (
            'country_id', 'country_name', 'country_code',
            'admin1_id', 'admin1_name',
            'admin2_id', 'admin2_name',
            'name', 'external_id', 'giga_id_school',
        )
        filterable_fields = (
            'country_id', 'country_name', 'country_code',
            'admin1_id', 'admin2_id',
            'admin1_name', 'admin2_name', 'id',
        )
        ordering = ('-row_score', 'country_name', 'admin1_name', 'admin2_name', 'name',)


class EntityIndex(object):
    entity_id = SimpleField(name='entity_id', type=SearchFieldDataType.String, key=True)
    entity_type = SimpleField(
        name='entity_type_code',
        type=SearchFieldDataType.String,
        filterable=True,
        sortable=True,
    )

    id = SimpleField(name='id', type=SearchFieldDataType.Int64, filterable=True)

    name = SearchableField(
        name='name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )
    giga_id = SearchableField(
        name='giga_id',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )
    external_id = SearchableField(
        name='external_id',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )

    admin1_id = SimpleField(name='admin1_id', type=SearchFieldDataType.Int64, filterable=True)
    admin1_name = SearchableField(
        name='admin1_name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )

    admin2_id = SimpleField(name='admin2_id', type=SearchFieldDataType.Int64, filterable=True)
    admin2_name = SearchableField(
        name='admin2_name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )

    country_id = SimpleField(name='country_id', type=SearchFieldDataType.Int64, filterable=True)
    country_name = SearchableField(
        name='country_name',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
        sortable=True,
    )
    country_code = SearchableField(
        name='country_code',
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
    )

    row_score = SimpleField(name='row_score', type=SearchFieldDataType.Int32, sortable=True)

    class Meta:
        index_name = settings.AZURE_CONFIG.get('COGNITIVE_SEARCH', {}).get('ENTITIES_INDEX_NAME')
        model = Entity

        searchable_fields = (
            'country_id', 'country_name', 'country_code',
            'admin1_id', 'admin1_name',
            'admin2_id', 'admin2_name',
            'name', 'external_id', 'giga_id',
        )
        filterable_fields = (
            'country_id', 'country_name', 'country_code',
            'admin1_id', 'admin2_id',
            'admin1_name', 'admin2_name', 'id',
            'entity_type_code',
        )
        ordering = ('entity_type_code', '-row_score', 'country_name', 'admin1_name', 'admin2_name', 'name',)
