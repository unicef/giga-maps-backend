from django.db.models import Q

from proco.accounts.models import AdvanceFilter
from proco.core import utils as core_utilities
from proco.entities.constants import ALL_ENTITIES


class EntityTypeCodeMixin:
    ENTITY_TYPE_CODE_PARAM = 'entity_type__code'

    @classmethod
    def parse_entity_type_code_param(cls, request):
        entity_type_code = request.query_params.get(cls.ENTITY_TYPE_CODE_PARAM)
        if not entity_type_code:
            return None
        return entity_type_code.strip().lower()

    @classmethod
    def parse_entity_type_code_params(cls, request):
        entity_type_codes = cls.parse_entity_type_code_param(request)
        if not entity_type_codes:
            return None

        entity_type_codes = [
            entity_type_code.strip().lower()
            for entity_type_code in entity_type_codes.split(',')
            if entity_type_code.strip()
        ]
        if not entity_type_codes or ALL_ENTITIES in entity_type_codes:
            return None

        return list(dict.fromkeys(entity_type_codes))

    def get_entity_type_code_param(self, request=None):
        request = request or self.request
        return self.parse_entity_type_code_param(request)

    def get_entity_type_code_params(self, request=None):
        request = request or self.request
        return self.parse_entity_type_code_params(request)

    def is_all_entity_type_codes_requested(self, request=None):
        return self.get_entity_type_code_params(request=request) is None


class EntityDetailFilterMixin:
    ENTITY_COMMON_FILTER_ALIASES = ('entities', 'entity_static')
    ENTITY_BASE_TABLE_NAME = 'entities_entity'
    ENTITY_BASE_PK_COLUMN = 'id'
    ENTITY_DETAIL_FK_COLUMN = 'entity_id'

    def get_entity_detail_filter_aliases(self, entity_type_obj):
        return list(
            AdvanceFilter.objects.filter(
                Q(entity_type=entity_type_obj) | Q(column_configuration__entity_type=entity_type_obj),
                status=AdvanceFilter.FILTER_STATUS_PUBLISHED,
            ).exclude(
                column_configuration__table_alias__in=self.ENTITY_COMMON_FILTER_ALIASES,
            ).values_list(
                'column_configuration__table_alias',
                flat=True,
            ).distinct()
        )

    def get_entity_detail_table_name(self, entity_type_obj):
        detail_model = entity_type_obj.get_detail_model_class()
        if not detail_model:
            return None
        return detail_model._meta.db_table

    def get_entity_detail_filter_extra(self, request, entity_type_obj, base_table_name=None):
        detail_table_name = self.get_entity_detail_table_name(entity_type_obj)
        if not detail_table_name:
            return {
                'tables': [],
                'where': [],
            }

        base_table_name = base_table_name or self.ENTITY_BASE_TABLE_NAME
        tables = []
        where = []

        for table_alias in self.get_entity_detail_filter_aliases(entity_type_obj):
            detail_filters = core_utilities.get_filter_sql(
                request,
                table_alias,
                detail_table_name,
                entity_type_obj.code,
            )
            if len(detail_filters) == 0:
                continue

            if detail_table_name not in tables:
                tables.append(detail_table_name)

            where.extend([
                '{0}."{1}" = {2}."{3}"'.format(
                    base_table_name,
                    self.ENTITY_BASE_PK_COLUMN,
                    detail_table_name,
                    self.ENTITY_DETAIL_FK_COLUMN,
                ),
                '{0}."deleted" IS NULL'.format(detail_table_name),
                detail_filters,
            ])

        return {
            'tables': tables,
            'where': where,
        }

    def apply_entity_detail_filters(self, queryset, entity_type_obj, request=None, base_table_name=None):
        request = request or self.request
        extra_kwargs = self.get_entity_detail_filter_extra(
            request,
            entity_type_obj,
            base_table_name=base_table_name,
        )
        if not extra_kwargs['where']:
            return queryset

        return queryset.extra(
            tables=extra_kwargs['tables'],
            where=extra_kwargs['where'],
        )
