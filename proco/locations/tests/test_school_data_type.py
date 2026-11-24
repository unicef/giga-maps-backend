import io
import os
import shutil
import tempfile

from django.conf import settings
from django.contrib.gis.geos import GEOSGeometry
from django.core.cache import cache
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import CommandError, call_command
from django.test import TestCase, override_settings
from django.urls import resolve, reverse

from rest_framework import status

from PIL import Image

from proco.custom_auth.tests import test_utils as test_utilities
from proco.entities.factories import EntityFactory
from proco.entities.models import EntityType
from proco.locations.models import Country
from proco.locations.tests.factories import CountryFactory
from proco.locations.tests.test_api import locations_url
from proco.schools.tests.factories import SchoolFactory
from proco.utils.cache import cache_manager
from proco.utils.tests import TestAPIViewSetMixin


def _flag_file():
    flag_bytes = io.BytesIO()
    Image.new('RGB', (1, 1)).save(flag_bytes, format='PNG')
    return SimpleUploadedFile('flag.png', flag_bytes.getvalue(), content_type='image/png')


def _cache_entry(key):
    return cache.get('{0}_{1}'.format(cache_manager.CACHE_PREFIX, key))


class CountrySchoolDataTypeModelTestCase(TestCase):

    def setUp(self):
        cache.clear()

    def test_defaults_to_not_mapped(self):
        self.assertEqual(CountryFactory().school_data_type, Country.SCHOOL_DATA_TYPE_NOT_MAPPED)

    def test_save_invalidates_search_and_v2_country_list_keys(self):
        country = CountryFactory()
        keys = (
            'GLOBAL_COUNTRY_SEARCH_MAPPING_',
            'V2_ENTITY_COUNTRIES_LIST_',
            'V2_ENTITY_COUNTRIES_LIST_ordering_name',
            'V2_ENTITY_COUNTRY_DETAIL_pk_{0}'.format(country.code.lower()),
        )
        for key in keys:
            cache_manager.set(key, ['cached'], request_path='/some/path/')

        country.school_data_type = Country.SCHOOL_DATA_TYPE_GOVERNMENT
        country.save()

        for key in keys:
            self.assertTrue(_cache_entry(key)['invalidated'], key)


class CountrySchoolDataTypeApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default']

    @classmethod
    def setUpTestData(cls):
        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        self.media_root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.media_root, True)
        media_override = override_settings(MEDIA_ROOT=self.media_root)
        media_override.enable()
        self.addCleanup(media_override.disable)

        self.country_with_schools = CountryFactory(
            flag=_flag_file(), school_data_type=Country.SCHOOL_DATA_TYPE_GOVERNMENT,
        )
        self.country_without_schools = CountryFactory(school_data_type=Country.SCHOOL_DATA_TYPE_OSM)
        SchoolFactory(country=self.country_with_schools, admin1=None)
        super().setUp()

    def _update_country(self, data):
        url, view, view_info = locations_url(
            (self.country_with_schools.id,), {}, view_name='update-retrieve-country',
        )
        payload = {
            'name': self.country_with_schools.name,
            'code': self.country_with_schools.code,
            'benchmark_metadata': {},
            'active_layers_list': [],
            'active_filters_list': [],
        }
        payload.update(data)
        return self.forced_auth_req('put', url, user=self.user, data=payload, view=view, view_info=view_info)

    def _search_by_country_id(self):
        url, _, view = locations_url((), {}, view_name='search-countries-admin-schools')
        response = self.forced_auth_req('get', url, view=view)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return {row['country_id']: row for row in response.data}

    def test_search_returns_school_data_type_with_and_without_schools(self):
        url, _, view = locations_url((), {}, view_name='search-countries-admin-schools')

        # schools aggregate, countries, admins, countries without schools
        with self.assertNumQueries(3):
            response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        rows = {row['country_id']: row for row in response.data}
        self.assertEqual(rows[self.country_with_schools.id]['school_data_type'], 'government')
        self.assertEqual(rows[self.country_without_schools.id]['school_data_type'], 'osm')
        self.assertIn('integration_status', rows[self.country_with_schools.id])
        self.assertIn('integration_status', rows[self.country_without_schools.id])

    def test_country_list_and_detail_return_school_data_type(self):
        list_url, _, list_view = locations_url((), {})
        list_response = self.forced_auth_req('get', list_url, view=list_view)
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            {row['code']: row['school_data_type'] for row in list_response.data}[self.country_with_schools.code],
            'government',
        )

        v2_list = self.forced_auth_req('get', reverse('entities:list-entity-countries'))
        self.assertEqual(v2_list.status_code, status.HTTP_200_OK)
        self.assertEqual(
            {row['code']: row['school_data_type'] for row in v2_list.data},
            {self.country_with_schools.code: 'government', self.country_without_schools.code: 'osm'},
        )

        v2_detail = self.forced_auth_req(
            'get', reverse('entities:retrieve-entity-country', args=[self.country_with_schools.code.lower()]),
        )
        self.assertEqual(v2_detail.status_code, status.HTTP_200_OK)
        self.assertEqual(v2_detail.data['school_data_type'], 'government')

    def test_update_accepts_each_valid_value(self):
        for value in ('osm', 'not_mapped', 'government'):
            response = self._update_country({'school_data_type': value})

            self.assertEqual(response.status_code, status.HTTP_200_OK, value)
            self.assertEqual(response.data['school_data_type'], value)
            self.country_with_schools.refresh_from_db()
            self.assertEqual(self.country_with_schools.school_data_type, value)

    def test_update_rejects_unknown_value(self):
        response = self._update_country({'school_data_type': 'engaged'})

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('school_data_type', response.data)
        self.country_with_schools.refresh_from_db()
        self.assertEqual(self.country_with_schools.school_data_type, 'government')

    def test_update_without_the_field_keeps_the_stored_value(self):
        response = self._update_country({'description': 'Edited by an admin form that predates the field'})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.country_with_schools.refresh_from_db()
        self.assertEqual(self.country_with_schools.school_data_type, 'government')

    def test_admin_update_shows_up_in_search_after_cached_read(self):
        self.assertEqual(self._search_by_country_id()[self.country_with_schools.id]['school_data_type'], 'government')
        self.assertFalse(_cache_entry('GLOBAL_COUNTRY_SEARCH_MAPPING_')['invalidated'])

        self.assertEqual(self._update_country({'school_data_type': 'osm'}).status_code, status.HTTP_200_OK)

        self.assertTrue(_cache_entry('GLOBAL_COUNTRY_SEARCH_MAPPING_')['invalidated'])
        url, _, view = locations_url((), {'cache': 'off'}, view_name='search-countries-admin-schools')
        fresh = {row['country_id']: row for row in self.forced_auth_req('get', url, view=view).data}
        self.assertEqual(fresh[self.country_with_schools.id]['school_data_type'], 'osm')


class LoadCountrySchoolDataTypeCommandTestCase(TestCase):
    CSV_HEADER = 'iso3_format,country_name,OSM_Govt_final,data_source\n'

    def setUp(self):
        cache.clear()
        self.tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp_dir, True)
        self.government = CountryFactory(iso3_format='GOV')
        self.osm = CountryFactory(iso3_format='osm')
        self.not_mapped = CountryFactory(iso3_format='NMP', school_data_type=Country.SCHOOL_DATA_TYPE_OSM)
        self.not_in_csv = CountryFactory(iso3_format='XXX', school_data_type=Country.SCHOOL_DATA_TYPE_OSM)

    def _write_csv(self, rows):
        path = os.path.join(self.tmp_dir, 'countries.csv')
        # The real export is not UTF-8: accented names arrive as single latin-1 bytes.
        with open(path, 'wb') as csv_file:
            csv_file.write((self.CSV_HEADER + ''.join(rows)).encode('latin-1'))
        return path

    def _valid_csv(self):
        return self._write_csv([
            "GOV,C\xf4te d'Ivoire,Government,x\n",
            'OSM,S\xe3o Tom\xe9,OSM,x\n',
            'NMP,Nowhere,Not mapped,x\n',
            'ZZZ,Unknown land,Government,x\n',
        ])

    def _run(self, path, dry_run=False):
        out = io.StringIO()
        args = ['load_country_school_data_type', '--file', path]
        if dry_run:
            args.append('--dry-run')
        call_command(*args, stdout=out)
        return out.getvalue()

    def _stored_types(self):
        return dict(Country.objects.values_list('iso3_format', 'school_data_type'))

    def test_dry_run_reports_but_writes_nothing(self):
        cache_manager.set('GLOBAL_COUNTRY_SEARCH_MAPPING_', ['cached'], request_path='/p/')
        before = self._stored_types()

        output = self._run(self._valid_csv(), dry_run=True)

        self.assertEqual(self._stored_types(), before)
        self.assertFalse(_cache_entry('GLOBAL_COUNTRY_SEARCH_MAPPING_')['invalidated'])
        self.assertIn('DRY RUN', output)
        self.assertIn('Changes that would be applied (3)', output)
        self.assertIn('GOV {0} (id {1}): not_mapped -> government'.format(
            self.government.name, self.government.id), output)
        self.assertIn('CSV rows without an active country (1): ZZZ', output)
        self.assertIn('Active countries without a CSV row, left unchanged (1): XXX', output)

    def test_applies_mapping_and_invalidates_cache_once(self):
        cache_manager.set('GLOBAL_COUNTRY_SEARCH_MAPPING_', ['cached'], request_path='/p/')
        cache_manager.set('V2_ENTITY_COUNTRIES_LIST_', ['cached'], request_path='/p/')

        output = self._run(self._valid_csv())

        self.assertEqual(self._stored_types(), {
            'GOV': 'government', 'osm': 'osm', 'NMP': 'not_mapped', 'XXX': 'osm',
        })
        self.assertIn('APPLIED', output)
        self.assertTrue(_cache_entry('GLOBAL_COUNTRY_SEARCH_MAPPING_')['invalidated'])
        self.assertTrue(_cache_entry('V2_ENTITY_COUNTRIES_LIST_')['invalidated'])

    def test_second_run_has_no_changes(self):
        path = self._valid_csv()
        self._run(path)

        self.assertIn('applied (0)', self._run(path))

    def test_unknown_value_aborts_without_writing(self):
        before = self._stored_types()
        path = self._write_csv(['GOV,Somewhere,Government,x\n', 'OSM,Elsewhere,Engaged,x\n'])

        with self.assertRaisesMessage(CommandError, 'OSM has unknown OSM_Govt_final value "Engaged"'):
            self._run(path)

        self.assertEqual(self._stored_types(), before)

    def test_missing_column_is_an_error(self):
        path = os.path.join(self.tmp_dir, 'bad.csv')
        with open(path, 'w') as csv_file:
            csv_file.write('iso3_format,country_name\nGOV,Somewhere\n')

        with self.assertRaisesMessage(CommandError, 'Missing CSV columns: OSM_Govt_final'):
            self._run(path)

    def test_duplicated_iso3_in_db_aborts(self):
        CountryFactory(iso3_format='gov')

        with self.assertRaisesMessage(CommandError, 'GOV'):
            self._run(self._valid_csv())

        self.assertEqual(Country.objects.get(id=self.government.id).school_data_type, 'not_mapped')


def _mvt_fields(buf):
    i = 0
    while i < len(buf):
        key, i = _mvt_varint(buf, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, i = _mvt_varint(buf, i)
        elif wire == 2:
            size, i = _mvt_varint(buf, i)
            value, i = buf[i:i + size], i + size
        else:
            size = 8 if wire == 1 else 4
            value, i = buf[i:i + size], i + size
        yield field, value


def _mvt_varint(buf, i):
    result = shift = 0
    while True:
        byte = buf[i]
        i += 1
        result |= (byte & 0x7F) << shift
        shift += 7
        if byte < 0x80:
            return result, i


def _mvt_feature_keys(tile):
    """Map each MVT layer name to the list of property-key sets of its features."""
    layers = {}
    for field, layer in _mvt_fields(tile):
        if field != 3:
            continue
        name, keys, features = None, [], []
        for layer_field, value in _mvt_fields(layer):
            if layer_field == 1:
                name = value.decode()
            elif layer_field == 3:
                keys.append(value.decode())
            elif layer_field == 2:
                features.append(value)
        layers[name] = []
        for feature in features:
            tags = b''.join(value for feature_field, value in _mvt_fields(feature) if feature_field == 2)
            indexes, j = [], 0
            while j < len(tags):
                index, j = _mvt_varint(tags, j)
                indexes.append(index)
            layers[name].append({keys[k] for k in indexes[0::2]})
    return layers


# Tiles are read through the read-only alias; pointing it at 'default' keeps the rows created here visible
# whatever the local test mirror setup is.
@override_settings(READ_ONLY_DB_KEY='default')
class PointTilesCountryIdTestCase(TestAPIViewSetMixin, TestCase):
    databases = {'default', settings.READ_ONLY_DB_KEY}

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()
        SchoolFactory(country=cls.country, admin1=None, geopoint=GEOSGeometry('Point(1 1)'))
        SchoolFactory(country=cls.country, admin1=None, geopoint=GEOSGeometry('Point(2 2)'))
        # Same rows as the seeded environments (entities_entity_type in the staging dump).
        EntityType.objects.update_or_create(code='school', defaults={
            'name': 'School',
            'is_legacy': True,
            'master_data_model': 'data_sources.SchoolMasterData',
            'extra_config': {'main_table': 'schools_school', 'srid': '4326'},
        })
        EntityType.objects.update_or_create(code='health', defaults={
            'name': 'Health Facility',
            'master_data_model': 'data_sources.HealthEntityMasterIntermediateData',
            'detail_model': 'entities.HealthEntity',
            'detail_related_name': 'health_entity',
            'extra_config': {'main_table': 'entities_entity', 'srid': '4326'},
        })
        EntityFactory.create_entity(
            'health', country=cls.country, name='Clinic', giga_id='clinic-1', geopoint=GEOSGeometry('Point(3 3)'),
        )

    def setUp(self):
        cache.clear()
        super().setUp()

    def _tile_layers(self, view_name, query):
        path = reverse('entities:' + view_name)
        url = '{0}?{1}&z=0&x=0&y=0.mvt'.format(path, query)
        view = resolve(path)
        response = self.forced_auth_req('get', url, view=view, view_info=view.func)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return _mvt_feature_keys(response.content)

    def _assert_every_feature_has_country_id(self, layers, expected_counts):
        self.assertEqual({name: len(features) for name, features in layers.items()}, expected_counts)
        for name, features in layers.items():
            for feature_keys in features:
                self.assertIn('country_id', feature_keys, name)

    def test_global_connectivity_tiles(self):
        layers = self._tile_layers('tiles-global-connectivity-view', 'entity_type__code=school,health&cache=off')

        self._assert_every_feature_has_country_id(layers, {'school': 2, 'entities': 1})

    def test_connectivity_status_combined_tiles(self):
        layers = self._tile_layers('tiles-entity-connectivity-status-view', 'cache=off')

        self._assert_every_feature_has_country_id(layers, {'school': 2, 'entities': 1})

    def test_connectivity_status_school_only_tiles(self):
        layers = self._tile_layers('tiles-entity-connectivity-status-view', 'entity_type__code=school&cache=off')

        self._assert_every_feature_has_country_id(layers, {'school': 2})
