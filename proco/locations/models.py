import numpy as np
from django.contrib.gis.db.models import MultiPolygonField
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon
from django.db import models
from django.utils import timezone
from django.utils.translation import ugettext as _
from jsonfield import JSONField
from model_utils.models import TimeStampedModel
from mptt.models import MPTTModel, TreeForeignKey
from sklearn.cluster import MiniBatchKMeans
# from sklearn.neighbors import DistanceMetric
from sklearn.metrics import DistanceMetric

from proco.core.models import BaseModelMixin, CustomDateTimeField
from proco.locations.managers import CountryManager
from proco.locations.utils import get_random_name_image
from proco.utils.cache import cache_manager


class BBoxMixin(models.Model):
    centroid = JSONField(null=True, blank=True, default=list)
    bbox = JSONField(null=True, blank=True, default=list)

    class Meta:
        abstract = True


class GeometryMixin(models.Model):
    geometry = MultiPolygonField(verbose_name=_('Country border geometry'), null=True, blank=True)
    geometry_simplified = MultiPolygonField(verbose_name=_('Simplified Geometry'), null=True, blank=True)

    class Meta:
        abstract = True

    @classmethod
    def to_multipolygon(cls, geos_geom: [MultiPolygon, Polygon]) -> MultiPolygon:
        return MultiPolygon(geos_geom) if isinstance(geos_geom, Polygon) else geos_geom

    @classmethod
    def optimize_geometry(cls, geometry: [GEOSGeometry]) -> [MultiPolygon]:
        if geometry is None:
            return geometry

        # magic numbers
        tolerance = 0.03
        tolerance_divider = 4
        max_attempts = 5

        for _i in range(max_attempts):
            geometry_simplified = geometry.simplify(tolerance=tolerance)
            if not geometry_simplified.empty:
                return cls.to_multipolygon(geometry_simplified)

            tolerance = tolerance / tolerance_divider

        return geometry

    def save(self, *args, **kwargs):
        self.geometry_simplified = self.optimize_geometry(self.geometry)

        super().save(*args, **kwargs)


class Country(GeometryMixin, TimeStampedModel):
    name = models.CharField(max_length=255)
    code = models.CharField(max_length=32)

    iso3_format = models.CharField(max_length=32, null=True, blank=True)

    flag = models.ImageField(verbose_name=_('Country flag'), upload_to=get_random_name_image)
    map_preview = models.ImageField(upload_to=get_random_name_image, null=True, blank=True, default=None)

    description = models.TextField(max_length=1000, blank=True, default='')
    data_source = models.TextField(max_length=500, blank=True, default='')
    data_source_description = models.TextField(max_length=500, blank=True, default='')

    date_of_join = models.DateField(null=True, blank=True, default=None)
    date_schools_mapped = models.DateField(null=True, blank=True, default=None)

    benchmark_metadata = JSONField(null=True, default=dict)

    last_weekly_status = models.ForeignKey(
        'connection_statistics.CountryWeeklyStatus',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='_country',
    )

    deleted = CustomDateTimeField(db_index=True, null=True, blank=True)

    objects = CountryManager()

    class Meta:
        ordering = ('name',)
        verbose_name = _('Country')
        verbose_name_plural = _('Countries')

    def __str__(self):
        return f'{self.name}'

    def invalidate_country_related_cache(self):
        cache_manager.invalidate((
            'GLOBAL_STATS*',
            'COUNTRIES_LIST*',
            'COUNTRY_INFO_pk_{0}'.format(str(self.code).lower()),
            'SCHOOLS_{0}_*'.format(str(self.code).lower()),
            'CONNECTIVITY_CONFIGURATIONS_STATS_*_country_id_*{}*'.format(self.id),
            'DATA_LAYER_INFO_*_country_id_*{}*'.format(self.id),
        ))

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        self.invalidate_country_related_cache()
        # cache_manager.invalidate('COUNTRY_BOUNDARY')

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()

        for school in self.schools.all():
            school.delete()

        self.daily_status.all().update(deleted=timezone.now())
        self.weekly_status.all().update(deleted=timezone.now())

        self.invalidate_country_related_cache()

    def _calculate_batch_avg_distance_school(self, points):
        earth_radius = 6371.0088
        dist = DistanceMetric.get_metric('haversine')
        distances = dist.pairwise(np.radians(points))
        indexes = np.tril_indices(n=distances.shape[0], k=-1, m=distances.shape[1])
        return earth_radius * np.mean(distances[indexes])

    def calculate_avg_distance_school(self):
        schools_count = self.schools.count()
        schools_point = self.schools.annotate(
            lon=models.Func(models.F('geopoint'), function='ST_X', output_field=models.FloatField()),
            lat=models.Func(models.F('geopoint'), function='ST_Y', output_field=models.FloatField()),
        ).values_list('lat', 'lon')

        if schools_count < 2:
            return None
        elif schools_count < 5000:
            return self._calculate_batch_avg_distance_school(schools_point)
        else:
            kmeans = MiniBatchKMeans(n_clusters=5000, batch_size=250, random_state=0).fit(schools_point)
            return self._calculate_batch_avg_distance_school(kmeans.cluster_centers_)

    def _clear_data_country(self):
        from proco.connection_statistics.models import CountryWeeklyStatus

        for school in self.schools.all():
            school.delete()

        for daily_status in self.daily_status.all():
            daily_status.delete()

        for weekly_status in self.weekly_status.all():
            weekly_status.delete()

        first_weekly_status = CountryWeeklyStatus.objects.create(country=self)
        self.last_weekly_status = first_weekly_status
        self.save(update_fields=('last_weekly_status',))


class Location(GeometryMixin, TimeStampedModel, MPTTModel):
    name = models.CharField(max_length=255)
    country = models.ForeignKey(Country, related_name='country_location', on_delete=models.CASCADE)
    parent = TreeForeignKey(
        'self',
        related_name='children',
        null=True,
        blank=True,
        db_index=True,
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ('id',)

    def __str__(self):
        return f'{self.name} - {self.country}'


class CountryAdminMetadata(BaseModelMixin, BBoxMixin):
    country = models.ForeignKey(
        Country,
        related_name='country_admins',
        null=True,
        blank=True,
        db_index=True,
        on_delete=models.CASCADE,
    )

    LAYER_NAME_ADMIN0 = 'adm0'  # Country
    LAYER_NAME_ADMIN1 = 'adm1'  # State
    LAYER_NAME_ADMIN2 = 'adm2'  # City

    LAYER_NAME_CHOICES = (
        (LAYER_NAME_ADMIN0, 'Admin 0'),
        (LAYER_NAME_ADMIN1, 'Admin 1'),
        (LAYER_NAME_ADMIN2, 'Admin 2'),
    )

    layer_name = models.CharField(
        max_length=10,
        choices=LAYER_NAME_CHOICES,
        verbose_name='Layer Name',
        default=LAYER_NAME_ADMIN1,
    )

    name = models.CharField(
        max_length=255,
        null=False,
        verbose_name='Name',
        db_index=True,
    )
    name_en = models.CharField(max_length=255, blank=True, null=True)
    description = models.CharField(max_length=255, blank=True, null=True)
    description_ui_label = models.CharField(max_length=255, default='Admins')

    giga_id_admin = models.CharField(max_length=50, blank=True, null=True)
    mapbox_id = models.CharField(max_length=100, blank=True, null=True)

    parent = models.ForeignKey(
        'self',
        related_name='children',
        null=True,
        blank=True,
        db_index=True,
        on_delete=models.SET_NULL,
    )

    class Meta:
        ordering = ('id',)

    def __str__(self):
        return f'{self.name} - {self.name_en} - {self.country}'
