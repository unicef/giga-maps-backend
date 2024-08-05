from rest_framework import serializers

from proco.connection_statistics.models import (
    CountryDailyStatus,
    CountryWeeklyStatus,
    SchoolDailyStatus,
    SchoolWeeklyStatus,
    SchoolRealTimeRegistration,
)
from proco.schools.models import School


class CountryWeeklyStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = CountryWeeklyStatus
        fields = (
            'schools_total',
            'schools_connected',
            'schools_connectivity_unknown',
            'schools_connectivity_no',
            'schools_connectivity_moderate',
            'schools_connectivity_good',
            'schools_coverage_unknown',
            'schools_coverage_no',
            'schools_coverage_moderate',
            'schools_coverage_good',
            'connectivity_speed',
            'integration_status',
            'connectivity_availability',
            'coverage_availability',
            'avg_distance_school',
            'created',
            'modified',
            'global_schools_connectivity_unknown',
            'global_schools_connectivity_no',
            'global_schools_connectivity_moderate',
            'global_schools_connectivity_good',
        )
        read_only_fields = fields


class SchoolWeeklyStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = SchoolWeeklyStatus
        fields = (
            'num_students',
            'num_teachers',
            'num_classroom',
            'num_latrines',
            'running_water',
            'electricity_availability',
            'computer_lab',
            'num_computers',
            'connectivity',
            'connectivity_type',
            'connectivity_speed',
            'connectivity_upload_speed',
            'connectivity_latency',
            'coverage_availability',
            'coverage_type',
            'created',
            'modified',
        )
        read_only_fields = fields


class CountryDailyStatusSerializer(serializers.ModelSerializer):
    year = serializers.ReadOnlyField(source='date.year')
    week = serializers.SerializerMethodField()
    weekday = serializers.SerializerMethodField()

    class Meta:
        model = CountryDailyStatus
        fields = (
            'date',
            'year',
            'week',
            'weekday',
            'connectivity_speed',
            'connectivity_upload_speed',
            'connectivity_latency',
        )
        read_only_fields = fields

    def get_week(self, obj):
        return obj.date.isocalendar()[1]

    def get_weekday(self, obj):
        return obj.date.isocalendar()[2]


class SchoolDailyStatusSerializer(serializers.ModelSerializer):
    year = serializers.ReadOnlyField(source='date.year')
    week = serializers.SerializerMethodField()
    weekday = serializers.SerializerMethodField()

    class Meta:
        model = SchoolDailyStatus
        fields = (
            'date',
            'year',
            'week',
            'weekday',
            'connectivity_speed',
            'connectivity_upload_speed',
            'connectivity_latency',
        )
        read_only_fields = fields

    def get_week(self, obj):
        return obj.date.isocalendar()[1]

    def get_weekday(self, obj):
        return obj.date.isocalendar()[2]


class SchoolConnectivityStatusSerializer(serializers.ModelSerializer):
    country_name = serializers.ReadOnlyField(source='country.name')

    admin1_name = serializers.ReadOnlyField(source='admin1.name', default='')
    admin2_name = serializers.ReadOnlyField(source='admin2.name', default='')

    admin1_code = serializers.ReadOnlyField(source='admin1.giga_id_admin', default='')
    admin2_code = serializers.ReadOnlyField(source='admin2.giga_id_admin', default='')

    admin1_description_ui_label = serializers.ReadOnlyField(source='admin1.description_ui_label', default='')
    admin2_description_ui_label = serializers.ReadOnlyField(source='admin2.description_ui_label', default='')

    is_data_synced = serializers.SerializerMethodField()

    live_avg = serializers.SerializerMethodField()
    live_avg_connectivity = serializers.SerializerMethodField()
    connectivity_status = serializers.SerializerMethodField()

    statistics = serializers.SerializerMethodField()
    graph_data = serializers.SerializerMethodField()

    is_rt_connected = serializers.SerializerMethodField()

    class Meta:
        model = School
        fields = (
            'id',
            'name',
            'external_id',
            'giga_id_school',
            'is_rt_connected',
            'live_avg',
            'live_avg_connectivity',
            'connectivity_status',
            'admin1_name',
            'admin1_code',
            'admin1_description_ui_label',
            'admin2_name',
            'admin2_code',
            'admin2_description_ui_label',
            'country_name',
            'is_data_synced',
            'geopoint',
            'statistics',
            'graph_data',
            'education_level',
            'environment',
        )
        read_only_fields = fields

    def __init__(self, *args, **kwargs):
        self.country = kwargs.pop('country', None)
        self.graph_data = kwargs.pop('graph_data', None)
        self.speed_benchmark = kwargs.pop('speed_benchmark', 20000000)

        super(SchoolConnectivityStatusSerializer, self).__init__(*args, **kwargs)

    def get_live_avg(self, instance):
        speed = None
        school_daily_data = self.graph_data.get(instance.id, [])
        positive_speeds = list(filter(lambda val: val is not None, [daily_data['value']
                                                                    for daily_data in school_daily_data]))
        if len(positive_speeds) > 0:
            speed = round(sum(positive_speeds) / len(positive_speeds), 2)

        return speed

    def get_connectivity_status(self, instance):
        status = instance.connectivity_status
        if status in ['good', 'moderate']:
            return 'connected'
        elif status == 'no':
            return 'not_connected'
        return 'unknown'

    def get_statistics(self, instance):
        school_weekly_data = SchoolWeeklyStatusSerializer(instance.last_weekly_status).data
        if school_weekly_data:
            school_weekly_data['connectivity_status'] = self.get_connectivity_status(instance)
            school_weekly_data['connectivity_speed'] = self.get_live_avg(instance)

        return school_weekly_data

    def get_graph_data(self, instance):
        return self.graph_data.get(instance.id, [])

    def get_is_data_synced(self, instance):
        return SchoolRealTimeRegistration.objects.filter(
            school=instance,
            rt_registered=True,
        ).exists()

    def get_is_rt_connected(self, instance):
        return instance.is_rt_connected

    def get_live_avg_connectivity(self, instance):
        live_avg = self.get_live_avg(instance)

        rounded_benchmark_value_int = round(self.speed_benchmark / 1000000, 2)
        rounded_base_benchmark_int = 1

        live_avg_connectivity = 'unknown'

        if live_avg is not None:
            if live_avg > rounded_benchmark_value_int:
                live_avg_connectivity = 'good'
            elif rounded_base_benchmark_int <= live_avg <= rounded_benchmark_value_int:
                live_avg_connectivity = 'moderate'
            elif live_avg < rounded_base_benchmark_int:
                live_avg_connectivity = 'bad'

        return live_avg_connectivity


class SchoolCoverageStatusSerializer(serializers.ModelSerializer):
    country_name = serializers.ReadOnlyField(source='country.name')
    statistics = serializers.SerializerMethodField()

    admin1_name = serializers.ReadOnlyField(source='admin1.name', default='')
    admin2_name = serializers.ReadOnlyField(source='admin2.name', default='')

    admin1_code = serializers.ReadOnlyField(source='admin1.giga_id_admin', default='')
    admin2_code = serializers.ReadOnlyField(source='admin2.giga_id_admin', default='')

    admin1_description_ui_label = serializers.ReadOnlyField(source='admin1.description_ui_label', default='')
    admin2_description_ui_label = serializers.ReadOnlyField(source='admin2.description_ui_label', default='')

    coverage_status = serializers.SerializerMethodField()

    class Meta:
        model = School
        fields = (
            'id',
            'name',
            'external_id',
            'giga_id_school',
            'admin1_name',
            'admin1_code',
            'admin1_description_ui_label',
            'admin2_name',
            'admin2_code',
            'admin2_description_ui_label',
            'country_name',
            'coverage_type',
            'coverage_status',
            'education_level',
            'environment',
            'geopoint',
            'statistics',
        )
        read_only_fields = fields

    def get_coverage_status(self, instance):
        status = str(instance.coverage_type).lower() if instance.coverage_type else None
        if status in ['5g', '4g']:
            return 'good'
        elif status in ['3g', '2g']:
            return 'moderate'
        elif status == 'no':
            return 'bad'
        return 'unknown'

    def get_connectivity_status(self, instance):
        status = instance.connectivity_status
        if status in ['good', 'moderate']:
            return 'connected'
        elif status == 'no':
            return 'not_connected'
        return 'unknown'

    def get_statistics(self, instance):
        school_weekly_data = SchoolWeeklyStatusSerializer(instance.last_weekly_status).data
        if school_weekly_data:
            school_weekly_data['connectivity_status'] = self.get_connectivity_status(instance)
            school_weekly_data['connectivity_speed'] = round(school_weekly_data['connectivity_speed'] / 1000000, 2) if (
                school_weekly_data['connectivity_speed'] is not None) else 0

        return school_weekly_data


class CountryWeeklyStatusUpdateRetriveSerializer(serializers.ModelSerializer):
    class Meta:
        model = CountryWeeklyStatus
        fields = '__all__'


class ListCountryWeeklyStatusSerializer(CountryWeeklyStatusSerializer):
    country_name = serializers.CharField(source='country.name')

    integration_status_verbose = serializers.SerializerMethodField()

    class Meta(CountryWeeklyStatus.Meta):
        model = CountryWeeklyStatus
        fields = (
            'id',
            'country_name',
            'year',
            'week',
            'integration_status',
            'integration_status_verbose',
            'connectivity_speed',
            'schools_total',
            'schools_connected',
            'schools_connectivity_unknown',
            'schools_connectivity_no',
            'schools_connectivity_moderate',
            'schools_connectivity_good',
        )

    def get_integration_status_verbose(self, instance):
        return dict(CountryWeeklyStatus.INTEGRATION_STATUS_TYPES).get(instance.integration_status)


class DetailCountryWeeklyStatusSerializer(CountryWeeklyStatusSerializer):
    statistics = serializers.SerializerMethodField()
    data_status = serializers.SerializerMethodField()

    class Meta(CountryWeeklyStatus.Meta):
        fields = CountryWeeklyStatusSerializer.Meta.fields + ('statistics', 'data_status', 'geometry')

    def get_statistics(self, instance):
        return CountryWeeklyStatusSerializer(instance.last_weekly_status if instance.last_weekly_status else None).data


class CountryDailyStatusUpdateRetrieveSerializer(serializers.ModelSerializer):
    """
    CountryDailyStatusUpdateRetrieveSerializer
        Serializer to create Country.
    """

    class Meta:
        model = CountryDailyStatus
        fields = '__all__'


class ListCountryDailyStatusSerializer(CountryDailyStatusSerializer):
    country_name = serializers.CharField(source='country.name')

    class Meta(CountryDailyStatus.Meta):
        model = CountryDailyStatus
        fields = ('id', 'country_name', 'date', 'connectivity_speed', 'connectivity_latency',)


class DetailCountryDailyStatusSerializer(CountryDailyStatusSerializer):
    class Meta(CountryWeeklyStatus.Meta):
        fields = CountryDailyStatusUpdateRetrieveSerializer.Meta.fields


class SchoolWeeklySummaryUpdateRetrieveSerializer(serializers.ModelSerializer):
    """
    SchoolWeeklySummaryUpdateRetrieveSerializer
        Serializer to create Country.
    """

    class Meta:
        model = SchoolWeeklyStatus
        fields = '__all__'


class ListSchoolWeeklySummarySerializer(SchoolWeeklyStatusSerializer):
    school_name = serializers.CharField(source='school.name')

    class Meta(SchoolWeeklyStatus.Meta):
        model = SchoolWeeklyStatus
        fields = (
            'id',
            'school_name',
            'date',
            'year',
            'week',
            'connectivity_speed',
            'connectivity_latency',
            'num_students',
            'num_teachers',
        )


class DetailSchoolWeeklySummarySerializer(SchoolWeeklyStatusSerializer):
    class Meta(SchoolWeeklyStatus.Meta):
        fields = "__all__"


class SchoolDailyStatusUpdateRetriveSerializer(serializers.ModelSerializer):
    """
    CountryUpdateRetriveSerializer
        Serializer to create Country.
    """

    class Meta:
        model = SchoolDailyStatus
        fields = '__all__'


class ListSchoolDailyStatusSerializer(SchoolDailyStatusSerializer):
    school_name = serializers.CharField(source='school.name')

    class Meta(SchoolDailyStatus.Meta):
        model = SchoolDailyStatus
        fields = ('id', 'school_name', 'date', 'connectivity_speed', 'connectivity_latency',)


class DetailSchoolDailyStatusSerializer(SchoolDailyStatusSerializer):
    class Meta(SchoolDailyStatus.Meta):
        fields = "__all__"
