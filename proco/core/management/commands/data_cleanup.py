import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Count, F
from django.db.models.functions import Extract

from proco.accounts.models import DataLayer, DataLayerCountryRelationship
from proco.connection_statistics import models as statistics_models
from proco.core.utils import get_current_datetime_object
from proco.data_sources import tasks as sources_tasks
from proco.data_sources.models import QoSData, SchoolMasterData
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils.dates import get_current_year
from proco.utils.tasks import redo_aggregations_task, populate_school_new_fields_task, rebuild_school_index

logger = logging.getLogger('gigamaps.' + __name__)


def delete_duplicate_schools_based_on_giga_id():
    # updated deleted time if multiple school has same deleted datetime
    rows_with_more_than_1_records = School.objects.all_deleted().values(
        'country_id', 'giga_id_school', 'deleted').annotate(
        total_records=Count('id', distinct=False),
    ).order_by('-total_records', 'country_id', 'giga_id_school', 'deleted').filter(total_records__gt=1)[:10000]
    logger.info('Queryset to get max 10K records to update the deleted time '
                'where more than 1 record has same Country, School Giga ID and deleted datetime '
                'in School table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        count = 1
        for deleted_row in School.objects.all_deleted().filter(
            country_id=row['country_id'],
            giga_id_school=row['giga_id_school'],
        ).order_by('-id')[1:]:
            logger.info('Deletion for: Id - {0}, Country ID - {1}, Giga ID - {2}'.format(
                deleted_row.id, deleted_row.country_id, deleted_row.giga_id_school))
            deleted_row.deleted = get_current_datetime_object() + timedelta(minutes=count)
            deleted_row.save(update_fields=('deleted',))
            count += 1

    # Delete duplicate schools
    rows_with_more_than_1_records = School.objects.all().values(
        'country_id', 'giga_id_school').annotate(
        total_records=Count('id', distinct=False),
    ).order_by('-total_records', 'country_id', 'giga_id_school').filter(total_records__gt=1)[:10000]
    logger.info('Queryset to get max 10K records to delete where more than 1 record has same '
                'Country and School Giga ID in School table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        for row_to_delete in School.objects.all().filter(
            country_id=row['country_id'],
            giga_id_school=row['giga_id_school'],
        ).order_by('-id')[1:]:
            logger.info('Deletion for: Id - {0}, Country ID - {1}, Giga ID - {2}'.format(
                row_to_delete.id, row_to_delete.country_id, row_to_delete.giga_id_school))
            row_to_delete.delete()


def delete_duplicate_schools_based_on_external_id():
    rows_with_more_than_1_records = School.objects.all_records().values(
        'country_id', 'external_id').annotate(
        total_records=Count('id', distinct=False),
    ).order_by('-total_records', 'country_id', 'external_id').filter(total_records__gt=1)[:10000]
    logger.info(
        'Queryset to get max 10K records to delete where more than 1 record has same Country and School External ID '
        'in School table: {0}'.format(rows_with_more_than_1_records.query))

    # for row in rows_with_more_than_1_records:
    #     for row_to_delete in School.objects.filter(
    #         country_id=row['country_id'],
    #         external_id=row['external_id'],
    #     ).order_by('-id')[1:]:
    #         print('Deletion for: Id - {0}, Country ID - {1}, External ID - {2}'.format(
    #             row_to_delete.id, row_to_delete.country_id, row_to_delete.external_id))
    #         # Hard deletion
    #         row_to_delete.delete(force=True)


def delete_duplicate_school_weekly_records():
    rows_with_more_than_1_records = statistics_models.SchoolWeeklyStatus.objects.all().values(
        'school_id', 'week', 'year').annotate(
        total_records=Count('school_id', distinct=False),
    ).order_by('-total_records', 'school_id', 'week', 'year').filter(total_records__gt=1)[:10000]
    logger.info('Queryset to get max 10K records to delete where more than 1 record has same Year, Week and '
                'School ID in School Weekly table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        school_id = row['school_id']
        last_weekly_id = None

        school = School.objects.filter(id=school_id).first()
        if school:
            last_weekly_id = school.last_weekly_status_id

        school_weekly_ids_to_delete = (statistics_models.SchoolWeeklyStatus.objects.filter(
            school_id=row['school_id'],
            week=row['week'],
            year=row['year'],
        ).values_list('id', flat=True).order_by('id'))

        if last_weekly_id in school_weekly_ids_to_delete:
            logger.info('School Last Weekly Status id ({0}) is IN the deletion list. '
                        'Hence skipping the current record and deleting all remaining.'.format(last_weekly_id))
            for row_to_delete in statistics_models.SchoolWeeklyStatus.objects.filter(
                id__in=school_weekly_ids_to_delete,
            ).exclude(id=last_weekly_id).order_by('-id'):
                logger.info('Deletion for: Id - {0}, Year - {1}, Week - {2}, School Id - {3}'.format(
                    row_to_delete.id, row_to_delete.year, row_to_delete.week, row_to_delete.school_id))
                # Hard deletion
                row_to_delete.delete(force=True)
        else:
            logger.info('School Last Weekly Status id ({0}) is NOT IN the deletion list. Hence skipping first '
                        'record and deleting all remaining based on ID DESC.'.format(last_weekly_id))
            for row_to_delete in statistics_models.SchoolWeeklyStatus.objects.filter(
                school_id=row['school_id'],
                week=row['week'],
                year=row['year'],
            ).order_by('-id')[1:]:
                logger.info('Deletion for: Id - {0}, Year - {1}, Week - {2}, School Id - {3}'.format(
                    row_to_delete.id, row_to_delete.year, row_to_delete.week, row_to_delete.school_id))
                # Hard deletion
                row_to_delete.delete(force=True)


def delete_duplicate_school_daily_records():
    rows_with_more_than_1_records = statistics_models.SchoolDailyStatus.objects.all().values(
        'school_id', 'date', 'live_data_source').annotate(
        total_records=Count('school_id', distinct=False),
    ).order_by('-total_records', 'school_id', 'date', 'live_data_source').filter(total_records__gt=1)[:10000]
    logger.info(
        'Queryset to get max 10K records to delete where more than 1 record has same Date, Live Data Source and '
        'School ID in School Daily table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        for row_to_delete in statistics_models.SchoolDailyStatus.objects.filter(
            school_id=row['school_id'],
            date=row['date'],
            live_data_source=row['live_data_source'],
        ).order_by('-id')[1:]:
            logger.info('Deletion for: Id - {0}, Date - {1}, Data Source - {2}, School Id - {3}'.format(
                row_to_delete.id, row_to_delete.date, row_to_delete.live_data_source, row_to_delete.school_id))
            # Hard deletion
            row_to_delete.delete(force=True)


def delete_duplicate_country_daily_records():
    rows_with_more_than_1_records = statistics_models.CountryDailyStatus.objects.all().values(
        'country_id', 'date', 'live_data_source').annotate(
        total_records=Count('country_id', distinct=False),
    ).order_by('-total_records', 'country_id', 'date', 'live_data_source').filter(total_records__gt=1)[:10000]
    logger.info(
        'Queryset to get max 10K records to delete where more than 1 record has same Date, Live Data Source and '
        'Country ID in Country Daily table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        for row_to_delete in statistics_models.CountryDailyStatus.objects.filter(
            country_id=row['country_id'],
            date=row['date'],
            live_data_source=row['live_data_source'],
        ).order_by('-id')[1:]:
            logger.info('Deletion for: Id - {0}, Date - {1}, Data Source - {2}, Country Id - {3}'.format(
                row_to_delete.id, row_to_delete.date, row_to_delete.live_data_source, row_to_delete.country_id))
            # Hard deletion
            row_to_delete.delete(force=True)


def delete_duplicate_qos_model_records():
    rows_with_more_than_1_records = QoSData.objects.all().values(
        'school_id', 'timestamp').annotate(
        total_records=Count('school_id'),
    ).order_by('-total_records', 'school_id', 'timestamp').filter(total_records__gt=1)
    logger.info('Queryset to get records to delete where more than 1 record has same school and timestamp in '
                'QoS Data table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        for row_to_delete in QoSData.objects.filter(
            school_id=row['school_id'],
            timestamp=row['timestamp'],
        ).order_by('-version')[1:]:
            logger.info('Deletion for: Id - {0}, Timestamp - {1}, School Id - {2}'.format(
                row_to_delete.id, row_to_delete.timestamp, row_to_delete.school_id))
            # Hard deletion
            row_to_delete.delete()


def delete_duplicate_country_weekly_records():
    rows_with_more_than_1_records = statistics_models.CountryWeeklyStatus.objects.all().values(
        'country_id', 'week', 'year').annotate(
        total_records=Count('country_id', distinct=False),
    ).order_by('-total_records', 'country_id', 'week', 'year').filter(total_records__gt=1)[:10000]
    logger.info('Queryset to get max 10K records to delete where more than 1 record has same Year, Week and '
                'Country ID in Country Weekly table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        country_id = row['country_id']
        last_weekly_id = None

        country = Country.objects.filter(id=country_id).first()
        if country:
            last_weekly_id = country.last_weekly_status_id

        country_weekly_ids_to_delete = (statistics_models.CountryWeeklyStatus.objects.filter(
            country_id=row['country_id'],
            week=row['week'],
            year=row['year'],
        ).values_list('id', flat=True).order_by('id'))

        if last_weekly_id in country_weekly_ids_to_delete:
            logger.info('Country Last Weekly Status id ({0}) is IN the deletion list. '
                        'Hence skipping the current record and deleting all remaining.'.format(last_weekly_id))
            for row_to_delete in statistics_models.CountryWeeklyStatus.objects.filter(
                id__in=country_weekly_ids_to_delete,
            ).exclude(id=last_weekly_id).order_by('-id'):
                logger.info('Deletion for: Id - {0}, Year - {1}, Week - {2}, Country Id - {3}'.format(
                    row_to_delete.id, row_to_delete.year, row_to_delete.week, row_to_delete.country_id))
                # Hard deletion
                row_to_delete.delete(force=True)
        else:
            logger.info('Country Last Weekly Status id ({0}) is NOT IN the deletion list. Hence skipping first '
                        'record and deleting all remaining based on ID DESC.'.format(last_weekly_id))
            for row_to_delete in statistics_models.CountryWeeklyStatus.objects.filter(
                country_id=row['country_id'],
                week=row['week'],
                year=row['year'],
            ).order_by('-id')[1:]:
                logger.info('Deletion for: Id - {0}, Year - {1}, Week - {2}, Country Id - {3}'.format(
                    row_to_delete.id, row_to_delete.year, row_to_delete.week, row_to_delete.country_id))
                # Hard deletion
                row_to_delete.delete(force=True)


def delete_duplicate_school_records():
    rows_with_more_than_1_records = School.objects.all().values('country_id', 'giga_id_school').annotate(
        total_records=Count('id', distinct=False),
    ).order_by(
        '-total_records', 'country_id', 'giga_id_school',
    ).filter(
        total_records__gt=1,
    ).exclude(
        giga_id_school='',
    )
    logger.info('Queryset to get records to delete where more than 1 record has same Giga ID and '
                'Country ID in School table: {0}'.format(rows_with_more_than_1_records.query))

    for row in rows_with_more_than_1_records:
        for row_to_delete in School.objects.filter(
            country_id=row['country_id'],
            giga_id_school=row['giga_id_school'],
        ).order_by('-id')[1:]:
            logger.info('Deletion for: Id - {0}, Country ID - {1}, Giga ID - {2}'.format(
                row_to_delete.id, row_to_delete.country_id, row_to_delete.giga_id_school))
            # Hard deletion may fail
            row_to_delete.delete()


def update_school_giga_ids():
    records = """giga_id_school,external_id,country_id
ea2dd471-45b4-3d5e-b901-412653a6639c,32083530,144
bfd9a0c6-537d-3c16-bd49-1efcdd7fb656,32083564,144
fd605a90-b81c-305f-b3fe-ac28d585acd5,15176231,144
5dda54b9-609d-367c-9524-60fd0c6741d6,51070898,144
808a2c06-3cc5-3294-864f-e1b9636d85a0,27054098,144
5b91575f-14fd-33c2-9c67-d33d3c4bc735,27054209,144
95e63450-ced1-3b61-b889-9f961b0e0805,23278358,144
2fed0546-3b2c-3bed-b0cd-38bcc7cff4b6,23278439,144
e0c99dd1-5b5b-37d0-bae1-ed726e340bab,31377538,144
9c35a002-5625-35cd-98c6-6530d4891000,31380385,144
2e299e5e-fc2d-353c-bf4b-d57ce3df2604,31381195,144
33c04291-0990-3c89-a92b-ef2975406bcd,41164504,144
3223e639-05bf-31a3-a1f3-3641070c0b35,35008079,144
a66bdac1-10fd-34eb-84ec-76f695a4f26d,42159938,144
009d937d-1ad1-392d-aff9-89dd5ecf6b60,42161436,144
8bbb7964-92dd-3aa2-9cd5-b523663c4d02,43213480,144
4d181c74-f60c-36c2-95f7-64aad7628a26,51070839,144
99a20076-23b9-3c8d-95f5-63aa6fc2dd47,42160855,144
595cfbcf-a83a-343e-add2-8109bed3f77d,50035240,144
cdc15aaf-0681-393c-88e4-c7d0ca87cacf,31377457,144
3ea03855-fc29-3f3f-831c-697435961dbe,31377562,144
bebd0156-ea0d-3ab8-9c10-76da995ca61c,31377678,144
b8c68436-3584-3fdc-a677-ba694a840dbd,31378437,144
5d6afd1b-467c-3ef3-8c6f-22b33bfd43df,31379301,144
7f85e78e-1b97-377d-b66c-d8d0c1356223,31379646,144
87b4aab0-dc79-3ba1-a83d-15d5fe934d1d,32083181,144
773dadef-59ad-3547-bcfd-43e785601364,33191719,144
cce341c3-c944-3571-b8d2-0428d2065473,33191735,144
7d64e9ea-9d9f-3310-ba91-ff565957bea4,35007292,144
15fb0256-2e24-3c68-ac50-726298c7a16c,35008835,144
c53add9e-b3d0-3be0-b76b-a2126fbd3e1b,35008882,144
22e992f5-37f9-3972-ba8a-47b58a2cae0f,41164032,144
1666ec39-0447-39fa-a751-cddfe93b77ee,42160553,144
e870ab20-9b29-3f78-b297-1e128ff5bd59,42160669,144
691a7c8e-342d-3b7a-889c-ddc329934fe7,51070308,144
ce2bfa28-b9ae-3fc7-beb4-5aefeb84033d,51071045,144
41e8c7c6-f9cf-3a7a-a85f-04f8e26650b3,17056659,144
9e8c5f90-8d5d-3fcb-aa2f-b3575848e83a,31381209,144
2810107c-8a02-3c7d-a104-3eefc0a85df0,33191743,144
4fe9afd3-abbc-37bf-92ee-ecf7a9cbb746,17057108,144
795615c9-4656-31a9-8aa8-1151c8748d5e,35008760,144
c7fbe102-1a4c-37cc-9cfc-d1b3ef1f59f9,42161070,144
744a1bbf-12cc-34ee-99e0-501a18806312,42161371,144
0acfcb6e-31b7-3141-9282-88b74d172a16,32083289,144
18a53090-6b0a-3acd-84d2-8c6a42738b87,35006504,144
df73348c-00c8-3163-9dd6-5c7d1f7590ec,42160391,144
ab578152-eabb-3d77-921b-db71aa8c18ae,31379484,144
142cc705-9a7a-3fe4-ab99-b275562728c2,51070537,144
a8dd377d-362e-3221-8db0-f3bf6d30c7cd,51070545,144
f94d021c-038b-3c22-adf3-f0e46963efdf,52106616,144
ea78e35c-5c41-3bf3-aea4-600104a8d066,42160456,144
286157e0-8216-3040-acf1-d406d131aeb6,51070669,144
580830e3-d6cd-3466-96fd-f4fbea74c339,51070782,144
5d75f63d-3556-354b-8425-91c95eb66895,35007127,144
691e3ca8-2649-35cb-bfed-3e69349ef6ee,35007208,144
4568a0bf-858c-331e-b383-ca513488fc91,35007220,144
af508e6e-cc11-3b1d-abad-eb990a8dff41,35007291,144
2656792d-1a07-3d19-8b11-6999416bde94,35008685,144
6cf27227-dca6-3699-976f-bdff1f6f2e72,35008762,144
a606e004-7605-3a29-a1df-d7de2ccb2377,42158982,144
41ef9c4b-8506-3221-bfe1-f805132b122e,51070677,144
9de24961-c30e-328b-b0ae-5e3bd0eb6712,51070774,144
0f0fd6a5-ac9d-3750-a829-2b5965f3aba1,51070804,144
39b054a9-391c-3566-bf41-452ed490201a,51070880,144
ee2a2ca2-3d93-3a4e-92f8-64bd45681a1f,35008684,144
eeee2a5f-de13-3e85-ada0-300b79fb344c,51070340,144
8380c6c9-405e-305f-af96-70fc6d5d0970,51070723,144
c757e69e-1d5d-3efc-9e2f-8651cdea041d,21289506,144
0a80df99-1d14-3f26-9162-0a773f489722,24089630,144
1cca55ec-645c-3fa0-9df0-3e4e008d89ad,27054055,144
09d98143-5999-3525-b1d3-ffd872763d53,35008910,144
9463f2a8-4463-3aab-ae75-bf53dfaa8371,35008067,144
2459d607-cf5d-3770-a74e-8625b3da8649,42161525,144
e0d1d375-dffd-32af-8751-dee7efd3dbcb,42161444,144
e3d38700-a4ce-3fe7-b303-86380da85668,42161452,144
e024c857-5b71-3231-a4e3-784efdfbdd64,43214924,144
3359bbec-4cd7-3e71-810e-438026200811,21289298,144
fc5f2401-447d-320d-bb38-8cd2c2b58521,51070790,144
4ebfa31a-f06f-392f-8487-85bf27a51e2c,51070820,144"""

    records_as_list = records.split('\n')
    headers = [tok.strip() for tok in records_as_list[0].split(',')]
    file_data = []
    for row in records_as_list[1:]:
        row_data = [
            tok.strip()
            for tok in row.split(',')
        ]
        file_data.append(dict(zip(headers, row_data)))
    logger.info(file_data)
    for data in file_data:
        School.objects.filter(
            country_id=data['country_id'],
            external_id=data['external_id'],
        ).update(giga_id_school=data['giga_id_school'])

def populate_default_layer_from_active_layer_list(country_id):
    data_layer_country_qs = DataLayerCountryRelationship.objects.all().filter(
        data_layer__type=DataLayer.LAYER_TYPE_LIVE,
        data_layer__status=DataLayer.LAYER_STATUS_PUBLISHED,
        data_layer__deleted__isnull=True,
    ).order_by('-data_layer__last_modified_at')

    if country_id and data_layer_country_qs.filter(country_id=country_id, is_default=True).exists():
        instance = data_layer_country_qs.filter(country_id=country_id, is_default=True).first()
        logger.error(
            'Default layer already exists for given country: \n\tCountry ID: {0}\n\t'
            'Layer Code: {1}\n\tLayer Name: {2}'.format(instance.country.name, instance.data_layer.code, instance.data_layer.name)
        )
    elif country_id:
        data_layer_country_qs = data_layer_country_qs.filter(country_id=country_id)

    active_country_ids = data_layer_country_qs.values_list('country_id', flat=True).order_by('country_id').distinct('country_id')

    for active_country_id in active_country_ids:
        if data_layer_country_qs.filter(country_id=active_country_id, is_default=True).exists():
            instance = data_layer_country_qs.filter(country_id=active_country_id, is_default=True).first()

            logger.warning(
                'Default layer already exists for given country: \n\tCountry ID: {0}\n\t'
                'Layer Code: {1}\n\tLayer Name: {2}'.format(instance.country.name, instance.data_layer.code, instance.data_layer.name)
            )
        else:
            live_download_layer_instance_entry = data_layer_country_qs.filter(
                country_id=active_country_id,
                data_layer__data_sources__data_source_column__icontains='"name":"connectivity_speed"',
                data_layer__data_sources__deleted__isnull=True,
            ).first()
            if live_download_layer_instance_entry:
                live_download_layer_instance_entry.is_default = True
                live_download_layer_instance_entry.save(update_fields=('is_default',))
            else:
                live_upload_layer_instance_entry = data_layer_country_qs.filter(
                    country_id=active_country_id,
                    data_layer__data_sources__data_source_column__icontains='"name":"connectivity_upload_speed"',
                    data_layer__data_sources__deleted__isnull=True,
                ).first()
                if live_upload_layer_instance_entry:
                    live_upload_layer_instance_entry.is_default = True
                    live_upload_layer_instance_entry.save(update_fields=('is_default',))
                else:
                    live_first_layer_instance_entry = data_layer_country_qs.filter(
                        country_id=active_country_id,
                        data_layer__data_sources__deleted__isnull=True,
                    ).first()
                    live_first_layer_instance_entry.is_default = True
                    live_first_layer_instance_entry.save(update_fields=('is_default',))



def delete_default_download_layer_from_active_layer_list(country_id, layer_id):
    data_layer_country_qs = DataLayerCountryRelationship.objects.all().filter(
        data_layer__created_by__isnull=True,
        data_layer__type=DataLayer.LAYER_TYPE_LIVE,
        data_layer__status=DataLayer.LAYER_STATUS_PUBLISHED,
        data_layer__deleted__isnull=True,
    )

    if layer_id:
        data_layer_country_qs = data_layer_country_qs.filter(data_layer_id=layer_id)

    if country_id:
        data_layer_country_qs = data_layer_country_qs.filter(country_id=country_id)

    logger.info('Queryset to get records to delete from active layer list for default download '
                'layer: {0}'.format(data_layer_country_qs.query))

    for row_to_delete in data_layer_country_qs:
        logger.info('Deletion for: Id - {0}, Country ID - {1}, Layer Id - {2}'.format(
            row_to_delete.id, row_to_delete.country_id, row_to_delete.data_layer_id))
        # Soft deletion
        row_to_delete.delete()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--clean_duplicate_school_gigs_ids', action='store_true', dest='clean_duplicate_school_gigs_ids',
            default=False,
            help='If provided, School duplicate records will be deleted based on "School Giga ID + Country ID".'
        )
        parser.add_argument(
            '--clean_duplicate_school_external_ids', action='store_true', dest='clean_duplicate_school_external_ids',
            default=False,
            help='If provided, School duplicate records will be deleted based on "School External ID + Country ID".'
        )
        parser.add_argument(
            '--clean_duplicate_school_weekly', action='store_true', dest='clean_duplicate_school_weekly',
            default=False,
            help='If provided, School Weekly duplicate records will be deleted based on "School + Year + Week".'
        )

        parser.add_argument(
            '--clean_duplicate_schools', action='store_true', dest='clean_duplicate_schools',
            default=False,
            help='If provided, School duplicate records will be deleted based on "Country + Giga ID".'
        )

        parser.add_argument(
            '--clean_duplicate_school_daily', action='store_true', dest='clean_duplicate_school_daily',
            default=False,
            help='If provided, School Daily duplicate records will be deleted based on '
                 '"School + Date + Live Data Source".'
        )

        parser.add_argument(
            '--clean_duplicate_country_weekly', action='store_true', dest='clean_duplicate_country_weekly',
            default=False,
            help='If provided, Country Weekly duplicate records will be deleted based on "Country + Year + Week".'
        )

        parser.add_argument(
            '--clean_duplicate_country_daily', action='store_true', dest='clean_duplicate_country_daily',
            default=False,
            help='If provided, Country Daily duplicate records will be deleted based on '
                 '"Country + Date + Live Data Source".'
        )

        parser.add_argument(
            '--cleanup_qos_data_rows', action='store_true', dest='cleanup_qos_data_rows',
            default=False,
            help='If provided, run the QoS Model data cleanup task manually in real time.'
        )

        parser.add_argument(
            '--cleanup_school_master_rows', action='store_true', dest='cleanup_school_master_rows',
            default=False,
            help='If provided, run the School Master data cleanup task manually in real time.'
        )

        parser.add_argument(
            '--handle_published_school_master_data_row', action='store_true',
            dest='handle_published_school_master_data_row', default=False,
            help='If provided, run the School Master data publish task manually in real time.'
        )

        parser.add_argument(
            '--handle_deleted_school_master_data_row', action='store_true',
            dest='handle_deleted_school_master_data_row', default=False,
            help='If provided, run the School Master data publish task manually in real time.'
        )

        parser.add_argument(
            '--update_school_giga_ids', action='store_true', dest='update_school_giga_ids',
            default=False,
            help='If provided, School Giga ID will be updated based on "Country + External ID".'
        )

        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Pass the Country ID in case want to control the update.'
        )

        parser.add_argument(
            '-year', dest='year', required=False, type=int,
            help='Pass the Year in case want to control the update.'
        )

        parser.add_argument(
            '-exclude_country_ids', dest='exclude_country_ids', required=False, type=str,
            help='Pass the list of comma separated Country IDs in case want to exclude the countries from the update.'
        )

        parser.add_argument(
            '--handle_published_school_master_data_row_with_scheduler', action='store_true',
            dest='handle_published_school_master_data_row_with_scheduler', default=False,
            help='If provided, run the School Master data publish task through Scheduler in real time.'
        )

        parser.add_argument(
            '--redo_aggregations_with_scheduler', action='store_true',
            dest='redo_aggregations_with_scheduler', default=False,
            help='If provided, run the redo_aggregation utility through Scheduler in real time.'
        )

        parser.add_argument(
            '--populate_school_new_fields_with_scheduler', action='store_true',
            dest='populate_school_new_fields_with_scheduler', default=False,
            help='If provided, run the Populate School New Fields task through Scheduler in real time.'
        )

        parser.add_argument(
            '-start_school_id', dest='start_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )
        parser.add_argument(
            '-end_school_id', dest='end_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )

        parser.add_argument(
            '-week_no', dest='week_no', required=False, type=int,
            help='Pass the School ID in case want to control the update.'
        )

        parser.add_argument(
            '--rebuild_school_index_with_scheduler', action='store_true',
            dest='rebuild_school_index_with_scheduler', default=False,
            help='If provided, run the rebuild_school_index utility through Scheduler in real time.'
        )

        parser.add_argument(
            '--data_loss_recovery_for_pcdc_weekly_with_scheduler', action='store_true',
            dest='data_loss_recovery_for_pcdc_weekly_with_scheduler', default=False,
            help='If provided, run the data_loss_recovery_for_pcdc_weekly utility through Scheduler in real time.'
        )

        parser.add_argument(
            '-start_week_no', dest='start_week_no', type=int,
            required=False,
            help='Start week no from which we need to pull the data and then do aggregation.'
        )

        parser.add_argument(
            '-end_week_no', dest='end_week_no', type=int,
            required=False,
            help='End week no from which we need to pull the data and then do aggregation.'
        )

        parser.add_argument(
            '--pull_data', action='store_true', dest='pull_data', default=False,
            help='Pull the PCDC live data from API for specified date.'
        )

        parser.add_argument(
            '--cleanup_active_download_layer', action='store_true', dest='cleanup_active_download_layer',
            default=False,
            help='If provided, delete the default download layers from active layer list and'
                 ' set the other layer as default.'
        )

        parser.add_argument(
            '-layer_id', dest='layer_id', required=False, type=int,
            help='Pass the Data Layer ID in case want to control the update.'
        )

    def handle(self, **options):
        logger.info('Executing data cleanup utility.\n')
        logger.info('Options: {}\n\n'.format(options))

        country_id = options.get('country_id', None)
        layer_id = options.get('layer_id', None)
        start_school_id = options.get('start_school_id', None)
        end_school_id = options.get('end_school_id', None)
        week_no = options.get('week_no', None)

        if options.get('clean_duplicate_school_gigs_ids'):
            logger.info('Performing school duplicate record cleanup base on giga ID and country ID.')
            delete_duplicate_schools_based_on_giga_id()
            logger.info('Completed school duplicate record cleanup base on giga ID and country ID.\n\n')

        if options.get('clean_duplicate_school_external_ids'):
            logger.info('Performing school duplicate record cleanup base on external ID and country ID.')
            delete_duplicate_schools_based_on_external_id()
            logger.info('Completed school duplicate record cleanup base on external ID and country ID.\n\n')

        if options.get('clean_duplicate_school_weekly'):
            logger.info('Performing school weekly duplicate record cleanup.')
            delete_duplicate_school_weekly_records()
            logger.info('Completed school weekly duplicate record cleanup.\n\n')

        if options.get('clean_duplicate_school_daily'):
            logger.info('Performing school daily duplicate record cleanup.')
            delete_duplicate_school_daily_records()
            logger.info('Completed school daily duplicate record cleanup.\n\n')

        if options.get('clean_duplicate_country_weekly'):
            logger.info('Performing country weekly duplicate record cleanup.')
            delete_duplicate_country_weekly_records()
            logger.info('Completed country weekly duplicate record cleanup.\n\n')

        if options.get('clean_duplicate_country_daily'):
            logger.info('Performing country daily duplicate record cleanup.')
            delete_duplicate_country_daily_records()
            logger.info('Completed country daily duplicate record cleanup.\n\n')

        if options.get('cleanup_qos_data_rows'):
            logger.info('Performing QoS data model duplicate record cleanup.')
            delete_duplicate_qos_model_records()
            logger.info('Completed QoS data model duplicate record cleanup.\n\n')

        if options.get('cleanup_school_master_rows'):
            logger.info('Performing school master data source duplicate record cleanup.')
            sources_tasks.cleanup_school_master_rows()
            logger.info('Completed school master data source duplicate record cleanup.\n\n')

        if options.get('clean_duplicate_schools'):
            logger.info('Performing school duplicate record cleanup.')
            delete_duplicate_school_records()
            logger.info('Completed school duplicate record cleanup.\n\n')

        if options.get('update_school_giga_ids'):
            logger.info('Performing school giga ID update.')
            update_school_giga_ids()
            logger.info('Completed school giga ID update.\n\n')

        if options.get('handle_published_school_master_data_row'):
            logger.info('Performing school master data source publish task handling.')

            if country_id:
                sources_tasks.handle_published_school_master_data_row(country_ids=[country_id, ])
            else:
                new_published_records = SchoolMasterData.objects.filter(
                    status=SchoolMasterData.ROW_STATUS_PUBLISHED, is_read=False,
                ).order_by('-pk')[:1000]

                for row in new_published_records:
                    sources_tasks.handle_published_school_master_data_row(published_row=row)
            logger.info('Completed school master data source publish task handling.\n\n')

        if options.get('handle_published_school_master_data_row_with_scheduler'):
            sources_tasks.handle_published_school_master_data_row.delay(country_ids=[country_id, ])

        if options.get('handle_deleted_school_master_data_row'):
            sources_tasks.handle_deleted_school_master_data_row()

        if options.get('populate_school_new_fields_with_scheduler'):
            populate_school_new_fields_task.delay(start_school_id, end_school_id, country_id)

        if options.get('redo_aggregations_with_scheduler'):
            country_id_vs_year_qs = statistics_models.SchoolDailyStatus.objects.filter(
                school__deleted__isnull=True,
            ).annotate(
                country_id=F('school__country_id'),
                year=Extract('date', 'year'),
            ).values_list(
                'country_id', 'year').order_by('country_id', 'year').distinct('country_id', 'year')

            if country_id:
                country_id_vs_year_qs = country_id_vs_year_qs.filter(country_id=country_id)

            if options.get('exclude_country_ids', None):
                exclude_country_ids = [int(c) for c in options.get('exclude_country_ids').split(',')]
                if len(exclude_country_ids) > 0:
                    country_id_vs_year_qs = country_id_vs_year_qs.exclude(country_id__in=exclude_country_ids)

            if options.get('year', None):
                country_id_vs_year_qs = country_id_vs_year_qs.filter(year=options.get('year'))

            country_id_vs_year_qs = country_id_vs_year_qs.filter(year__lte=get_current_year(), )
            logger.info('Query to select country and year for scheduling: {}\n\n'.format(country_id_vs_year_qs.query))
            for country_year in country_id_vs_year_qs:
                # redo_aggregations_task(country_year[0], country_year[1], None)
                redo_aggregations_task.delay(country_year[0], country_year[1], week_no)

        if options.get('data_loss_recovery_for_pcdc_weekly_with_scheduler'):
            start_week_no = options.get('start_week_no', None)
            end_week_no = options.get('end_week_no', None)
            year = options.get('year', None)
            pull_data = options.get('pull_data', False)

            sources_tasks.data_loss_recovery_for_pcdc_weekly_task.delay(start_week_no, end_week_no, year, pull_data)

        if options.get('rebuild_school_index_with_scheduler'):
            rebuild_school_index.delay()

        if options.get('cleanup_active_download_layer'):
            delete_default_download_layer_from_active_layer_list(country_id, layer_id)
            populate_default_layer_from_active_layer_list(country_id)

        logger.info('Completed data cleanup successfully.\n')
