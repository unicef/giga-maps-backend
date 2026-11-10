import json
import logging
import os
import tempfile

from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Count, Max

import delta_sharing

from proco.data_sources.models import QoSData

logger = logging.getLogger('gigamaps.' + __name__)


def parse_version_overrides(values):
    """
    parse_version_overrides
        Method to parse the --set_version values given as ISO3=VERSION.

    :param values: list of strings like 'BRA=700'
    :return: dict of ISO3 code to version number
    """
    overrides = {}
    for value in values or []:
        iso3, sep, version = value.partition('=')
        iso3 = iso3.strip().upper()
        if not sep or not iso3 or not version.strip().isdigit():
            raise CommandError('Invalid --set_version value "{0}". Expected ISO3=VERSION, e.g. BRA=700.'.format(value))
        overrides[iso3] = int(version.strip())
    return overrides


class Command(BaseCommand):
    """
    Command
        Sets the QoS data version that Maps stores for each country, so that the next QoS load continues from
        the given version of the Delta Sharing host that is configured now (DATA_SOURCE_CONFIG['QOS']).
        Use it right after QOS_ENDPOINT points to another host: table versions are per host, so the version
        stored for the old host is meaningless for the new one.
        Only the version column of QoSData and the qos_data_last_version_<ISO3> cache keys change.
        No measurement, RealTimeConnectivity or aggregated row is created or deleted.
        Dry run unless --apply is given.

    Inherits: `BaseCommand`
    """
    help = 'Set the stored QoS data version per country to the version of the configured Delta Sharing host.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--country_code', action='append', dest='country_codes', default=[],
            help='ISO3 code of a country to seed. Repeat for several. Default: every country with QoSData rows.',
        )
        parser.add_argument(
            '--margin', type=int, dest='margin', default=2,
            help='Seed with the current host version minus this margin, so the next load pulls these last '
                 'versions again. Re-pulling is safe: QoSData upserts by (school, timestamp) and realtime rows '
                 'are added only after the last aggregated measurement. Default: 2.',
        )
        parser.add_argument(
            '--set_version', action='append', dest='set_versions', default=[],
            help='Explicit version for a country as ISO3=VERSION, e.g. BRA=700. Skips the host lookup for it.',
        )
        parser.add_argument(
            '--apply', action='store_true', dest='apply', default=False,
            help='Write the versions. Without it the command only prints what it would change.',
        )

    def get_remote_versions(self, iso3_codes):
        """
        get_remote_versions
            Method to read the current table version of each country from the configured Delta Sharing host.

        :param iso3_codes: list of ISO3 codes
        :return: dict of ISO3 code to version number, or to an error message when the lookup failed
        """
        qos_ds_settings = settings.DATA_SOURCE_CONFIG.get('QOS')
        profile_json = {
            'shareCredentialsVersion': qos_ds_settings.get('SHARE_CREDENTIALS_VERSION', 1),
            'endpoint': qos_ds_settings.get('ENDPOINT'),
            'bearerToken': qos_ds_settings.get('BEARER_TOKEN'),
            'expirationTime': qos_ds_settings.get('EXPIRATION_TIME'),
        }

        fd, profile_file = tempfile.mkstemp(suffix='.share')
        versions = {}
        try:
            with os.fdopen(fd, 'w') as profile:
                profile.write(json.dumps(profile_json))

            for iso3 in iso3_codes:
                table_url = profile_file + '#{0}.{1}.{2}'.format(
                    qos_ds_settings['SHARE_NAME'], qos_ds_settings['SCHEMA_NAME'], iso3,
                )
                try:
                    versions[iso3] = delta_sharing.get_table_version(table_url)
                except (OSError, ValueError, KeyError) as ex:
                    versions[iso3] = 'lookup failed: {0}'.format(type(ex).__name__)
        finally:
            os.remove(profile_file)
        return versions

    def handle(self, **options):
        margin = options['margin']
        if margin < 0:
            raise CommandError('--margin must be 0 or greater.')

        overrides = parse_version_overrides(options['set_versions'])

        stored = {
            row['country__iso3_format']: row
            for row in QoSData.objects.all().values('country__iso3_format').annotate(
                db_version=Max('version'), rows=Count('id'),
            )
        }

        iso3_codes = [code.strip().upper() for code in options['country_codes']] or sorted(stored.keys())
        iso3_codes = sorted(set(iso3_codes) | set(overrides.keys()))
        if not iso3_codes:
            self.stdout.write('No country has QoSData rows and none was given. Nothing to do.')
            return

        endpoint = settings.DATA_SOURCE_CONFIG.get('QOS', {}).get('ENDPOINT')
        self.stdout.write('Configured QoS endpoint: {0}'.format(endpoint))
        self.stdout.write('Mode: {0}'.format('APPLY' if options['apply'] else 'DRY RUN'))

        remote_versions = self.get_remote_versions([code for code in iso3_codes if code not in overrides])

        plan = {}
        header = '{0:<6} {1:>8} {2:>12} {3:>12} {4:>14} {5:>10}'.format(
            'ISO3', 'rows', 'db_version', 'cache', 'host_version', 'target')
        self.stdout.write(header)
        for iso3 in iso3_codes:
            row = stored.get(iso3, {})
            cached = cache.get(QoSData.DATA_VERSION_CACHE_KEY.format(iso3))

            if iso3 in overrides:
                host_version = 'set'
                target = overrides[iso3]
            else:
                host_version = remote_versions.get(iso3)
                target = max(0, host_version - margin) if isinstance(host_version, int) else None

            self.stdout.write('{0:<6} {1:>8} {2:>12} {3:>12} {4:>14} {5:>10}'.format(
                iso3, row.get('rows', 0), str(row.get('db_version')), str(cached), str(host_version), str(target),
            ))

            if target is None:
                self.stderr.write('  {0}: no target version, skipped. Pass --set_version {0}=N to seed it.'.format(
                    iso3))
            elif not row.get('rows'):
                self.stdout.write('  {0}: no QoSData rows, nothing to seed. The next load pulls the last 10 '
                                  'versions of the host.'.format(iso3))
            else:
                plan[iso3] = target

        if not options['apply']:
            self.stdout.write('Dry run: nothing written. Re-run with --apply to set the target versions.')
            return

        with transaction.atomic():
            for iso3, target in plan.items():
                updated = QoSData.objects.all().filter(country__iso3_format=iso3).update(version=target)
                logger.info('Seeded QoS data version for %s: %s rows set to version %s', iso3, updated, target)

        for iso3, target in plan.items():
            cache.delete(QoSData.DATA_VERSION_CACHE_KEY.format(iso3))
            current = QoSData.get_last_version(iso3)
            status = 'OK' if current == target else 'MISMATCH'
            self.stdout.write('{0}: version now {1} ({2})'.format(iso3, current, status))
