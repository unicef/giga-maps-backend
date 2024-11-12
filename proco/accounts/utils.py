import logging
import re

from anymail.message import AnymailMessage
from django.conf import settings
from django.db import connections, utils as django_db_utilities
from django.http import HttpResponse
from django.template.loader import get_template
from rest_framework.response import Response

from proco.accounts.config import app_config as config
from proco.core import utils as core_utilities

logger = logging.getLogger('gigamaps.' + __name__)


def send_standard_email(user, data):
    """
    A standard email is sent to user by Application, containing message, Signature by using MailJet creds.

    :param user: user for which message needs to be done
    :param data: subject/message
    """
    if (
        core_utilities.is_blank_string(settings.ANYMAIL.get('MAILJET_API_KEY')) or
        core_utilities.is_blank_string(settings.ANYMAIL.get('MAILJET_SECRET_KEY'))
    ):
        logger.error('MailJet creds are not configured to send the email. Hence email notification is disabled.')
        return

    data.update({
        'project_title': core_utilities.get_project_title(),
        'user_name': user.first_name + ' ' + user.last_name,
        'support_number': settings.SUPPORT_PHONE_NUMBER,
        'support_email': core_utilities.get_support_email(),
        'footer_copyright': core_utilities.get_footer_copyright(),
    })
    email_body = get_template(config.standard_email_template_name).render(data)

    mail = AnymailMessage(
        data.get('subject'),
        email_body,
        to=[user.email],
    )
    mail.content_subtype = 'html'
    logger.debug('Sending standard message over email')
    mail.send()


def send_email_over_mailjet_service(recipient_list, cc=None, bcc=None, fail_silently=False,
                                    template=config.standard_email_template_name, user_name='User', **kwargs):
    if (
        core_utilities.is_blank_string(settings.ANYMAIL.get('MAILJET_API_KEY')) or
        core_utilities.is_blank_string(settings.ANYMAIL.get('MAILJET_SECRET_KEY'))
    ):
        logger.error('MailJet creds are not configured to send the email. Hence email notification is disabled.')
        return

    kwargs.update({
        'project_title': core_utilities.get_project_title(),
        'user_name': user_name,
        'support_number': settings.SUPPORT_PHONE_NUMBER,
        'support_email': core_utilities.get_support_email(),
        'footer_copyright': core_utilities.get_footer_copyright(),
    })
    email_body = get_template(template).render(kwargs)

    mail = AnymailMessage(
        kwargs.get('subject'),
        email_body,
        to=recipient_list,
        cc=cc,
        bcc=bcc
    )
    mail.content_subtype = 'html'
    logger.debug('Sending message over email')
    response = mail.send(fail_silently=fail_silently)
    return response


class BaseTileGenerator:
    def path_to_tile(self, request):
        path = "/" + request.query_params.get('z') + "/" + request.query_params.get(
            'x') + "/" + request.query_params.get('y')

        if m := re.search(r'^\/(\d+)\/(\d+)\/(\d+)\.(\w+)', path):
            return {'zoom': int(m[1]), 'x': int(m[2]), 'y': int(m[3]), 'format': m[4]}
        return None

    def tile_is_valid(self, tile):
        if 'x' not in tile or 'y' not in tile or 'zoom' not in tile:
            return False
        if 'format' not in tile or tile['format'] not in ['pbf', 'mvt']:
            return False

        size = 2 ** tile['zoom']
        if tile['x'] >= size or tile['y'] >= size:
            return False
        return tile['x'] >= 0 and tile['y'] >= 0

    def tile_to_envelope(self, tile):
        # Width of world in EPSG:3857
        worldMercMax = 20037508.3427892
        worldMercMin = -1 * worldMercMax
        worldMercSize = worldMercMax - worldMercMin
        # Width in tiles
        worldTileSize = 2 ** tile['zoom']
        # Tile width in EPSG:3857
        tileMercSize = worldMercSize / worldTileSize
        # Calculate geographic bounds from tile coordinates
        # XYZ tile coordinates are in "image space" so origin is
        # top-left, not bottom right
        return {
            'xmin': worldMercMin + tileMercSize * tile['x'],
            'xmax': worldMercMin + tileMercSize * (tile['x'] + 1),
            'ymin': worldMercMax - tileMercSize * (tile['y'] + 1),
            'ymax': worldMercMax - tileMercSize * (tile['y']),
        }

    def envelope_to_bounds_sql(self, env):
        DENSIFY_FACTOR = 4
        env['segSize'] = (env['xmax'] - env['xmin']) / DENSIFY_FACTOR
        sql_tmpl = 'ST_Segmentize(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857),{segSize})'
        return sql_tmpl.format(**env)

    def envelope_to_sql(self, env, request):
        raise NotImplementedError("envelope_to_sql must be implemented in the subclass.")

    def sql_to_pbf(self, sql):
        with connections[settings.READ_ONLY_DB_KEY].cursor() as cur:
            try:
                cur.execute(sql)
                if not cur:
                    response = Response({"error": f"sql query failed: {sql}"}, status=404)
                else:
                    response = cur.fetchone()[0]
            except django_db_utilities.OperationalError:
                response = Response({"error": "An error occurred while executing requested query"}, status=500)
        return response

    def generate_tile(self, request):
        tile = self.path_to_tile(request)
        if not (tile and self.tile_is_valid(tile)):
            return Response({"error": "Invalid tile path"}, status=400)

        env = self.tile_to_envelope(tile)

        sql = self.envelope_to_sql(env, request)

        logger.debug(sql.replace('\n', ''))

        pbf = self.sql_to_pbf(sql)
        if isinstance(pbf, memoryview):
            response = HttpResponse(pbf.tobytes(), content_type="application/vnd.mapbox-vector-tile")
            response["Access-Control-Allow-Origin"] = "*"
            return response
        return pbf
