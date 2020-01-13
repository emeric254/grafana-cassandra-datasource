# -*- coding: utf-8 -*-

import os
import logging
import configparser
from tornado import web

logger = logging.getLogger(__name__)

# load API settings
config = configparser.ConfigParser()
config.read(os.getenv('SETTINGS_FILE', './settings.ini'))

if 'API' not in config:
    raise ValueError('No [API] section inside the settings file')
settings = config['API']
allow_CORS = settings.getboolean('ALLOW_CORS', fallback=False)
CORS_domain = settings.get('CORS_DOMAIN', fallback='*')


class BaseHandler(web.RequestHandler):

    def data_received(self, chunk):
        """
        Override.

        :param chunk:
        :return:
        """
        raise NotImplementedError

    def set_default_headers(self):  # to allow CORS
        if allow_CORS:
            self.set_header('Access-Control-Allow-Origin', CORS_domain)
            self.set_header('Access-Control-Allow-Headers', 'x-requested-with')
            self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def options(self):  # to allow CORS
        if allow_CORS:
            # no body, just a status code wanted
            self.set_status(204)
            self.finish()
