# -*- coding: utf-8 -*-

import logging
from handlers.BaseHandler import BaseHandler

logger = logging.getLogger(__name__)

version_file = './VERSION'

current_version = 'N/A'
try:
    with open(version_file, mode='r', encoding='UTF-8') as file:
        current_version = file.read().strip()
except FileNotFoundError:
    # no VERSION file
    pass
logger.debug('This server is running version: ' + current_version)


class VersionHandler(BaseHandler):

    def data_received(self, _):
        # we don't care about streamed data
        pass

    def get(self):
        self.write({'version': str(current_version)})
