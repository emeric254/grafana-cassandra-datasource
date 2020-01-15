# -*- coding: utf-8 -*-

import logging
from cassandra import DriverException
from tools.CassandraClient import Client
from handlers.BaseHandler import BaseHandler

logger = logging.getLogger(__name__)


class HealthHandler(BaseHandler):

    def data_received(self, _):
        # we don't care about streamed data
        pass

    async def get(self):
        try:
            Client.get_client()
        except DriverException:
            self.set_status(503)
            return

        self.write({'status': 'ok'})
