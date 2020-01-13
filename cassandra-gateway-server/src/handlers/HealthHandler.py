# -*- coding: utf-8 -*-

import logging
from tools.CassandraClient import Client
from handlers.BaseHandler import BaseHandler

logger = logging.getLogger(__name__)


class HealthHandler(BaseHandler):

    def data_received(self, _):
        # we don't care about streamed data
        pass

    async def get(self):
        try:
            cassandra_client = Client.get_client()
        except:
            self.set_status(503)
            return

        # return ok
        self.write({'ok'})
