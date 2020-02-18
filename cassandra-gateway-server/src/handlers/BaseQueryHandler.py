# -*- coding: utf-8 -*-

import logging
from datetime import datetime
from tornado.escape import json_decode
from handlers.BaseHandler import BaseHandler

logger = logging.getLogger(__name__)


class BaseQueryHandler(BaseHandler):

    def data_received(self, _):
        # we don't care about streamed data
        pass

    def prepare(self):
        self.args = {}
        if self.request.headers['Content-Type'] in (
            'application/x-json',
            'application/json'
        ):
            self.args = json_decode(self.request.body)

    def _extract_time_range(self):
        time_range = self.args.get('range')

        if not time_range:
            return False

        raw_from = time_range.get('from')
        raw_to = time_range.get('to')

        if not raw_from or not raw_to:
            return False

        from_datetime = datetime.fromisoformat(raw_from.replace('Z', ''))
        to_datetime = datetime.fromisoformat(raw_to.replace('Z', ''))

        self.start_time = str(int(from_datetime.timestamp() * 1e6))  # timestamp in microseconds
        self.end_time = str(int(to_datetime.timestamp() * 1e6))  # timestamp in microseconds

        return True
