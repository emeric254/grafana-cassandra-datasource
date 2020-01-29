# -*- coding: utf-8 -*-

import logging
from datetime import datetime
from cassandra import DriverException, Unauthorized
from tornado.escape import json_decode, json_encode
from handlers.BaseHandler import BaseHandler
from tools.CassandraClient import Client

logger = logging.getLogger(__name__)


class AnnotationsHandler(BaseHandler):

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

    @staticmethod
    def _parse_results(name: str, rows):
        results = []

        for row in rows:
            timestamp = row.timestamp
            title = row.title
            tags = row.tags
            text = row.text

            results.append(
                {
                    'annotation': name,
                    'time': timestamp / 1000,  # convert our timestamp in microsecond into a millisecond one
                    'title': title,
                    'tags': tags,
                    'text': text
                }
            )

        return results

    def post(self):

        annotation = self.args.get('annotation')

        if not annotation:
            self.set_status(400)
            return

        name = annotation.get('name')
        request = annotation.get('query')

        if not request:
            self.set_status(400)
            return

        if not self._extract_time_range():
            self.set_status(400)
            return

        try:
            cassandra_client = Client.get_client()
        except DriverException as e:
            logger.error(f'The query got refused because of a cassandra driver error: {e}')
            self.set_status(503)
            return

        request = request.replace('$startTime', self.start_time)
        request = request.replace('$endTime', self.end_time)

        logger.debug(f'Executing: {request}')

        try:
            rows = cassandra_client.execute(request)
            results = self._parse_results(name, rows)
        except Unauthorized as e:
            logger.warning(f'The query got refused because of authorization reasons: {e}')
            self.set_status(403)
            return

        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(results))
