# -*- coding: utf-8 -*-

import logging
from cassandra import DriverException, Unauthorized
from tornado.escape import json_encode
from handlers.BaseQueryHandler import BaseQueryHandler
from tools.CassandraClient import Client

logger = logging.getLogger(__name__)


class AnnotationsHandler(BaseQueryHandler):

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
