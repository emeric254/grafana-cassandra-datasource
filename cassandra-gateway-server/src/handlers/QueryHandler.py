# -*- coding: utf-8 -*-

import logging
from datetime import datetime
from cassandra import DriverException, Unauthorized
from tornado.escape import json_decode, json_encode
from handlers.BaseHandler import BaseHandler
from tools.CassandraClient import Client

logger = logging.getLogger(__name__)


class QueryHandler(BaseHandler):

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
    def _parse_results(rows):
        results = []

        entries = {}

        for row in rows:
            timestamp = row.timestamp
            name = row.name

            if row.value is None:
                continue

            try:
                value = float(row.value)
            except ValueError or TypeError:
                value = str(row.value)

            if name not in entries:
                entries[name] = {
                    'target': row.name,
                    'datapoints': [
                        # an entry is: [ value (float), timestamp in milliseconds (int)]
                    ]
                }

            entries[name]['datapoints'].append([
                value,
                timestamp / 1000  # convert our timestamp in microsecond into a millisecond one
            ])

        for entry in entries.values():
            results.append(entry)

        return results

    def post(self):

        targets = self.args.get('targets')

        if not targets:
            self.set_status(400)
            return

        if not self._extract_time_range():
            self.set_status(400)
            return

        results = []

        try:
            cassandra_client = Client.get_client()
        except DriverException:
            self.set_status(503)
            return

        for target in targets:
            request = target.get('target')

            request = request.replace('$startTime', self.start_time)
            request = request.replace('$endTime', self.end_time)

            if not request:
                continue

            try:
                rows = cassandra_client.execute(request)
                results = self._parse_results(rows)
            except Unauthorized:
                self.set_status(403)
                return

        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(results))
