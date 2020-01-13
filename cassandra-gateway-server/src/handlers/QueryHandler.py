# -*- coding: utf-8 -*-

import logging
from datetime import datetime
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

    def post(self):

        # TODO extract query from the request data
        targets = self.args.get('targets')
        if not targets:
            self.set_status(400)
            return

        time_range = self.args.get('range')
        if not time_range:
            self.set_status(400)
            return

        raw_from = time_range.get('from')
        raw_to = time_range.get('to')

        if not raw_from or not raw_to:
            self.set_status(400)
            return

        from_datetime = datetime.fromisoformat(raw_from.replace('Z', ''))
        to_datetime = datetime.fromisoformat(raw_to.replace('Z', ''))

        start_time = str(int(from_datetime.timestamp() * 1e6))  # timestamp in microseconds
        end_time = str(int(to_datetime.timestamp() * 1e6))  # timestamp in microseconds
        # interval = str(self.args.get('intervalMs'))

        results = []

        try:
            cassandra_client = Client.get_client()
        except:
            self.set_status(503)
            return

        for target in targets:
            request = target.get('target')

            request = request.replace('$startTime', start_time)
            request = request.replace('$endTime', end_time)
            # request = request.replace('$interval', interval)

            if not request:
                continue

            rows = cassandra_client.execute(request)

            entries = {}

            for row in rows:
                timestamp = row.timestamp
                name = row.name
                try:
                    value = float(row.value)
                except ValueError:
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

        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(results))
