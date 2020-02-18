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
    def _parse_results_as_timeserie(rows):
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

    @staticmethod
    def _parse_results_as_table(rows):
        result = {
            'columns': [],
            'rows': [],
            'type': 'table'
        }

        for row in rows:
            if not result['columns']:
                for column in list(row._fields):
                    result['columns'].append(
                        {
                            'text': column,
                            'type': 'string'
                        }
                    )
            value_list = []
            for column in list(row._fields):
                value = getattr(row, column)
                if column == 'timestamp':
                    value = int(value) / 1000
                value_list.append(value)
            result['rows'].append(value_list)

        return [result]

    @staticmethod
    def _compute_aggregation(points: list, method):
        if method == 'sum':
            return sum(points)
        elif method == 'minimum':
            return min(points)
        elif method == 'maximum':
            return max(points)
        elif method == 'and':
            return all(points)
        elif method == 'or':
            return any(points)
        elif method == 'count':
            return len(points)
        # default is average
        return sum(points) / len(points)

    def _aggregate_datapoint(self, entry_results, method):
        start_interval_timestamp = 0
        end_interval_timestamp = 0
        interval_ms = self.args.get('intervalMs')

        buffer = []

        new_datapoints = []

        for (value, timestamp) in entry_results:

            if not start_interval_timestamp:
                # start with first value timestamp
                start_interval_timestamp = timestamp
                end_interval_timestamp = start_interval_timestamp + interval_ms

            # verify we are not in a new interval
            if timestamp >= end_interval_timestamp:
                # store last interval average
                if buffer:
                    new_datapoints.append([
                        self._compute_aggregation(buffer, method),
                        timestamp
                    ])

                # reset buffers
                buffer = []

                # increment interval start timestamp as much as needed
                while timestamp >= end_interval_timestamp:
                    start_interval_timestamp += interval_ms
                    end_interval_timestamp = start_interval_timestamp + interval_ms

            # aggregate values in the same interval
            try:
                buffer.append(value)
            except ValueError or TypeError:
                # you can't aggregate str
                return entry_results

        return new_datapoints

    @staticmethod
    def _aggregate_datapoint_changes(entry_results):

        last_value = None

        new_datapoints = []

        for (value, timestamp) in entry_results:

            if value == last_value:
                continue

            new_datapoints.append([
                value,
                timestamp
            ])

            last_value = value

        return new_datapoints

    def _aggregate_results(self, all_results, aggregation):
        new_results = []

        for result in all_results:
            target = result['target']
            datapoints = result['datapoints']

            logger.critical(aggregation)
            if aggregation == 'on changes':
                # aggregate by duplicate value
                datapoints = self._aggregate_datapoint_changes(datapoints)
            elif aggregation != 'none':
                # aggregate by time intervals
                datapoints = self._aggregate_datapoint(datapoints, aggregation)

            new_results.append({
                    'target': target,
                    'datapoints': datapoints
            })

        return new_results

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
        except DriverException as e:
            logger.error(f'The query got refused because of a cassandra driver error: {e}')
            self.set_status(503)
            return

        for target in targets:
            request = target.get('target')
            target_type = target.get('type') or 'timeserie'
            aggregation = target.get('aggregation')

            request = request.replace('$startTime', self.start_time)
            request = request.replace('$endTime', self.end_time)

            if not request:
                continue

            logger.debug(f'Executing: {request}')

            try:
                rows = cassandra_client.execute(request)
                if target_type == 'timeserie':
                    tmp_results = self._parse_results_as_timeserie(rows)
                    results.extend(self._aggregate_results(tmp_results, aggregation))
                else:
                    tmp_results = self._parse_results_as_table(rows)
                    results.extend(tmp_results)
            except Unauthorized as e:
                logger.warning(f'The query got refused because of authorization reasons: {e}')
                self.set_status(403)
                return

        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(results))
