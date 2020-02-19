import unittest

from handlers.QueryHandler import QueryHandler

from collections import namedtuple

cassandraRow = namedtuple('row', ['timestamp', 'name', 'value'])
ANNOTATION_NAME = 'test annotation'


class TestAnnotationsHandler(unittest.TestCase):

    rows = [
        cassandraRow(timestamp=9876543210, name='oxygen', value=0.001),
        cassandraRow(timestamp=1256903478, name='laser', value=123),
        cassandraRow(timestamp=1000000000, name='door', value=True),
        cassandraRow(timestamp=2000000000, name='file name', value='text'),
        cassandraRow(timestamp=6000000000, name='oxygen', value=0.002),
        cassandraRow(timestamp=7000000000, name='laser', value=0),
        cassandraRow(timestamp=9000000000, name='door', value=False),
    ]

    def test_parse_results_as_timeserie(self):
        results = QueryHandler._parse_results_as_timeserie(self.rows)

        names = set(line.name for line in self.rows)
        self.assertEqual(len(names), len(results), msg='Parsed results have one entry for every different name')

        for line in results:
            self.assertTrue(line['target'] in names, msg='All entries have a target from a raw result name')

            self.assertTrue(len(line['datapoints']) > 0, msg='All entries have at least one point')

            row_values = [row for row in self.rows if row.name == line['target']]

            self.assertEqual(len(row_values), len(line['datapoints']), msg='An entry have as much point as row values')

            for i in range(len(row_values)):
                raw_point = row_values[i]
                parsed_point = line['datapoints'][i]
                parsed_value, parsed_timestamp = parsed_point

                self.assertEqual(
                    raw_point.timestamp,
                    parsed_timestamp * 1000,
                    msg='Timestamp must be reduce to microsecond'
                )

                if type(parsed_value) == str:
                    self.assertEqual(str(raw_point.value), parsed_value, msg='Parsed point equals str(raw value)')
                elif type(parsed_value) == float:
                    self.assertEqual(float(raw_point.value), parsed_value, msg='Parsed point equals float(raw value)')
                else:
                    self.fail('Parsed points type must be float or str')

    def test_parse_results_as_table(self):
        results = QueryHandler._parse_results_as_table(self.rows)

        expected_result = [{
            'columns': [
                {
                    'text': 'timestamp',
                    'type': 'string'
                },
                {
                    'text': 'name',
                    'type': 'string'
                },
                {
                    'text': 'value',
                    'type': 'string'
                }
            ],
            'rows': [
                [9876543.210, 'oxygen', 0.001],
                [1256903.478, 'laser', 123],
                [1000000.000, 'door', True],
                [2000000.000, 'file name', 'text'],
                [6000000.000, 'oxygen', 0.002],
                [7000000.000, 'laser', 0],
                [9000000.000, 'door', False]
            ],
            'type': 'table'
        }]

        self.assertEqual(expected_result, results, msg='Table entries must respect the Grafana contract')

    def test_compute_aggregation(self):
        some_values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]

        self.assertEqual(
            45,
            QueryHandler._compute_aggregation(some_values, 'sum'),
            msg='Sum of all the values'
        )
        self.assertEqual(
            0,
            QueryHandler._compute_aggregation(some_values, 'minimum'),
            msg='Minimum of all the values'
        )
        self.assertEqual(
            9,
            QueryHandler._compute_aggregation(some_values, 'maximum'),
            msg='Maximum of all the values'
        )
        self.assertEqual(
            False,
            QueryHandler._compute_aggregation(some_values, 'and'),
            msg='Boolean and of all the values'
        )
        self.assertEqual(
            True,
            QueryHandler._compute_aggregation(some_values, 'or'),
            msg='Boolean or of all the values'
        )
        self.assertEqual(
            10,
            QueryHandler._compute_aggregation(some_values, 'count'),
            msg='Count of all the values'
        )

        self.assertEqual(
            4.5,
            QueryHandler._compute_aggregation(some_values, 'average'),
            msg='Average of all the values'
        )

        self.assertEqual(
            4.5,
            QueryHandler._compute_aggregation(some_values, 'not existing method'),
            msg='Default is Average'
        )

        empty_values = []

        self.assertEqual(0, QueryHandler._compute_aggregation(empty_values, 'sum'), msg='Sum on empty array is 0')
        with self.assertRaises(ValueError, msg='Minimum on empty array is a ValueError'):
            QueryHandler._compute_aggregation(empty_values, 'minimum')
        with self.assertRaises(ValueError, msg='Maximum on empty array is a ValueError'):
            QueryHandler._compute_aggregation(empty_values, 'maximum')
        self.assertEqual(
            True,
            QueryHandler._compute_aggregation(empty_values, 'and'),
            msg='Boolean and on empty array is True'
        )
        self.assertEqual(
            False,
            QueryHandler._compute_aggregation(empty_values, 'or'),
            msg='Boolean or on empty array is True'
        )
        self.assertEqual(
            0,
            QueryHandler._compute_aggregation(empty_values, 'count'),
            msg='Count on empty array is 0'
        )
        with self.assertRaises(ZeroDivisionError, msg='Average on empty array is a ZeroDivisionError'):
            QueryHandler._compute_aggregation(empty_values, 'average')

    def test_aggregate_datapoint_changes(self):
        some_values = [
            [1, 1000],
            [1, 1001],
            [1, 1002],
            [2, 1003],
            [2, 1004],
            [2, 1005],
            [3, 1006],
            [3, 1007],
            [4, 1008],
            [1, 1009],
        ]

        unique_values = [
            some_values[0],
            some_values[3],
            some_values[6],
            some_values[8],
            some_values[9]
        ]

        self.assertEqual(
            unique_values,
            QueryHandler._aggregate_datapoint_changes(some_values),
            msg='Keep only entries when the value change'
        )

        empty_values = []

        self.assertEqual(
            empty_values,
            QueryHandler._aggregate_datapoint_changes(empty_values),
            msg='No entries when no values given'
        )
