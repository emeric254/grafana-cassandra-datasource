import unittest

from handlers.AnnotationsHandler import AnnotationsHandler

from collections import namedtuple

cassandraRow = namedtuple('row', ['timestamp', 'title', 'tags', 'text'])
ANNOTATION_NAME = 'test annotation'


class TestAnnotationsHandler(unittest.TestCase):

    rows = [
        cassandraRow(timestamp=1234567890, title='line 1', tags='some tags', text='here is some text'),
        cassandraRow(timestamp=1122334455, title='line 2', tags='', text='lorem ipsum'),
        cassandraRow(timestamp=6677889900, title='line 3', tags='some other tags', text='')
    ]

    def test_parse_results(self):
        results = AnnotationsHandler._parse_results(ANNOTATION_NAME, self.rows)
        self.assertEqual(len(self.rows), len(results), msg='It should be as much parsed results as source ones')

        verify_name = all(line['annotation'] == ANNOTATION_NAME for line in results)
        self.assertTrue(verify_name, msg='All annotation entries should have the same given name')

        for i in range(len(self.rows)):
            raw_line = self.rows[i]
            parsed_line = results[i]

            self.assertEqual(
                raw_line.timestamp,
                parsed_line['time'] * 1000,
                msg='Timestamp must be reduce to microsecond'
            )
            self.assertEqual(raw_line.title, parsed_line['title'], msg='Title must be the same')
            self.assertEqual(raw_line.tags, parsed_line['tags'], msg='Title must be the same')
            self.assertEqual(raw_line.text, parsed_line['text'], msg='Title must be the same')
