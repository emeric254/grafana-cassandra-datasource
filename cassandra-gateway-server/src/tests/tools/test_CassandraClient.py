import unittest

from tools.CassandraClient import Client as CassandraClient


class TestCassandraClient(unittest.TestCase):

    def test_stop_closed_client(self):
        CassandraClient.close_connection()
