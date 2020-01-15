# -*- coding: utf-8 -*-

from tornado import testing


class HealthTestCase(testing.AsyncTestCase):

    @testing.gen_test
    def test_http_fetch(self):
        """
        Test if the /health endpoint return 'ok'
        """
        client = testing.AsyncHTTPClient()

        response = yield client.fetch("http://localhost/health")

        self.assertEqual(200, response.status_code, msg='/health should return 200 status code')
