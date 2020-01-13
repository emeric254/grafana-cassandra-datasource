# -*- coding: utf-8 -*-

from tornado import testing


class HealthTestCase(testing.AsyncTestCase):
    @testing.gen_test
    def test_http_fetch(self):
        client = testing.AsyncHTTPClient()
        response = yield client.fetch("http://localhost/health")
        # Test contents of response
        self.assertIn('ok', response.body)
