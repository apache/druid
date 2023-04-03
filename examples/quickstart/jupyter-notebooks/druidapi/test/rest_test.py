# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from requests import Response
from requests.exceptions import HTTPError
from .setup import setup, ROUTER_ENDPOINT
setup()

from druidapi.rest import DruidRestClient, build_url, check_error
from druidapi.error import ClientError

class TestRestClient(unittest.TestCase):

    def setUp(self) -> None:
        self.rest_client = DruidRestClient(ROUTER_ENDPOINT)
        return super().setUp()
    
    def tearDown(self) -> None:
        self.rest_client.close()
        return super().tearDown()

    def test_build_url(self):
        base = 'http://foo.org'
        self.assertEqual(base + '/bar', build_url(base, '/bar'))
        self.assertEqual(base + '/bar/mumble', build_url(base, '/bar/{}', args=['mumble']))

        self.assertEqual(
            ROUTER_ENDPOINT + '/bar/mumble', 
            self.rest_client.build_url('/bar/{}', args=['mumble']))

    def test_check_error(self):

        # OK response
        r = Response()
        r.status_code = 200
        self.assertIsNone(check_error(r))

        # Accepted response
        r = Response()
        r.status_code = 202
        self.assertIsNone(check_error(r))

        # Actual HTTP error code
        r = Response()
        r.status_code = 404
        r.reason = 'Not Found'
        with self.assertRaises(HTTPError) as cm:
            check_error(r)

        # Payload is not json
        r = Response()
        r.status_code = 400
        r.reason = 'Oops'
        r._content = 'Something went wrong'
        with self.assertRaises(HTTPError) as cm:
            check_error(r)

        # Payload is unusual JSON
        r = Response()
        r.status_code = 400
        r.reason = 'Oops'
        r._content = bytes('{"foo": "bar"}', 'utf-8')
        r.encoding = 'utf-8'
        self.assertEqual({'foo': 'bar'}, r.json())
        with self.assertRaises(HTTPError) as cm:
            check_error(r)
        self.assertEqual({'foo': 'bar'}, cm.exception.json)

        # Payload with truncated error response
        r = Response()
        r.status_code = 400
        r.reason = 'Oops'
        r._content = bytes('{"error": "Oops"}', 'utf-8')
        r.encoding = 'utf-8'
        with self.assertRaises(ClientError) as cm:
            check_error(r)
        self.assertEqual('Oops', str(cm.exception))

        # Payload with full error response
        r = Response()
        r.status_code = 400
        r.reason = 'Oops'
        r._content = bytes('{"error": "Oops", "errorMessage": "Explanation"}', 'utf-8')
        r.encoding = 'utf-8'
        with self.assertRaises(ClientError) as cm:
            check_error(r)
        self.assertEqual('Explanation', str(cm.exception))

    # Sanity test of HTTP get with a JSON payload
    def test_get(self):
        r = self.rest_client.get('/status')
        json = r.json()
        self.assertIsNotNone(json)
        self.assertIs(dict, type(json))
        self.assertIsNotNone(json.get('version'))

        json = self.rest_client.get_json('/status')
        self.assertIsNotNone(json)
        self.assertIs(dict, type(json))
        self.assertIsNotNone(json.get('version'))

        json = self.rest_client.get_json('/{}', args=['status'])
        self.assertIsNotNone(json)
        self.assertIs(dict, type(json))
        self.assertIsNotNone(json.get('version'))

        # Check parameters
        # Return format: ['broker:8082', 'historical:8083']
        json = self.rest_client.get_json('/druid/coordinator/v1/servers')
        self.assertIsNotNone(json)
        self.assertNotEqual(0, len(json))
        self.assertIs(str, type(json[0]))

        # Return format: [dict, dict]
        # Not sure why this longer format is "simple"...
        json = self.rest_client.get_json('/druid/coordinator/v1/servers', params={'simple': True})
        self.assertIsNotNone(json)
        self.assertNotEqual(0, len(json))
        self.assertIs(dict, type(json[0]))

    def test_post_only_json(self):
        sql = 'SELECT * FROM INFORMATION_SCHEMA.SCHEMATA'
        req = {'query': sql}
        r = self.rest_client.post_only_json('/druid/v2/sql', req)
        self.assertIs(Response, type(r))
        json = r.json()
        self.assertIs(list, type(json))

    def test_post_json(self):
        sql = 'SELECT * FROM INFORMATION_SCHEMA.SCHEMATA'
        req = {'query': sql}
        json = self.rest_client.post_json('/druid/v2/sql', req)
        self.assertIs(list, type(json))
