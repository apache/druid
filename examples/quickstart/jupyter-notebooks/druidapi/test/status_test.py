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
from .setup import setup, ROUTER_ENDPOINT
setup()

import druidapi

class TestStatusClient(unittest.TestCase):

    def setUp(self) -> None:
        self.druid = druidapi.client(ROUTER_ENDPOINT)
        self.status = self.druid.status

        # Wait for the server to come up so tests are repeatable
        self.status.wait_until_ready()
        return super().setUp()
    
    def tearDown(self) -> None:
        self.druid.close()
        return super().tearDown()
    
    def test_is_healthy(self):
        self.assertTrue(self.status.is_healthy)

        # The following should immediately return
        self.status.wait_until_ready()

    def test_in_cluster(self):
        self.assertTrue(self.status.in_cluster)

    def test_status(self):
        json = self.status.status
        self.assertIsNotNone(json)
        self.assertIs(dict, type(json))
        self.assertIsNotNone(json.get('version'))

    def test_version(self):
        v = self.status.version
        self.assertGreater(len(v), 2)

    def test_properties(self):
        json = self.status.properties
        self.assertIs(dict, type(json))
        self.assertIsNotNone(json.get('druid.extensions.loadList'))

    def test_brokers(self):
        json = self.status.brokers
        self.assertIs(dict, type(json))
        self.assertNotEqual(0, len(json))
