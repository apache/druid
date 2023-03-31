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
from .setup import setup, ROUTER_ENDPOINT, BROKER_ENDPOINT
setup()

import druidapi
from druidapi.rest import DruidRestClient
from druidapi.status import StatusClient
from druidapi.catalog import CatalogClient
from druidapi.sql import QueryClient
from druidapi.tasks import TaskClient
from druidapi.datasource import DatasourceClient

class TestRestClient(unittest.TestCase):

    def setUp(self) -> None:
        self.druid = druidapi.client(ROUTER_ENDPOINT)
        return super().setUp()
    
    def tearDown(self) -> None:
        self.druid.close()
        return super().tearDown()
    
    def test_clients(self):
        self.assertIs(DruidRestClient, type(self.druid.rest))
        self.assertIs(StatusClient, type(self.druid.status))
        self.assertIs(CatalogClient, type(self.druid.catalog))
        self.assertIs(QueryClient, type(self.druid.sql))
        self.assertIs(TaskClient, type(self.druid.tasks))
        self.assertIs(DatasourceClient, type(self.druid.datasources))

        # Verify the same client is retured on each request
        self.assertIs(self.druid.rest, self.druid.rest)
        self.assertIs(self.druid.status, self.druid.status)
        self.assertIs(self.druid.catalog, self.druid.catalog)
        self.assertIs(self.druid.sql, self.druid.sql)
        self.assertIs(self.druid.tasks, self.druid.tasks)
        self.assertIs(self.druid.datasources, self.druid.datasources)

    def test_service_status(self):
        c = self.druid.status_for(BROKER_ENDPOINT)
        self.assertTrue(c.owns_client)
        self.assertIsNot(self.druid.status, c)
        c.close()
