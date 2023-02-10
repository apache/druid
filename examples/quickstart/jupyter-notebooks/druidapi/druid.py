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

from .rest import DruidRestClient
from .status import StatusClient
from .catalog import CatalogClient
from .sql import QueryClient
from .tasks import TaskClient
from .datasource import DatasourceClient

class DruidClient:

    def __init__(self, router_endpoint):
        self.rest_client = DruidRestClient(router_endpoint)
        self.status_client = None
        self.catalog_client = None
        self.sql_client = None
        self.tasks_client = None
        self.datasource_client = None

    def rest(self):
        return self.rest_client

    def trace(self, enable=True):
        self.rest_client.enable_trace(enable)
    
    def status(self, endpoint=None) -> StatusClient:
        '''
        Returns the status client for the router by default, else the status
        endpoint for the specified endpoint.
        '''
        if endpoint is None:
            if self.status_client is None:
                self.status_client = StatusClient(self.rest_client)
            return self.status_client
        else:
            endpoint_client = DruidRestClient(endpoint)
            return StatusClient(endpoint_client)

    def catalog(self):
        if self.catalog_client is None:
            self.catalog_client = CatalogClient(self.rest_client)
        return self.catalog_client

    def sql(self):
        if self.sql_client is None:
            self.sql_client = QueryClient(self)
        return self.sql_client

    def tasks(self):
        if self.tasks_client is None:
            self.tasks_client = TaskClient(self.rest_client)
        return self.tasks_client

    def datasources(self):
        if self.datasource_client is None:
            self.datasource_client = DatasourceClient(self.rest_client)
        return self.datasource_client
