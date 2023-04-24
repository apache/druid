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

from druidapi.rest import DruidRestClient
from druidapi.status import StatusClient
from druidapi.catalog import CatalogClient
from druidapi.sql import QueryClient
from druidapi.tasks import TaskClient
from druidapi.datasource import DatasourceClient
from druidapi.basic_auth import BasicAuthClient

class DruidClient:
    '''
    Client for a Druid cluster. Functionality is split into a number of
    specialized "clients" that group many of Druid's REST API calls.
    '''

    def __init__(self, router_endpoint, display_client=None, auth=None):
        self.rest_client = DruidRestClient(router_endpoint, auth=auth)
        self.status_client = None
        self.catalog_client = None
        self.sql_client = None
        self.tasks_client = None
        self.datasource_client = None
        if display_client:
            self.display_client = display_client
        else:
            from druidapi.text_display import TextDisplayClient
            self.display_client = TextDisplayClient()
        self.display_client._druid = self

    @property
    def rest(self):
        '''
        Returns the low-level REST client. Useful for debugging and to access REST API
        calls not yet wrapped by the various function-specific clients.

        If you find you need to use this, consider creating a wrapper function in Python
        and contributing it to Druid via a pull request.
        '''
        return self.rest_client

    def trace(self, enable=True):
        '''
        Enable or disable tracing. When enabled, the Druid client prints the
        URL and payload for each REST API call. Useful for debugging, or if you want
        to learn what the code does so you can replicate it in your own client.
        '''
        self.rest_client.enable_trace(enable)

    @property
    def status(self) -> StatusClient:
        '''
        Returns the status client for the Router service.
        '''
        if not self.status_client:
            self.status_client = StatusClient(self.rest_client)
        return self.status_client

    def status_for(self, endpoint) -> StatusClient:
        '''
        Returns the status client for a Druid service.

        Parameters
        ----------
        endpoint: str
            The URL for a Druid service.
        '''
        return StatusClient(DruidRestClient(endpoint), True)

    @property
    def catalog(self) -> CatalogClient:
        '''
        Returns the catalog client to interact with the Druid catalog.
        '''
        if not self.catalog_client:
            self.catalog_client = CatalogClient(self.rest_client)
        return self.catalog_client

    @property
    def sql(self) -> QueryClient:
        '''
        Returns the SQL query client to submit interactive or MSQ queries.
        '''
        if not self.sql_client:
            self.sql_client = QueryClient(self)
        return self.sql_client

    @property
    def tasks(self) -> TaskClient:
        '''
        Returns the Overlord tasks client to submit and track tasks.
        '''
        if not self.tasks_client:
            self.tasks_client = TaskClient(self.rest_client)
        return self.tasks_client

    @property
    def datasources(self) -> DatasourceClient:
        '''
        Returns the Coordinator datasources client to manipulate datasources.
        Prefer to use the SQL client to query the INFORMATION_SCHEMA to obtain
        information about datasources.
        '''
        if not self.datasource_client:
            self.datasource_client = DatasourceClient(self.rest_client)
        return self.datasource_client
    
    def basic_security(self, authenticator, authorizer=None):
        '''
        Returns a client to work with a basic authorization authenticator/authorizer pair.
        This client assumes the typical case of one authenticator and one authorizer. If
        you have more than one, create multiple clients.

        The basic security API is not proxied through the Router: it must work directly with
        the Coordinator. Create an ad hoc Druid client for your Coordinator. Because you have
        basic security enabled, you must specify the admin user and password:

        ```
        coord = druidapi.jupyter_client('http://localhost:8081', auth=('admin', 'admin-pwd'))
        ac = coord.basic_security('yourAuthenticator', 'yourAuthorizer')
        ```

        Parameters
        ----------
        authenticator: str
            Authenticator name as set in the `druid.auth.authenticatorChain`
            runtime property.

        authorizer: str, default = same as authenticator
            Authorizer name as set in the `druid.auth.authorizers` runtime property.
            Defaults to the same name as the `authenticator` parameter for simple cases.
        '''
        return BasicAuthClient(self.rest_client, authenticator, authorizer)

    @property
    def display(self):
        return self.display_client

    def close(self):
        self.rest_client.close()
        self.rest_client = None
        self.catalog_client = None
        self.tasks_client = None
        self.datasource_client = None
        self.sql_client = None
