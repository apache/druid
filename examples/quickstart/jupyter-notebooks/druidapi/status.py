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

import time

STATUS_BASE = '/status'
REQ_STATUS = STATUS_BASE
REQ_HEALTH = STATUS_BASE + '/health'
REQ_PROPERTIES = STATUS_BASE + '/properties'
REQ_IN_CLUSTER = STATUS_BASE + '/selfDiscovered/status'

ROUTER_BASE = '/druid/router/v1'
REQ_BROKERS = ROUTER_BASE + '/brokers'

class StatusClient:
    '''
    Client for status APIs. These APIs are available on all nodes.
    If used with the Router, they report the status of just the Router.
    To check the status of other nodes, first create a REST endpoint for that
    node:

      status_client = StatusClient(DruidRestClient("<service endpoint>"))

    You can find the service endpoints by querying the sys.servers table using SQL.

    See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
    '''
    
    def __init__(self, rest_client, owns_client=False):
        self.rest_client = rest_client
        self.owns_client = owns_client

    def close(self):
        if self.owns_client:
            self.rest_client.close()
        self.rest_client = None
    
    #-------- Common --------

    @property
    def status(self):
        '''
        Returns the Druid version, loaded extensions, memory used, total memory 
        and other useful information about the Druid service.

        GET `/status`
        '''
        return self.rest_client.get_json(REQ_STATUS)
    
    @property
    def is_healthy(self) -> bool:
        '''
        Returns `True` if the node is healthy, `False` otherwise. Check service health
        before using other Druid API methods to ensure the server is ready.

        See also `wait_until_ready()`.
 
        GET `/status/health`
        '''
        try:
            return self.rest_client.get_json(REQ_HEALTH)
        except Exception:
            return False
    
    def wait_until_ready(self):
        '''
        Sleeps until the node reports itself as healthy. Will run forever if the node
        is down or never becomes healthy.
        '''
        while not self.is_healthy:
            time.sleep(0.5)
    
    @property
    def properties(self) -> map:
        '''
        Returns the effective set of Java properties used by the service, including
        system properties and properties from the `common_runtime.propeties` and
        `runtime.properties` files.

        GET `/status/properties`
        '''
        return self.rest_client.get_json(REQ_PROPERTIES)
    
    @property
    def in_cluster(self):
        '''
        Returns `True` if the node is visible within the cluster, `False` if not.
        That is, returns the value of the `{"selfDiscovered": true/false}`
        field in the response.

        GET `/status/selfDiscovered/status`
        '''
        try:
            result = self.rest_client.get_json(REQ_IN_CLUSTER)
            return result.get('selfDiscovered', False)
        except ConnectionError:
            return False

    @property
    def version(self):
        '''
        Returns the version of the Druid server. If the server is running in an IDE, the
        version will be empty.
        '''
        return self.status.get('version')

    @property
    def brokers(self):
        '''
        Returns the list of broker nodes known to this node. Must be called on the Router.
        '''
        return self.rest_client.get_json(REQ_BROKERS)
