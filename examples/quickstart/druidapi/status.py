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

STATUS_BASE = "/status"
REQ_STATUS = STATUS_BASE
REQ_HEALTH = STATUS_BASE + "/health"
REQ_PROPERTIES = STATUS_BASE + "/properties"
REQ_IN_CLUSTER = STATUS_BASE + "/selfDiscovered/status"

class StatusClient:
    '''
    Client for status APIs. These APIs are available on all nodes.
    If used with the router, they report the status of just the router.
    '''
    
    def __init__(self, rest_client):
        self.client = rest_client
    
    #-------- Common --------

    def status(self):
        """
        Returns the Druid version, loaded extensions, memory used, total memory 
        and other useful information about the process.

        GET `/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        return self.client.get_json(REQ_STATUS)
    
    def is_healthy(self) -> bool:
        """
        Returns `True` if the node is healthy, an exception otherwise.
        Useful for automated health checks.

        GET `/status/health`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        try:
            return self.client.get_json(REQ_HEALTH)
        except Exception:
            return False
    
    def properties(self) -> map:
        """
        Returns the effective set of Java properties used by the service, including
        system properties and properties from the `common_runtime.propeties` and
        `runtime.properties` files.

        GET `/status/properties`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        return self.client.get_json(REQ_PROPERTIES)
    
    def in_cluster(self):
        """
        Returns `True` if the node is visible wihtin the cluster, `False` if not.
        (That is, returns the value of the `{"selfDiscovered": true/false}`
        field in the response.

        GET `/status/selfDiscovered/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        try:
            result = self.client.get_json(REQ_IN_CLUSTER)
            return result.get('selfDiscovered', False)
        except ConnectionError:
            return False

    def version(self):
        return self.status().get('version')
