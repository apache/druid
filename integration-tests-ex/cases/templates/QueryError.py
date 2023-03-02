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

from template import BaseTemplate, generate
from template import ZOO_KEEPER

class Template(BaseTemplate):

    # No historical in this cluster
    def define_historical(self):
        pass

    # Custom historical node for Query Error category
    def define_custom_services(self):
        service_name = 'historical-for-query-error-test'
        service = self.define_full_service(service_name, None, 14)
        service['container_name'] = service_name
        self.add_env_config(service, 'historical-for-query-retry-error-test')
        self.add_env(service, 'druid_host', service_name)
        service['ports'] = [ '8086:8083', '8284:8283', '5010:8000']
        #8084 port is used by mono service on GHA runners
        self.add_depends(service, [ ZOO_KEEPER ] )

generate(__file__, Template())
