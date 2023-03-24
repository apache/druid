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
from template import COORDINATOR, ZOO_KEEPER, METADATA, OVERLORD

# The second Coordinator (and Overlord) cannot extend
# The base service: they need distinct ports.
class Template(BaseTemplate):

    def define_coordinator(self):
        self.define_coordinator_one()
        self.define_coordinator_two()

    def define_coordinator_one(self):
        service_name = COORDINATOR + '-one'
        service = self.define_master_service(service_name, COORDINATOR)
        service['container_name'] = service_name
        self.add_env(service, 'DRUID_INSTANCE', 'one')
        self.add_env(service, 'druid_host', service_name)
        service['container_name'] = service_name

    def define_coordinator_two(self):
        service_name = COORDINATOR + '-two'
        service = self.define_full_service(service_name, COORDINATOR, 120)
        service['container_name'] = service_name
        self.add_env(service, 'DRUID_INSTANCE', 'two')
        self.add_env(service, 'druid_host', service_name)
        service['ports'] = [ '18081:8081', '18281:8281', '15006:8000' ]
        self.add_depends(service, [ ZOO_KEEPER, METADATA ] )

    def define_overlord(self):
        self.define_overlord_one()
        self.define_overlord_two()

    def define_overlord_one(self):
        service_name = OVERLORD + '-one'
        service = self.define_master_service(service_name, OVERLORD)
        service['container_name'] = service_name
        self.add_env(service, 'DRUID_INSTANCE', 'one')
        self.add_env(service, 'druid_host', service_name)

    def define_overlord_two(self):
        service_name = OVERLORD + '-two'
        service = self.define_full_service(service_name, OVERLORD, 110)
        service['container_name'] = service_name
        self.add_env(service, 'DRUID_INSTANCE', 'two')
        self.add_env(service, 'druid_host', service_name)
        service['ports'] = [ '18090:8090', '18290:8290', '15009:8000' ]
        self.add_depends(service, [ ZOO_KEEPER, METADATA ] )

    # No indexer in this cluster
    def define_indexer(self):
        pass

    # No historical in this cluster
    def define_historical(self):
        pass

    # The custom node role has no base definition. Also, there is
    # no environment file: the needed environment settings are
    # given here.
    def define_custom_services(self):
        service_name = 'custom-node-role'
        service = self.define_full_service(service_name, None, 90)
        service['container_name'] = service_name
        self.add_env(service, 'DRUID_SERVICE', service_name)
        self.add_env(service, 'SERVICE_DRUID_JAVA_OPTS', '-Xmx64m -Xms64m')
        self.add_env(service, 'druid_host', service_name)
        service['ports'] = [ '50011:50011', '9301:9301', '9501:9501', '5010:8000' ]
        self.add_depends(service, [ ZOO_KEEPER ] )

generate(__file__, Template())
