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
from template import COORDINATOR, MIDDLE_MANAGER

class Template(BaseTemplate):

    def define_coordinator(self):
        service_name = COORDINATOR
        service = self.define_master_service(service_name, COORDINATOR)
        self.add_env(service, 'druid_host', service_name)
        self.add_env(service, 'druid_manager_segments_pollDuration', 'PT5S')
        self.add_env(service, 'druid_coordinator_period', 'PT10S')

    def define_indexer(self):
        '''
        Override the indexer to MIDDLE_MANAGER
        '''
        service = self.define_std_indexer(MIDDLE_MANAGER)
        self.add_env(service, 'druid_msq_intermediate_storage_enable', 'true')
        self.add_env(service, 'druid_msq_intermediate_storage_type', 'local')
        self.add_env(service, 'druid_msq_intermediate_storage_basePath', '/shared/durablestorage/')

    # No kafka dependency in this cluster
    def define_kafka(self):
        pass

generate(__file__, Template())
