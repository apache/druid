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

class Template(BaseTemplate):

    def define_indexer(self):
        service = super().define_indexer()
        self.add_property(service, 'druid.msq.intermediate.storage.enable', 'true')
        self.add_property(service, 'druid.msq.intermediate.storage.type', 'local')
        self.add_property(service, 'druid.msq.intermediate.storage.basePath', '/shared/durablestorage/')
        self.add_property(service, 'druid.export.storage.baseDir', '/')

    def extend_druid_service(self, service):
        self.add_env_file(service, '../Common/environment-configs/auth.env')
        self.add_env(service, 'druid_test_loadList', 'druid-basic-security')


generate(__file__, Template())
