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

    def gen_header_comment(self):
        self.emit('''
# Cluster for the Google Cluster Storage (GCS) deep storage test.
#
# Required env vars:
#
# GOOGLE_BUCKET
# GOOGLE_PREFIX
# GOOGLE_APPLICATION_CREDENTIALS - must point to a file that holds the Google
#   credentials. Mounted into each Druid container.

''')

    def extend_druid_service(self, service):
        self.add_env(service, 'druid_test_loadList', 'druid-google-extensions')
        self.add_property(service, 'druid.storage.type', 'google')
        self.add_property(service, 'druid.google.bucket', '${GOOGLE_BUCKET}')
        self.add_property(service, 'druid.google.prefix', '${GOOGLE_PREFIX}')
        self.add_env(service, 'GOOGLE_APPLICATION_CREDENTIALS', '/resources/credentials.json')
        self.add_volume(service, '${GOOGLE_APPLICATION_CREDENTIALS}', '/resources/credentials.json')

    # This test uses different data than the default.
    def define_data_dir(self, service):
        self.add_volume(service, '../data', '/resources')

generate(__file__, Template())
