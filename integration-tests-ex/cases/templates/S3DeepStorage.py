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
# Cluster for the S3 deep storage test.
#
# Required env vars:
#
# DRUID_CLOUD_BUCKET
# DRUID_CLOUD_PATH
# AWS_REGION
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY

''')

    def extend_druid_service(self, service):
        self.add_property(service, 'druid.storage.type', 's3')
        self.add_property(service, 'druid.s3.accessKey', '${AWS_ACCESS_KEY_ID}')
        self.add_property(service, 'druid.s3.secretKey', '${AWS_SECRET_ACCESS_KEY}')
        self.add_property(service, 'druid.storage.bucket', '${DRUID_CLOUD_BUCKET}')
        self.add_property(service, 'druid.storage.baseKey', '${DRUID_CLOUD_PATH}')
        self.add_env(service, 'AWS_REGION', '${AWS_REGION}')

    # This test uses different data than the default.
    def define_data_dir(self, service):
        self.add_volume(service, '../data', '/resources')

generate(__file__, Template())
