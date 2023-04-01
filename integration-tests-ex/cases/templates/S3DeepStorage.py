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
from template import ZOO_KEEPER, METADATA, OVERLORD

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

        # Adding the following to make druid work with MinIO
        # See https://blog.min.io/how-to-druid-superset-minio/ for more details
        self.add_property(service, 'druid.s3.protocol', 'http')
        self.add_property(service, 'druid.s3.enablePathStyleAccess', 'true')
        self.add_property(service, 'druid.s3.endpoint.url', 'http://172.172.172.5:9000/')

    def define_overlord(self):
        service = self.define_druid_service(OVERLORD, OVERLORD)
        self.add_depends(service, [ZOO_KEEPER, METADATA, "create_minio_buckets"])
        return service

    # This test uses different data than the default.
    def define_data_dir(self, service):
        self.add_volume(service, '../data', '/resources')

    def create_minio_container(self):
        return self.define_external_service("minio")

    def create_minio_bucket(self):
        service = self.define_external_service("create_minio_buckets")
        self.add_depends(service, ["minio"])
        return service

    def define_custom_services(self):
        self.create_minio_container()
        self.create_minio_bucket()


generate(__file__, Template())
