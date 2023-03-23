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

from druidapi.druid import DruidClient

def jupyter_client(endpoint) -> DruidClient:
    '''
    Create a Druid client configured to display results as HTML withing a Jupyter notebook.
    Waits for the cluster to become ready to avoid intermitent problems when using Druid.
    '''
    from druidapi.html_display import HtmlDisplayClient
    druid = DruidClient(endpoint, HtmlDisplayClient())
    druid.status.wait_until_ready()
    return druid

def client(endpoint) -> DruidClient:
    '''
    Create a Druid client for use in Python scripts that uses a text-based format for
    displaying results. Does not wait for the cluster to be ready: clients should call
    `status().wait_until_ready()` before making other Druid calls if there is a chance
    that the cluster has not yet fully started.
    '''
    return DruidClient(endpoint)

