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

import requests
from druidapi.consts import COORD_BASE
from druidapi.rest import check_error

# Catalog (new feature in Druid 26)
CATALOG_BASE = COORD_BASE + '/catalog'
REQ_CAT_SCHEMAS = CATALOG_BASE + '/schemas'
REQ_CAT_SCHEMA = REQ_CAT_SCHEMAS + '/{}'
REQ_CAT_SCHEMA_TABLES = REQ_CAT_SCHEMA + '/tables'
REQ_CAT_SCHEMA_TABLE = REQ_CAT_SCHEMA_TABLES + '/{}'
REQ_CAT_SCHEMA_TABLE_EDIT = REQ_CAT_SCHEMA_TABLE + '/edit'

class CatalogClient:
    '''
    Client for the Druid catalog feature that provides metadata for tables,
    including both datasources and external tables.
    '''

    def __init__(self, rest_client):
        self.client = rest_client

    def post_table(self, schema, table_name, table_spec, version=None, overwrite=None):
        params = {}
        if version:
            params['version'] = version
        if overwrite is not None:
            params['overwrite'] = overwrite
        return self.client.post_json(REQ_CAT_SCHEMA_TABLE, table_spec, args=[schema, table_name], params=params)

    def create(self, schema, table_name, table_spec):
        self.post_table(schema, table_name, table_spec)

    def table(self, schema, table_name):
        return self.client.get_json(REQ_CAT_SCHEMA_TABLE, args=[schema, table_name])

    def drop_table(self, schema, table_name, if_exists=False):
        r = self.client.delete(REQ_CAT_SCHEMA_TABLE, args=[schema, table_name])
        if if_exists and r.status_code == requests.codes.not_found:
            return
        check_error(r)

    def edit_table(self, schema, table_name, action):
        return self.client.post_json(REQ_CAT_SCHEMA_TABLE_EDIT, action, args=[schema, table_name])

    def schema_names(self):
        return self.client.get_json(REQ_CAT_SCHEMAS)

    def tables_in_schema(self, schema, list_format='name'):
        return self.client.get_json(REQ_CAT_SCHEMA_TABLES, args=[schema], params={'format': list_format})
