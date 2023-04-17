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

COORD_BASE = '/druid/coordinator/v1'
ROUTER_BASE = '/druid/v2'
OVERLORD_BASE = '/druid/indexer/v1'

# System schemas and table names. Note: case must match in Druid, though
# SQL itself is supposed to be case-insensitive.
SYS_SCHEMA = 'sys'
INFORMATION_SCHEMA = 'INFORMATION_SCHEMA'
DRUID_SCHEMA = 'druid'
EXT_SCHEMA = 'ext'

# Information Schema tables
SCHEMA_TABLE = INFORMATION_SCHEMA + '.SCHEMATA'
TABLES_TABLE = INFORMATION_SCHEMA + '.TABLES'
COLUMNS_TABLE = INFORMATION_SCHEMA + '.COLUMNS'

# SQL request formats
SQL_OBJECT = 'object'
SQL_ARRAY = 'array'
SQL_ARRAY_WITH_TRAILER = 'arrayWithTrailer'
SQL_CSV = 'csv'

# Type names as known to Druid and mentioned in documentation.
DRUID_STRING_TYPE = 'string'
DRUID_LONG_TYPE = 'long'
DRUID_FLOAT_TYPE = 'float'
DRUID_DOUBLE_TYPE = 'double'
DRUID_TIMESTAMP_TYPE = 'timestamp'

# SQL type names as returned from the INFORMATION_SCHEMA
SQL_VARCHAR_TYPE = 'VARCHAR'
SQL_BIGINT_TYPE = 'BIGINT'
SQL_FLOAT_TYPE = 'FLOAT'
SQL_DOUBLE_TYPE = 'DOUBLE'
SQL_TIMESTAMP_TYPE = 'TIMESTAMP'
SQL_ARRAY_TYPE = 'ARRAY'

# Task status code
RUNNING_STATE = 'RUNNING'
SUCCESS_STATE = 'SUCCESS'
FAILED_STATE = 'FAILED'

# Resource constants
DATASOURCE_RESOURCE = 'DATASOURCE'
STATE_RESOURCE = 'STATE'
CONFIG_RESOURCE = 'CONFIG'
EXTERNAL_RESOURCE = 'EXTERNAL'
READ_ACTION = 'READ'
WRITE_ACTION = 'WRITE'
