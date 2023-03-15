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

from druidapi.error import ClientError

def dict_get(dict, key, default=None):
    '''
    Returns the value of key in the given dict, or the default value if
    the key is not found.
    '''
    if not dict:
        return default
    return dict.get(key, default)

def split_table_name(table_name, default_schema):
    if not table_name:
        raise ClientError('Table name is required')
    parts = table_name.split('.')
    if len(parts) > 2:
        raise ClientError('Druid supports one or two-part table names')
    if len(parts) == 2:
        return parts
    return [default_schema, parts[0]]
