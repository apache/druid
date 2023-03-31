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

# Unit test for the drupdapi.util.py file

import unittest
from .setup import setup
setup()

from druidapi.util import dict_get, split_table_name
from druidapi.error import ClientError

class TestUtils(unittest.TestCase):

    def test_dict_get(self):
        self.assertIsNone(dict_get(None, 'foo'))
        self.assertIsNone(dict_get({}, 'foo'))
        self.assertEqual(10, dict_get({'foo': 10}, 'foo'))

    def test_split_table_name(self):
        with self.assertRaises(ClientError) as cm:
            split_table_name(None, 'foo')
        ex = cm.exception
        self.assertEqual('Table name is required', str(ex))
        
        self.assertEqual(['foo', 'bar'], split_table_name('bar', 'foo'))
        self.assertEqual(['druid', 'bar'], split_table_name('druid.bar', 'foo'))

        with self.assertRaises(ClientError) as cm:
            split_table_name('a.b.c', 'foo')
        ex = cm.exception
        self.assertEqual('Druid supports one or two-part table names', str(ex))

if __name__ == '__main__':
    unittest.main()
