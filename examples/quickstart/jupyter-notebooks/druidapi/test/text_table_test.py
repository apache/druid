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

import unittest
from .setup import setup, ROUTER_ENDPOINT, BROKER_ENDPOINT
setup()

from druidapi.text_display import TextTable
from druidapi.base_table import ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT

class TestTextTable(unittest.TestCase):

    def test_empty_table(self):
        table = TextTable()
        self.assertEqual('', table.format())
        table = TextTable()
        table.from_object_list({})
        self.assertEqual('', table.format())

    def test_basic_table(self):
        data = [
            {'String': 'foo', 'Number': 10},
            {'String': 'bar-o-matic', 'Number': 203}
        ]
        table = TextTable()
        table.from_object_list(data)
        text = table.format()
        expected = '''
String      Number
foo             10
bar-o-matic    203
        '''
        self.assertEqual(expected.strip(), text.strip())

        table = TextTable()
        table.with_border()
        table.from_object_list(data)
        text = table.format()
        expected = '''
String      | Number
------------+-------
foo         |     10
bar-o-matic |    203
        '''
        self.assertEqual(expected.strip(), text.strip())

    def test_align(self):
        data = [
            {'String': 'foo', 'Number': 10},
            {'String': 'bar-o-matic', 'Number': 203}
        ]
        table = TextTable()
        table.from_object_list(data)
        table.alignments([ALIGN_CENTER, ALIGN_RIGHT])
        text = table.format()
        expected = '''
  String    Number
    foo         10
bar-o-matic    203
        '''
        self.assertEqual(expected.strip(), text.strip())
      
    def test_object(self):
        data = {
            'First': 10,
            'Second': 'foo'
        }
        table = TextTable()
        table.from_object(data)
        text = table.format()
        expected = '''
Key    Value
First     10
Second   foo
        '''
        self.assertEqual(expected.strip(), text.strip())
