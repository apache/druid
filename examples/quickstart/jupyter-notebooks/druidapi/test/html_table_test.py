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
from .setup import setup
setup()

from druidapi.html_display import HtmlTable
from druidapi.base_table import ALIGN_CENTER, ALIGN_RIGHT

class TestHtmlTable(unittest.TestCase):

    def test_empty_table(self):
        table = HtmlTable()
        self.assertEqual('', table.format())
        table = HtmlTable()
        table.from_object_list({})
        self.assertEqual('', table.format())

    def test_basic_table(self):
        data = [
            {'String': 'foo', 'Number': 10},
            {'String': 'bar-o-matic', 'Number': 203}
        ]
        table = HtmlTable()
        table.from_object_list(data)
        html = table.format()
        expected = '''
<table>
<tr><th>String</th><th class="druid-right">Number</th></tr>
<tr><td>foo</td><td class="druid-right">10</td></tr>
<tr><td>bar-o-matic</td><td class="druid-right">203</td></tr>
</table>
        '''
        self.assertEqual(expected.strip(), html.strip())

    def test_align(self):
        data = [
            {'String': 'foo', 'Number': 10},
            {'String': 'bar-o-matic', 'Number': 203}
        ]
        table = HtmlTable()
        table.from_object_list(data)
        table.alignments([ALIGN_CENTER, ALIGN_RIGHT])
        html = table.format()
        expected = '''
<table>
<tr><th class="druid-center">String</th><th class="druid-right">Number</th></tr>
<tr><td class="druid-center">foo</td><td class="druid-right">10</td></tr>
<tr><td class="druid-center">bar-o-matic</td><td class="druid-right">203</td></tr>
</table>
        '''
        self.assertEqual(expected.strip(), html.strip())
      
    def test_object(self):
        data = {
            'First': 10,
            'Second': 'foo'
        }
        table = HtmlTable()
        table.from_object(data)
        html = table.format()
        expected = '''
<table>
<tr><th>Key</th><th>Value</th></tr>
<tr><td>First</td><td>10</td></tr>
<tr><td>Second</td><td>foo</td></tr>
</table>
        '''
        self.assertEqual(expected.strip(), html.strip())
