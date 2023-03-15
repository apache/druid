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

from IPython.display import display, HTML
from html import escape
from druidapi.display import DisplayClient
from druidapi.base_table import BaseTable

STYLES = '''
<style>
  .druid table {
    border: 1px solid black;
    border-collapse: collapse;
  }

  .druid th, .druid td {
    padding: 4px 1em ;
    text-align: left;
  }

  td.druid-right, th.druid-right {
    text-align: right;
  }

  td.druid-center, th.druid-center {
    text-align: center;
  }

  .druid .druid-left {
    text-align: left;
  }

  .druid-alert {
    font-weight: bold;
  }

  .druid-error {
    color: red;
  }
</style>
'''

def escape_for_html(s):
    # Annoying: IPython treats $ as the start of Latex, which is cool,
    # but not wanted here.
    return s.replace('$', '\\$')

def html(s):
    display(HTML(s))

initialized = False

alignments = ['druid-left', 'druid-center', 'druid-right']

def start_tag(tag, align):
    s = '<' + tag
    if align:
        s += ' class="{}"'.format(alignments[align])
    return s + '>'

class HtmlTable(BaseTable):

    def __init__(self):
        BaseTable.__init__(self)

    def widths(self, widths):
        self._widths = widths

    def format(self) -> str:
        if not self._rows and not self._headers:
            return ''
        _, width = self.row_width(self._rows)
        headers = self.pad_headers(width)
        rows = self.pad_rows(self._rows, width)
        s = '<table>\n'
        s += self.gen_header(headers)
        s += self.gen_rows(rows)
        return s + '\n</table>'

    def gen_header(self, headers):
        if not headers:
            return ''
        s = '<tr>'
        for i in range(len(headers)):
            s += start_tag('th', self.col_align(i)) + escape(headers[i]) + '</th>'
        return s + '</tr>\n'

    def gen_rows(self, rows):
        html_rows = []
        for row in rows:
            r = '<tr>'
            for i in range(len(row)):
                r += start_tag('td', self.col_align(i))
                cell = row[i]
                value = '' if cell is None else escape(str(cell))
                r += value + '</td>'
            html_rows.append(r + '</tr>')
        return '\n'.join(html_rows)

    def col_align(self, col):
        if not self._align:
            return None
        if col >= len(self._align):
            return None
        return self._align[col]

class HtmlDisplayClient(DisplayClient):

    def __init__(self):
        DisplayClient.__init__(self)
        global initialized
        if not initialized:
            display(HTML(STYLES))
            initialized = True

    def text(self, msg):
        html('<div class="druid">' + escape_for_html(msg) + '</div>')

    def alert(self, msg):
        html('<div class="druid-alert">' + escape_for_html(msg.replace('\n', '<br/>')) + '</div>')

    def error(self, msg):
        html('<div class="druid-error">ERROR: ' + escape_for_html(msg.replace('\n', '<br/>')) + '</div>')

    def new_table(self):
        return HtmlTable()

    def show_table(self, table):
        self.text(table.format())
