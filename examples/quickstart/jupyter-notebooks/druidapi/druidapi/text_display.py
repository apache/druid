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

from druidapi.display import DisplayClient
from druidapi.base_table import pad, BaseTable

alignments = ['', '^', '>']

def simple_table(table_def):
    table = []
    if table_def.headers:
        table.append(' '.join(table_def.format_row(table_def.headers)))
    for row in table_def.rows:
        table.append(' '.join(table_def.format_row(row)))
    return table

def border_table(table_def):
    fmt = ' | '.join(table_def.formats)
    table = []
    if table_def.headers:
        table.append(fmt.format(*table_def.headers))
        bar = ''
        for i in range(table_def.width):
            width = table_def.widths[i]
            if i > 0:
                bar += '+'
            if table_def.width == 1:
                pass
            elif i == 0:
                width += 1
            elif i == table_def.width - 1:
                width += 1
            else:
                width += 2
            bar += '-' * width
        table.append(bar)
    for row in table_def.rows:
        table.append(fmt.format(*row))
    return table

class TableDef:

    def __init__(self):
        self.width = None
        self.headers = None
        self.align = None
        self.formats = None
        self.rows = None
        self.widths = None

    def find_widths(self):
        self.widths = [0 for i in range(self.width)]
        if self.headers:
            for i in range(len(self.headers)):
                self.widths[i] = len(self.headers[i])
        for row in self.rows:
            for i in range(len(row)):
                if row[i] is not None:
                    self.widths[i] = max(self.widths[i], len(row[i]))

    def apply_widths(self, widths):
        if not widths:
            return
        for i in range(min(len(self.widths), len(widths))):
            if widths[i] is not None:
                self.widths[i] = widths[i]

    def define_row_formats(self):
        self.formats = []
        for i in range(self.width):
            f = '{{:{}{}.{}}}'.format(
                alignments[self.align[i]],
                self.widths[i], self.widths[i])
            self.formats.append(f)

    def format_header(self):
        if not self.headers:
            return None
        return self.format_row(self.headers)

    def format_row(self, data_row):
        row = []
        for i in range(self.width):
            value = data_row[i]
            if not value:
                row.append(' ' * self.widths[i])
            else:
                row.append(self.formats[i].format(value))
        return row

class TextTable(BaseTable):

    def __init__(self):
        BaseTable.__init__(self)
        self.formatter = simple_table
        self._widths = None

    def with_border(self):
        self.formatter = border_table

    def widths(self, widths):
        self._widths = widths

    def compute_def(self, rows):
        table_def = TableDef()
        min_width, max_width = self.row_width(rows)
        table_def.width = max_width
        table_def.headers = self.pad_headers(max_width)
        table_def.rows = self.format_rows(rows, min_width, max_width)
        table_def.find_widths()
        table_def.apply_widths(self._widths)
        table_def.align = self.find_alignments(rows, max_width)
        table_def.define_row_formats()
        return table_def

    def format(self):
        if not self._rows:
            self._rows = []
        table_rows = self.formatter(self.compute_def(self._rows))
        return '\n'.join(table_rows)

    def format_rows(self, rows, min_width, max_width):
        if not self._col_fmt:
            return self.default_row_format(rows, min_width, max_width)
        else:
            return self.apply_row_formats(rows, max_width)

    def default_row_format(self, rows, min_width, max_width):
        new_rows = []
        if min_width <= max_width:
            rows = self.pad_rows(rows, max_width)
        for row in rows:
            new_row = ['' if v is None else str(v) for v in row]
            new_rows.append(pad(new_row, max_width, None))
        return new_rows

    def apply_row_formats(self, rows, max_width):
        new_rows = []
        fmts = self._col_fmt
        if len(fmts) < max_width:
            fmts = fmts.copy()
            for i in range(len(fmts), max_width):
                fmts.append(lambda v: v)
        for row in rows:
            new_row = []
            for i in range(len(row)):
                new_row.append(fmts[i](row[i]))
            new_rows.append(pad(new_row, max_width, None))
        return new_rows

class TextDisplayClient(DisplayClient):

    def __init__(self):
        DisplayClient.__init__(self)

    def text(self, msg):
        print(msg)

    def alert(self, msg):
        print("Alert:", msg)

    def error(self, msg):
        print("ERROR:", msg)

    def new_table(self):
        return TextTable()

    def show_table(self, table):
        print(table.format())
