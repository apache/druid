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

TEXT_TABLE = 0
HTML_TABLE = 1

class Display:

    def __init__(self):
        self.format = TEXT_TABLE
        self.html_initialized = False

    def text(self):
        self.format = TEXT_TABLE

    def html(self):
        self.format = HTML_TABLE
        if not self.html_initialized:
            from .html_table import styles
            styles()
            self.html_initialized = True
    
    def table(self):
        if self.format == HTML_TABLE:
            from .html_table import HtmlTable
            return HtmlTable()
        else:
            from .text_table import TextTable
            return TextTable()
    
    def show_object_list(self, objects, cols):
        list_to_table(self.table(), objects, cols)

    def show_object(self, obj, labels):
        object_to_table(self.table(), obj, labels)

    def show_error(self, msg):
        from .html_table import html_error
        html_error("<b>ERROR: " + msg + "</b")
    
    def show_message(self, msg):
        from .html_table import html
        html("<b>" + msg + "</b")

def list_to_table(table, objects, cols):
    cols = infer_keys(objects) if cols is None else cols
    rows = []
    for obj in objects:
        row = []
        for key in cols.keys():
            row.append(obj.get(key))
        rows.append(row)
    table.headers([head for head in cols.values()])
    table.show(rows)

def object_to_table(table, obj, labels):
    labels = infer_keys(obj) if labels is None else labels
    table_rows = []
    for key, head in labels.items():
        table_rows.append([head, obj.get(key)])
    table.headers(['Key', 'Value'])
    table.show(table_rows)

def infer_keys(data):
    if type(data) is list:
        data = data[0]
    keys = {}
    for key in data.keys():
        keys[key] = key
    return keys

display = Display()
