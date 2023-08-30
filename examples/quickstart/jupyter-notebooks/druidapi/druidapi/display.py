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

from druidapi import consts
import time

class DisplayClient:
    '''
    Abstract base class to display various kinds of results.
    '''

    def __init__(self, druid=None):
        # If the client is None, it must be backfilled by the caller.
        # This case occurs only when creating the DruidClient to avoid
        # a circular depencency.
        self._druid = druid

    # Basic display operations

    def text(self, msg):
        raise NotImplementedError()

    def alert(self, msg):
        raise NotImplementedError()

    def error(self, msg):
        raise NotImplementedError()

    # Tabular formatting

    def new_table(self):
        raise NotImplementedError()

    def show_table(self, table):
        raise NotImplementedError()

    def data_table(self, rows, cols=None):
        '''
        Display a table of data with the optional column headings.

        Parameters
        ----------
        objects: list[list]
            The data to display as a list of lists, where each inner list represents one
            row of data. Rows should be of the same width: ragged rows will display blank
            cells. Data can be of any scalar type and is formatted correctly for that type.

        cols: list[str]
            Optional list of column headings.
        '''
        table = self.new_table()
        table.rows(rows)
        table.headers(cols)
        self.show_table(table)

    def object_list(self, objects, cols=None):
        '''
        Display a list of objects represented as dictionaries with optional headings.

        Parameters
        ----------
        objects: list[dict]
            List of dictionaries: one dictionary for each row.

        cols: dict, Default = None
            A list of column headings in the form `{'key': 'label'}`
        '''
        table = self.new_table()
        table.from_object_list(objects, cols)
        self.show_table(table)

    def object(self, obj, labels=None):
        '''
        Display a single object represented as a dictionary with optional headings.
        The object is displayed in two columns: keys and values.

        Parameters
        ----------
        objects: list[dict]
            List of dictionaries: one dictionary for each row.

        labels: list, Default = None
            A list of column headings in the form `['key', 'value']`. Default headings
            are used if the lables are not provided.
        '''
        table = self.new_table()
        table.from_object(obj, labels)
        self.show_table(table)

    # SQL formatting

    def sql(self, sql):
        '''
        Run a query and display the result as a table.

        Parameters
        ----------
        query
            The query as either a string or a SqlRequest object.
        '''
        self._druid.sql.sql_query(sql).show(display=self)

    def table(self, table_name):
        '''
        Describe a table by returning the list of columns in the table.

        Parameters
        ----------
        table_name str
            The name of the table as either "table" or "schema.table".
            If the form is "table", then the 'druid' schema is assumed.
        '''
        self._druid.sql._schema_query(table_name).show(display=self)

    def function(self,  table_name):
        '''
        Retrieve the list of parameters for a partial external table defined in
        the Druid catalog.

        Parameters
        ----------
        table_name str
            The name of the table as either "table" or "schema.table".
            If the form is "table", then the 'ext' schema is assumed.
        '''
        return self._druid.sql._function_args_query(table_name).show(display=self)

    def schemas(self):
        '''
        Display the list of schemas available in Druid.
        '''
        self._druid.sql._schemas_query().show()

    def tables(self, schema=consts.DRUID_SCHEMA):
        self._druid.sql._tables_query(schema).show(display=self)

    def run_task(self, query):
        '''
        Run an MSQ task while displaying progress in the cell output.
        :param query: INSERT/REPLACE statement to run
        :return: None
        '''
        from tqdm import tqdm

        task = self._druid.sql.task(query)
        with tqdm(total=100.0) as pbar:
            previous_progress = 0.0
            while True:
                reports=task.reports_no_wait()
                # check if progress metric is available and display it
                if 'multiStageQuery' in reports.keys():
                    if 'payload' in reports['multiStageQuery'].keys():
                        if 'counters' in reports['multiStageQuery']['payload'].keys():
                            if ('0' in reports['multiStageQuery']['payload']['counters'].keys() ) and \
                               ('0' in reports['multiStageQuery']['payload']['counters']['0'].keys()):
                                if 'progressDigest' in reports['multiStageQuery']['payload']['counters']['0']['0']['sortProgress'].keys():
                                    current_progress = reports['multiStageQuery']['payload']['counters']['0']['0']['sortProgress']['progressDigest']*100.0
                                    pbar.update( current_progress - previous_progress ) # update requires a relative value
                                    previous_progress = current_progress
                        # present status if available
                        if 'status' in reports['multiStageQuery']['payload'].keys():
                            pbar.set_description(f"Loading data, status:[{reports['multiStageQuery']['payload']['status']['status']}]")
                            # stop when job is done
                            if reports['multiStageQuery']['payload']['status']['status'] in ['SUCCESS', 'FAILED']:
                                break;
                else:
                    pbar.set_description('Initializing...')
                time.sleep(1)
