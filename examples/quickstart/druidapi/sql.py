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

import time, requests
from . import consts, display
from .consts import ROUTER_BASE
from .util import is_blank
from .error import DruidError, ClientError

REQ_ROUTER_QUERY = ROUTER_BASE
REQ_ROUTER_SQL = ROUTER_BASE + '/sql'
REQ_ROUTER_SQL_TASK = REQ_ROUTER_SQL + '/task'

class SqlRequest:

    def __init__(self, druid, sql):
        self.druid_client = druid
        self.sql = sql
        self.context = None
        self.params = None
        self.header = False
        self.format = consts.SQL_OBJECT
        self.headers = None
        self.types = None
        self.sqlTypes = None
    
    def with_format(self, result_format):
        self.format = result_format
        return self
    
    def with_headers(self, sqlTypes=False, druidTypes=False):
        self.headers = True
        self.types = druidTypes
        self.sqlTypes = sqlTypes
        return self
    
    def with_context(self, context):
        if self.context is None:
            self.context = context
        else:
            self.context.update(context)
        return self
    
    def with_parameters(self, params):
        if self.params is None:
            self.params = params
        else:
            self.params.update(params)
        return self

    def response_header(self):
        self.header = True
        return self

    def request_headers(self, headers):
        self.headers = headers
        return self
    
    def to_request(self):
        query_obj = {"query": self.sql}
        if self.context is not None and len(self.context) > 0:
            query_obj['context'] = self.context
        if self.params is not None and len(self.params) > 0:
            query_obj['parameters'] = self.params
        if self.header:
            query_obj['header'] = True
        if self.result_format is not None:
            query_obj['resultFormat'] = self.format
        if self.sqlTypes:
            query_obj['sqlTypesHeader'] = self.sqlTypes
        if self.types:
            query_obj['typesHeader'] = self.types
        return query_obj

    def result_format(self):
        return self.format.lower()

    def run(self):
        return self.druid_client.sql().sql_query(self)

def parse_rows(fmt, context, results):
    if fmt == consts.SQL_ARRAY_WITH_TRAILER:
        rows = results['results']
    elif fmt == consts.SQL_ARRAY:
        rows = results
    else:
        return results
    if not context.get('headers', False):
        return rows
    header_size = 1
    if context.get('sqlTypesHeader', False):
        header_size += 1
    if context.get('typesHeader', False):
        header_size += 1
    return rows[header_size:]

def label_non_null_cols(results):
    if results is None or len(results) == 0:
        return []
    is_null = {}
    for key in results[0].keys():
        is_null[key] = True
    for row in results:
        for key, value in row.items():
            if type(value) == str:
                if value != '':
                    is_null[key] = False
            elif type(value) == float:
                if value != 0.0:
                    is_null[key] = False
            elif value is not None:
                is_null[key] = False
    return is_null

def filter_null_cols(results):
    '''
    Filter columns from a Druid result set by removing all null-like
    columns. A column is considered null if all values for that column
    are null. A value is null if it is either a JSON null, an empty
    string, or a numeric 0. All rows are preserved, as is the order
    of the remaining columns.
    '''
    if results is None or len(results) == 0:
        return results
    is_null = label_non_null_cols(results)
    revised = []
    for row in results:
        new_row = {}
        for key, value in row.items():
            if is_null[key]:
                continue
            new_row[key] = value
        revised.append(new_row)
    return revised

def parse_object_schema(results):
    schema = []
    if len(results) == 0:
        return schema
    row = results[0]
    for k, v in row.items():
        druid_type = None
        sql_type = None
        if type(v) is str:
            druid_type = consts.DRUID_STRING_TYPE
            sql_type = consts.SQL_VARCHAR_TYPE
        elif type(v) is int or type(v) is float:
            druid_type = consts.DRUID_LONG_TYPE
            sql_type = consts.SQL_BIGINT_TYPE
        schema.append(ColumnSchema(k, sql_type, druid_type))
    return schema

def parse_array_schema(context, results):
    schema = []
    if len(results) == 0:
        return schema
    has_headers = context.get(consts.HEADERS_KEY, False)
    if not has_headers:
        return schema
    has_sql_types = context.get(consts.SQL_TYPES_HEADERS_KEY, False)
    has_druid_types = context.get(consts.DRUID_TYPE_HEADERS_KEY, False)
    size = len(results[0])
    for i in range(size):
        druid_type = None
        if has_druid_types:
            druid_type = results[1][i]
        sql_type = None
        if has_sql_types:
            sql_type = results[2][i]
        schema.append(ColumnSchema(results[0][i], sql_type, druid_type))
    return schema

def parse_schema(fmt, context, results):
    if fmt == consts.SQL_OBJECT:
        return parse_object_schema(results)
    elif fmt == consts.SQL_ARRAY or fmt == consts.SQL_ARRAY_WITH_TRAILER:
        return parse_array_schema(context, results)
    else:
        return []

def is_response_ok(http_response):
    code = http_response.status_code
    return code == requests.codes.ok or code == requests.codes.accepted
 
class ColumnSchema:

    def __init__(self, name, sql_type, druid_type):
        self.name = name
        self.sql_type = sql_type
        self.druid_type = druid_type

    def __str__(self):
        return "{{name={}, SQL type={}, Druid type={}}}".format(self.name, self.sql_type, self.druid_type)

class SqlQueryResult:
    """
    Defines the core protocol for Druid SQL queries.
    """

    def __init__(self, request, response):
        self.http_response = response
        self._json = None
        self._rows = None
        self._schema = None
        self.request = request
        self._error = None
        self._id = None
        if not is_response_ok(response):
            try:
                self._error = response.json()
            except Exception:
                self._error = response.text
                if self._error is None or len(self._error) == 0:
                    self._error = "Failed with HTTP status {}".format(response.status_code)
        try:
            self._id = self.http_response.headers['X-Druid-SQL-Query-Id']
        except KeyError:
            self._error = "Query returned no query ID"

    def result_format(self):
        return self.request.result_format()

    def ok(self):
        """
        Reports if the query succeeded.

        The query rows and schema are available only if ok() returns True.
        """
        return is_response_ok(self.http_response)
    
    def error(self):
        """
        If the query fails, returns the error, if any provided by Druid.
        """
        return self._error

    def error_msg(self):
        err = self.error()
        if err is None:
            return "unknown"
        if type(err) is str:
            return err
        msg = err.get("error")
        text = err.get("errorMessage")
        if msg is None and text is None:
            return "unknown"
        if msg is None:
            return text
        if text is None:
            return msg
        return msg + ": " + text

    def id(self):
        """
        Returns the unique identifier for the query.
        """
        return self._id

    def non_null(self):
        if not self.ok():
            return None
        if self.result_format() != consts.SQL_OBJECT:
            return None
        return filter_null_cols(self.rows())

    def as_array(self):
        if self.result_format() == consts.SQL_OBJECT:
            rows = []
            for obj in self.rows():
                rows.append([v for v in obj.values()])
            return rows
        else:
            return self.rows()

    def error(self):
        if self.ok():
            return None
        if self._error is not None:
            return self._error
        if self.http_response is None:
            return { "error": "unknown"}
        if is_response_ok(self.http_response):
            return None
        return {"error": "HTTP {}".format(self.http_response.status_code)}

    def json(self):
        if not self.ok():
            return None
        if self._json is None:
            self._json = self.http_response.json()
        return self._json
    
    def rows(self):
        """
        Returns the rows of data for the query.

        Druid supports many data formats. The method makes its best
        attempt to map the format into an array of rows of some sort.
        """
        if self._rows is None:
            json = self.json()
            if json is None:
                return self.http_response.text
            self._rows = parse_rows(self.result_format(), self.request.context, json)
        return self._rows

    def schema(self):
        """
        Returns the data schema as a list of ColumnSchema objects.

        Druid supports many data formats, not all of them provide
        schema information. This method makes its best attempt to
        extract the schema from the query results.
        """
        if self._schema is None:
            self._schema = parse_schema(self.result_format(), self.request.context, self.json())
        return self._schema

    def show(self, non_null=False):
        data = None
        if non_null:
            data = self.non_null()
        if data is None:
            data = self.as_array()
        disp = display.display.table()
        disp.headers([c.name for c in self.schema()])
        disp.show(data)

    def show_schema(self):
        disp = display.display.table()
        disp.headers(['Name', 'SQL Type', 'Druid Type'])
        data = []
        for c in self.schema():
            data.append([c.name, c.sql_type, c.druid_type])
        disp.show(data)

class QueryTaskResult:

    def __init__(self, request, response):
        self._request = request
        self.http_response = response
        self._status = None
        self._results = None
        self._details = None
        self._schema = None
        self._rows = None
        self._reports = None
        self._schema = None
        self._results = None
        self._error = None
        self._id = None
        if not is_response_ok(response):
            self._state = consts.FAILED_STATE
            try:
                self._error = response.json()
            except Exception:
                self._error = response.text
                if self._error is None or len(self._error) == 0:
                    self._error = "Failed with HTTP status {}".format(response.status_code)
            return

        # Typical response:
        # {'taskId': '6f7b514a446d4edc9d26a24d4bd03ade_fd8e242b-7d93-431d-b65b-2a512116924c_bjdlojgj',
        # 'state': 'RUNNING'}
        self.response_obj = response.json()
        self._id = self.response_obj['taskId']
        self._state = self.response_obj['state']
    
    def ok(self):
        """
        Reports if the query succeeded.

        The query rows and schema are available only if ok() returns True.
        """
        return self._error is None

    def id(self):
        return self._id

    def _tasks(self):
        return self._request.druid_client.tasks()

    def status(self):
        """
        Polls Druid for an update on the query run status.
        """
        self.check_valid()
        # Example:
        # {'task': 'talaria-sql-w000-b373b68d-2675-4035-b4d2-7a9228edead6', 
        # 'status': {
        #   'id': 'talaria-sql-w000-b373b68d-2675-4035-b4d2-7a9228edead6', 
        #   'groupId': 'talaria-sql-w000-b373b68d-2675-4035-b4d2-7a9228edead6', 
        #   'type': 'talaria0', 'createdTime': '2022-04-28T23:19:50.331Z', 
        #   'queueInsertionTime': '1970-01-01T00:00:00.000Z', 
        #   'statusCode': 'RUNNING', 'status': 'RUNNING', 'runnerStatusCode': 'PENDING', 
        #   'duration': -1, 'location': {'host': None, 'port': -1, 'tlsPort': -1}, 
        #   'dataSource': 'w000', 'errorMsg': None}}
        self._status = self._tasks().task_status(self._id)
        self._state = self._status['status']['status']
        if self._state == consts.FAILED_STATE:
            self._error = self._status['status']['errorMsg']
        return self._status

    def done(self):
        """
        Reports if the query is done: succeeded or failed.
        """
        return self._state == consts.FAILED_STATE or self._state == consts.SUCCESS_STATE

    def succeeded(self):
        """
        Reports if the query succeeded.
        """
        return self._state == consts.SUCCESS_STATE

    def state(self):
        """
        Reports the engine-specific query state.

        Updated after each call to status().
        """
        return self._state

    def error(self):
        return self._error

    def error_msg(self):
        err = self.error()
        if err is None:
            return "unknown"
        if type(err) is str:
            return err
        msg = err.get("error")
        text = err.get("errorMessage")
        if msg is None and text is None:
            return "unknown"
        if text is not None:
            text = text.replace('\\n', '\n')
        if msg is None:
            return text
        if text is None:
            return msg
        return msg + ": " + text

    def join(self):
        if not self.done():
            self.status()
            while not self.done():
                time.sleep(0.1)
                self.status()
        return self.succeeded()

    def check_valid(self):
        if self._id is None:
            raise ClientError("Operation is invalid on a failed query")

    def wait_done(self):
        if not self.join():
            raise DruidError("Query failed: " + self._error)

    def wait(self):
        self.wait_done()
        return self.rows()

    def reports(self) -> dict:
        self.check_valid()
        if self._reports is None:
            self.join()
            self._reports = self._tasks().task_reports(self._id)
        return self._reports

    def results(self):
        if self._results is None:
            rpts = self.reports()
            self._results = rpts['multiStageQuery']['payload']['results']
        return self._results

    def schema(self):
        if self._schema is None:
            results = self.results()
            sig = results['signature']
            sqlTypes = results['sqlTypeNames']
            size = len(sig)
            self._schema = []
            for i in range(size):
                self._schema.append(ColumnSchema(sig[i]['name'], sqlTypes[i], sig[i]['type']))
        return self._schema

    def rows(self):
        if self._rows is None:
            results = self.results()
            self._rows = results['results']
        return self._rows

    def show(self, non_null=False):
        data = self.rows()
        if non_null:
            data = filter_null_cols(data)
        disp = display.display.table()
        disp.headers([c.name for c in self.schema()])
        disp.show(data)

class QueryClient:
    
    def __init__(self, druid):
        self.druid_client = druid

    def rest_client(self):
        return self.druid_client.rest_client
        
    def _prepare_query(self, request):
        if request is None:
            raise ClientError("No query provided.")
        if type(request) == str:
            request = self.sql_request(request)
        if is_blank(request.sql):
            raise ClientError("No query provided.")
        if self.rest_client().trace:
            print(request.sql)
        query_obj = request.to_request()
        return (request, query_obj)

    def sql_query(self, request) -> SqlQueryResult:
        '''
        Submit a SQL query with control over the context, parameters and other
        options. Returns a response with either a detailed error message, or
        the rows and query ID.
        '''
        request, query_obj = self._prepare_query(request)
        r = self.rest_client().post_only_json(REQ_ROUTER_SQL, query_obj, headers=request.headers)
        return SqlQueryResult(request, r)

    def sql(self, sql, *args):
        if len(args) > 0:
            sql = sql.result_format(*args)
        resp = self.sql_query(sql)
        if resp.ok():
            return resp.rows()
        raise ClientError(resp.error_msg())

    def explain_sql(self, query):
        """
        Run an EXPLAIN PLAN FOR query for the given query.

        Returns
        -------
        An object with the plan JSON parsed into Python objects:
        plan: the query plan
        columns: column schema
        tables: dictionary of name/type pairs
        """
        if is_blank(query):
            raise ClientError("No query provided.")
        results = self.sql('EXPLAIN PLAN FOR ' + query)
        return results[0]
    
    def sql_request(self, sql):
        return SqlRequest(self, sql)

    def show(self, query):
        result = self.sql_query(query)
        if result.ok():
            result.show()
        else:
            display.display.show_error(result.error_msg())

    def task(self, request):
        request, query_obj = self._prepare_query(request)
        r = self.rest_client().post_only_json(REQ_ROUTER_SQL, query_obj, headers=request.headers)
        return QueryTaskResult(request, r)

    def _tables_query(self, schema):
        return self.sql_query('''
            SELECT TABLE_NAME AS TableName
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{}'
            ORDER BY TABLE_NAME
            '''.format(schema))

    def tables(self, schema=consts.DRUID_SCHEMA):
        return self._tables_query(schema).rows()
    
    def show_tables(self, schema=consts.DRUID_SCHEMA):
        self._tables_query(schema).show()

    def describe_table(self, part1, part2=None):
        if part2 is None:
            schema = consts.DRUID_SCHEMA
            table = part1
        else:
            schema = part1
            table = part2
        self.show('''
            SELECT 
              ORDINAL_POSITION AS "Position",
              COLUMN_NAME AS "Name",
              DATA_TYPE AS "Type"
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'
            ORDER BY ORDINAL_POSITION
            '''.format(schema, table))
