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
from druidapi import consts
from druidapi.util import dict_get, split_table_name
from druidapi.error import DruidError, ClientError

REQ_SQL = consts.ROUTER_BASE + '/sql'
REQ_SQL_TASK = REQ_SQL + '/task'

class SqlRequest:

    def __init__(self, query_client, sql):
        self.query_client = query_client
        self.sql = sql
        self.context = None
        self.params = None
        self.header = False
        self.format = consts.SQL_OBJECT
        self.headers = None
        self.types = None
        self.sql_types = None

    def with_format(self, result_format):
        self.format = result_format
        return self

    def with_headers(self, sql_types=False, druidTypes=False):
        self.headers = True
        self.types = druidTypes
        self.sql_types = sql_types
        return self

    def with_context(self, context):
        if not self.context:
            self.context = context
        else:
            self.context.update(context)
        return self

    def add_context(self, key, value):
        return self.with_context({key: value})

    def with_parameters(self, params):
        '''
        Set the array of parameters. Parameters must each be a map of 'type'/'value' pairs:
        {'type': the_type, 'value': the_value}. The type must be a valid SQL type
        (in upper case). See the consts module for a list.
        '''
        for param in params:
            self.add_parameters(param)
        return self

    def add_parameter(self, value):
        '''
        Add one parameter value. Infers the type of the parameter from the Python type.
        '''
        if value is None:
            raise ClientError('Druid does not support null parameter values')
        data_type = None
        value_type = type(value)
        if value_type is str:
            data_type = consts.SQL_VARCHAR_TYPE
        elif value_type is int:
            data_type = consts.SQL_BIGINT_TYPE
        elif value_type is float:
            data_type = consts.SQL_DOUBLE_TYPE
        elif value_type is list:
            data_type = consts.SQL_ARRAY_TYPE
        else:
            raise ClientError('Unsupported value type')
        if not self.params:
            self.params = []
        self.params.append({'type': data_type, 'value': value})

    def response_header(self):
        self.header = True
        return self

    def request_headers(self, headers):
        self.headers = headers
        return self

    def to_common_format(self):
        self.header = False
        self.sql_types = False
        self.types = False
        self.format = consts.SQL_OBJECT
        return self

    def to_request(self):
        query_obj = {'query': self.sql}
        if self.context:
            query_obj['context'] = self.context
        if self.params:
            query_obj['parameters'] = self.params
        if self.header:
            query_obj['header'] = True
        if self.format:
            query_obj['resultFormat'] = self.format
        if self.sql_types is not None: # Note: boolean variable
            query_obj['sqlTypesHeader'] = self.sql_types
        if self.types is not None: # Note: boolean variable
            query_obj['typesHeader'] = self.types
        return query_obj

    def result_format(self):
        return self.format.lower()

    def run(self):
        return self.query_client.sql_query(self)

def request_from_sql_query(query_client, sql_query):
    try:
        req = SqlRequest(query_client, sql_query['query'])
    except KeyError:
        raise ClientError('A SqlRequest dictionary must have \'query\' set')
    req.context = sql_query.get('context')
    req.params = sql_query.get('parameters')
    req.header = sql_query.get('header')
    req.format = sql_query.get('resultFormat')
    req.format = consts.SQL_OBJECT if req.format is None else req.format
    req.sql_types = sql_query.get('sqlTypesHeader')
    req.types = sql_query.get('typesHeader')
    return req

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
    if not results:
        return []
    is_null = {}
    for key in results[0].keys():
        is_null[key] = True
    for row in results:
        for key, value in row.items():
            # The following is hack to check for null values, empty strings and numeric 0s.
            is_null[key] = not not value
    return is_null

def filter_null_cols(results):
    '''
    Filter columns from a Druid result set by removing all null-like
    columns. A column is considered null if all values for that column
    are null. A value is null if it is either a JSON null, an empty
    string, or a numeric 0. All rows are preserved, as is the order
    of the remaining columns.
    '''
    if not results:
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
        return '{{name={}, SQL type={}, Druid type={}}}'.format(self.name, self.sql_type, self.druid_type)

class SqlQueryResult:
    '''
    Response from a classic request/response query.
    '''

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
                if not self._error:
                    self._error = 'Failed with HTTP status {}'.format(response.status_code)
        try:
            self._id = self.http_response.headers['X-Druid-SQL-Query-Id']
        except KeyError:
            self._error = 'Query returned no query ID'

    @property
    def _druid(self):
        return self.request.query_client.druid_client

    @property
    def result_format(self):
        return self.request.result_format()

    @property
    def ok(self):
        '''
        Reports if the query succeeded.

        The query rows and schema are available only if ok is True.
        '''
        return is_response_ok(self.http_response)

    @property
    def error(self):
        '''
        If the query fails, returns the error, if any provided by Druid.
        '''
        if self.ok:
            return None
        if self._error:
            return self._error
        if not self.http_response:
            return { 'error': 'unknown'}
        if is_response_ok(self.http_response):
            return None
        return {'error': 'HTTP {}'.format(self.http_response.status_code)}

    @property
    def error_message(self):
        if self.ok:
            return None
        err = self.error
        if not err:
            return 'unknown'
        if type(err) is str:
            return err
        msg = err.get('error')
        text = err.get('errorMessage')
        if not msg and not text:
            return 'unknown'
        if not msg:
            return text
        if not text:
            return msg
        return msg + ': ' + text

    @property
    def id(self):
        '''
        Returns the unique identifier for the query.
        '''
        return self._id

    @property
    def non_null(self):
        if not self.ok:
            return None
        if self.result_format != consts.SQL_OBJECT:
            return None
        return filter_null_cols(self.rows)

    @property
    def as_array(self):
        if self.result_format == consts.SQL_OBJECT:
            rows = []
            for obj in self.rows:
                rows.append([v for v in obj.values()])
            return rows
        else:
            return self.rows

    @property
    def json(self):
        if not self.ok:
            return None
        if not self._json:
            self._json = self.http_response.json()
        return self._json

    @property
    def rows(self):
        '''
        Returns the rows of data for the query.

        Druid supports many data formats. The method makes its best
        attempt to map the format into an array of rows of some sort.
        '''
        if not self._rows:
            json = self.json
            if not json:
                return self.http_response.text
            self._rows = parse_rows(self.result_format, self.request.context, json)
        return self._rows

    @property
    def schema(self):
        '''
        Returns the data schema as a list of ColumnSchema objects.

        Druid supports many data formats; not all of which provide
        schema information. This method makes a best effort to
        extract the schema from the query results.
        '''
        if not self._schema:
            self._schema = parse_schema(self.result_format, self.request.context, self.json)
        return self._schema

    def _display(self, display):
        return self._druid.display if not display else display

    def show(self, non_null=False, display=None):
        display = self._display(display)
        if not self.ok:
            display.error(self.error_message)
            return
        data = None
        if non_null:
            data = self.non_null
        if not data:
            data = self.as_array
        if not data:
            display.alert('Query returned no results')
            return
        display.data_table(data, [c.name for c in self.schema])

    def show_schema(self, display=None):
        display = self._display(display)
        if not self.ok:
            display.error(self.error_message)
            return
        data = []
        for c in self.schema:
            data.append([c.name, c.sql_type, c.druid_type])
        if not data:
            display.alert('Query returned no schema')
            return
        display.data_table(data, ['Name', 'SQL Type', 'Druid Type'])

class QueryTaskResult:
    '''
    Response from an asynchronous MSQ query, which may be an ingestion or a retrieval
    query. Can monitor task progress and wait for the task to complete. For a SELECT query,
    obtains the rows from the task reports. There are no results for an ingestion query,
    just a success/failure status.

    Note that SELECT query support is preliminary. The result structure is subject to
    change. Use a version of the library that matches your version of Druid for best
    results with MSQ SELECT queries.
    '''

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
                if not self._error:
                    self._error = 'Failed with HTTP status {}'.format(response.status_code)
            return

        # Typical response:
        # {'taskId': '6f7b514a446d4edc9d26a24d4bd03ade_fd8e242b-7d93-431d-b65b-2a512116924c_bjdlojgj',
        # 'state': 'RUNNING'}
        self.response_obj = response.json()
        self._id = self.response_obj['taskId']
        self._state = self.response_obj['state']

    @property
    def ok(self):
        '''
        Reports if the query completed successfully or is still running.
        Use succeeded() to check if the task is done and successful.
        '''
        return not self._error

    @property
    def id(self):
        return self._id

    def _druid(self):
        return self._request.query_client.druid_client

    def _tasks(self):
        return self._druid().tasks

    @property
    def status(self):
        '''
        Polls Druid for an update on the query run status.
        '''
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

    @property
    def done(self):
        '''
        Reports whether the query is done. The query is done when the Overlord task
        that runs the query completes. A completed task is one with a status of either
        SUCCESS or FAILED.
        '''
        return self._state == consts.FAILED_STATE or self._state == consts.SUCCESS_STATE

    @property
    def succeeded(self):
        '''
        Reports if the query succeeded.
        '''
        return self._state == consts.SUCCESS_STATE

    @property
    def state(self):
        '''
        Reports the task state from the Overlord task.

        Updated after each call to status().
        '''
        return self._state

    @property
    def error(self):
        return self._error

    @property
    def error_message(self):
        err = self.error()
        if not err:
            return 'unknown'
        if type(err) is str:
            return err
        msg = dict_get(err, 'error')
        text = dict_get(err, 'errorMessage')
        if not msg and not text:
            return 'unknown'
        if text:
            text = text.replace('\\n', '\n')
        if not msg:
            return text
        if not text:
            return msg
        return msg + ': ' + text

    def join(self):
        '''
        Wait for the task to complete, if still running. Returns at task
        completion: success or failure.

        Returns True for success, False for failure.
        '''
        if not self.done:
            self.status
            while not self.done:
                time.sleep(0.5)
                self.status
        return self.succeeded

    def check_valid(self):
        if not self._id:
            raise ClientError('Operation is invalid on a failed query')

    def wait_until_done(self):
        '''
        Wait for the task to complete. Raises an error if the task fails.
        A caller can proceed to do something with the successful result
        once this method returns without raising an error.
        '''
        if not self.join():
            raise DruidError('Query failed: ' + self.error_message())

    def wait(self):
        '''
        Wait for a SELECT query to finish running, then returns the rows from the query.
        '''
        self.wait_until_done()
        return self.rows

    @property
    def reports(self) -> dict:
        self.check_valid()
        if not self._reports:
            self.join()
            self._reports = self._tasks().task_reports(self._id)
        return self._reports

    def reports_no_wait(self) -> dict:
        return self._tasks().task_reports(self._id, require_ok=False)

    @property
    def results(self):
        if not self._results:
            rpts = self.reports()
            self._results = rpts['multiStageQuery']['payload']['results']
        return self._results

    @property
    def schema(self):
        if not self._schema:
            results = self.results
            sig = results['signature']
            sql_types = results['sqlTypeNames']
            size = len(sig)
            self._schema = []
            for i in range(size):
                self._schema.append(ColumnSchema(sig[i]['name'], sql_types[i], sig[i]['type']))
        return self._schema

    @property
    def rows(self):
        if not self._rows:
            results = self.results
            self._rows = results['results']
        return self._rows

    def _display(self, display):
        return self._druid().display if not display else display

    def show(self, non_null=False, display=None):
        display = self._display(display)
        if not self.done:
            display.alert('Task has not finished running')
            return
        if not self.succeeded:
            display.error(self.error_message)
            return
        data = self.rows
        if non_null:
            data = filter_null_cols(data)
        if not data:
            display.alert('Query returned no {}rows'.format("visible " if non_null else ''))
            return
        display.data_table(data, [c.name for c in self.schema])

class QueryClient:

    def __init__(self, druid, rest_client=None):
        self.druid_client = druid
        self._rest_client = druid.rest_client if not rest_client else rest_client

    @property
    def rest_client(self):
        return self._rest_client

    def _prepare_query(self, request):
        if not request:
            raise ClientError('No query provided.')
        # If the request is a dictionary, assume it is already in SqlQuery form.
        query_obj = None
        if type(request) == dict:
            query_obj = request
            request = request_from_sql_query(self, request)
        elif type(request) == str:
            request = self.sql_request(request)
        if not request.sql:
            raise ClientError('No query provided.')
        if self.rest_client.trace:
            print(request.sql)
        if not query_obj:
            query_obj = request.to_request()
        return (request, query_obj)

    def sql_query(self, request) -> SqlQueryResult:
        '''
        Submits a SQL query with control over the context, parameters and other
        options. Returns a response with either a detailed error message, or
        the rows and query ID.

        Parameters
        ----------
        request: str | SqlRequest | dict
            If a string, then gives the SQL query to execute.

            Can also be a `SqlRequest`, obtained from the
            'sql_request()` method, with optional query context, query parameters or
            other options.

            Can also be a dictionary that represents a `SqlQuery` object. The
            `SqlRequest` is a convenient wrapper to generate a `SqlQuery`.

        Note that some of the Druid SqlQuery options will return data in a format
        that this library cannot parse. In that case, obtain the raw payload from
        the response and avoid using the rows() and schema() methods.

        Returns
        -------
        A SqlQueryResult object that provides either the error message for a failed query,
        or the results of a successul query. The object provides access to the schema and
        rows if data is requested in a supported format. The default request object sets the
        options to return data in the required format.
        '''
        request, query_obj = self._prepare_query(request)
        r = self.rest_client.post_only_json(REQ_SQL, query_obj, headers=request.headers)
        return SqlQueryResult(request, r)

    def sql(self, sql, *args) -> list:
        '''
        Run a SQL query and return the results. Typically used to receive data as part
        of another operation, rathre than to display results to the user.

        Parameters
        ----------
        sql: str
            The SQL statement with optional Python `{}` parameters.

        args: list[str], Default = None
            Array of values to insert into the parameters.
        '''
        if len(args) > 0:
            sql = sql.format(*args)
        resp = self.sql_query(sql)
        if resp.ok:
            return resp.rows
        raise ClientError(resp.error_message)

    def explain_sql(self, query):
        '''
        Runs an EXPLAIN PLAN FOR query for the given query.

        Returns
        -------
        An object with the plan JSON parsed into Python objects:
        plan: the query plan
        columns: column schema
        tables: dictionary of name/type pairs
        '''
        if not query:
            raise ClientError('No query provided.')
        results = self.sql('EXPLAIN PLAN FOR ' + query)
        return results[0]

    def sql_request(self, sql) -> SqlRequest:
        '''
        Creates a SqlRequest object for the given SQL query text.
        '''
        return SqlRequest(self, sql)

    def task(self, query) -> QueryTaskResult:
        '''
        Submits an MSQ query. Returns a QueryTaskResult to track the task.

        Parameters
        ----------
        query
            The query as either a string or a SqlRequest object.
        '''
        request, query_obj = self._prepare_query(query)
        r = self.rest_client.post_only_json(REQ_SQL_TASK, query_obj, headers=request.headers)
        return QueryTaskResult(request, r)

    def run_task(self, query):
        '''
        Submits an MSQ query and wait for completion. Returns a QueryTaskResult to track the task.

        Parameters
        ----------
        query
            The query as either a string or a SqlRequest object.
        '''
        resp = self.task(query)
        if not resp.ok:
            raise ClientError(resp.error_message)
        resp.wait_until_done()

    def _tables_query(self, schema):
        return self.sql_query('''
            SELECT TABLE_NAME AS TableName
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{}'
            ORDER BY TABLE_NAME
            '''.format(schema))

    def tables(self, schema=consts.DRUID_SCHEMA):
        '''
        Returns a list of tables in the given schema.

        Parameters
        ----------
        schema
            The schema to query, `druid` by default.
        '''
        return self._tables_query(schema).rows

    def _schemas_query(self):
        return self.sql_query('''
            SELECT SCHEMA_NAME AS SchemaName
            FROM INFORMATION_SCHEMA.SCHEMATA
            ORDER BY SCHEMA_NAME
            ''')

    def schemas(self):
        return self._schemas_query().rows

    def _schema_query(self, table_name):
        parts = split_table_name(table_name, consts.DRUID_SCHEMA)
        return self.sql_query('''
            SELECT
              ORDINAL_POSITION AS "Position",
              COLUMN_NAME AS "Name",
              DATA_TYPE AS "Type"
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{}'
              AND TABLE_NAME = '{}'
            ORDER BY ORDINAL_POSITION
            '''.format(parts[0], parts[1]))

    def table_schema(self, table_name):
        '''
        Returns the schema of a table as an array of dictionaries of the
        form {"Position": "<n>", "Name": "<name>", "Type": "<type>"}

        Parameters
        ----------
        table_name: str
            The name of the table as either "table" or "schema.table".
            If the form is "table", then the 'druid' schema is assumed.
        '''
        return self._schema_query(table_name).rows

    def _function_args_query(self, table_name):
        parts = split_table_name(table_name, consts.EXT_SCHEMA)
        return self.sql_query('''
            SELECT
              ORDINAL_POSITION AS "Position",
              PARAMETER_NAME AS "Parameter",
              DATA_TYPE AS "Type",
              IS_OPTIONAL AS "Optional"
            FROM INFORMATION_SCHEMA.PARAMETERS
            WHERE SCHEMA_NAME = '{}'
              AND FUNCTION_NAME = '{}'
            ORDER BY ORDINAL_POSITION
            '''.format(parts[0], parts[1]))

    def function_parameters(self,  table_name):
        '''
        Retruns the list of parameters for a partial external table defined in
        the Druid catalog. Returns the parameters as an array of objects in the
        form {"Position": <n>, "Parameter": "<name>", "Type": "<type>",
              "Optional": True|False}

        Parameters
        ----------
        table_name str
            The name of the table as either "table" or "schema.table".
            If the form is "table", then the 'ext' schema is assumed.
        '''
        return self._function_args_query(table_name).rows

    def wait_until_ready(self, table_name, verify_load_status=True):
        '''
        Waits for a datasource to be loaded in the cluster, and to become available to SQL.

        Parameters
        ----------
        table_name str
            The name of a datasource in the 'druid' schema.
        verify_load_status
            If true, checks whether all published segments are loaded before testing query.
            If false, tries the test query before checking whether all published segments are loaded.
        '''
        if verify_load_status:
            self.druid_client.datasources.wait_until_ready(table_name)
        while True:
            try:
                self.sql('SELECT 1 FROM "{}" LIMIT 1'.format(table_name));
                return
            except Exception:
                time.sleep(0.5)
