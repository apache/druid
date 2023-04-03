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
from .setup import setup, ROUTER_ENDPOINT
setup()

import druidapi
from druidapi import consts
from druidapi.sql import ColumnSchema
from druidapi.error import ClientError

class TestSqlClient(unittest.TestCase):
    '''
    Tests for interactive query methods. See msq_test for task-related tests,
    and catalog_test for catalog-related methods.
    '''

    def setUp(self) -> None:
        self.druid = druidapi.client(ROUTER_ENDPOINT)
        self.client = self.druid.sql

        # Wait for the server to come up so tests are repeatable
        self.druid.status.wait_until_ready()
        return super().setUp()
    
    def tearDown(self) -> None:
        self.druid.close()
        return super().tearDown()
    
    def test_rest_client(self):
        self.assertIs(self.druid.rest, self.client.rest_client)
    
    def test_simple_query(self):
        sql = '''
        SELECT *
        FROM INFORMATION_SCHEMA.SCHEMATA
        '''
        resp = self.client.sql_query(sql)
        self.assertTrue(resp.ok)
        self.assertEqual(consts.SQL_OBJECT, resp.result_format)
        self.assertIsNone(resp.error_message)
        self.assertIsNone(resp.error)
        self.assertGreater(len(resp.id), 10)
        json = resp.json
        self.assertIs(list, type(json))
        rows = resp.rows
        self.assertGreater(len(rows), 3)
        schema = resp.schema
        self.assertGreater(len(schema), 3)
        self.assertIs(ColumnSchema, type(schema[0]))

        # Do-it-yourself SqlQuery form
        count = len(rows)
        req = {'query': sql}
        resp = self.client.sql_query(req)
        self.assertEqual(count, len(resp.rows))

    def test_sql(self):
        sql = '''
        SELECT *
        FROM INFORMATION_SCHEMA.SCHEMATA
        '''
        rows = self.client.sql(sql)
        count = len(rows)
        self.assertGreater(count, 3)

        # Python-parameterized form. Often used internally.
        sql = '''
        SELECT *
        FROM {}.{}
        '''
        rows = self.client.sql(sql, 'INFORMATION_SCHEMA', 'SCHEMATA')
        self.assertEqual(count, len(rows))

    def test_parameters(self):
        sql = '''
        SELECT *
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = ?
        '''
        req = self.client.sql_request(sql)
        req.add_parameter('druid')
        sql_query = {
            'query': sql,
            'parameters': [
                {'type': consts.SQL_VARCHAR_TYPE, 'value': 'druid'}
            ],
            'resultFormat': consts.SQL_OBJECT
        }
        self.assertEqual(sql_query, req.to_request())
        resp = self.client.sql_query(req)
        self.assertEqual(1, len(resp.rows))

        # Test types
        req = self.client.sql_request(sql)
        req.add_parameter('foo')
        req.add_parameter(10)
        req.add_parameter(10.5)
        req.add_parameter(['a', 'b'])
        self.assertEqual(consts.SQL_VARCHAR_TYPE, req.params[0]['type'])
        self.assertEqual('foo', req.params[0]['value'])
        self.assertEqual(consts.SQL_BIGINT_TYPE, req.params[1]['type'])
        self.assertEqual(10, req.params[1]['value'])
        self.assertEqual(consts.SQL_DOUBLE_TYPE, req.params[2]['type'])
        self.assertEqual(10.5, req.params[2]['value'])
        self.assertEqual(consts.SQL_ARRAY_TYPE, req.params[3]['type'])
        self.assertEqual(['a', 'b'], req.params[3]['value'])

        # Druid doesn't allow null parameter valuse
        with self.assertRaises(ClientError):
            req.add_parameter(None)

        # Unsupported type
        with self.assertRaises(ClientError):
            req.add_parameter({})
            
        # Do-it-yourself SqlQuery form
        resp = self.client.sql_query(sql_query)
        self.assertEqual(1, len(resp.rows))
    
    def test_context(self):
        sql = '''
        SELECT *
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = 'druid'
        '''
        req = self.client.sql_request(sql)
        req.add_context('sqlQueryId', 'myID1')
        sql_query = {
            'query': sql,
            'context': {'sqlQueryId': 'myID1'},
            'resultFormat': consts.SQL_OBJECT
        }
        self.assertEqual(sql_query, req.to_request())
        resp = self.client.sql_query(req)
        self.assertEqual(1, len(resp.rows))
        self.assertEqual('myID1', resp.id)

        # Do-it-yourself SqlQuery form
        sql_query['context']['sqlQueryId'] = 'myID2'
        resp = self.client.sql_query(sql_query)
        self.assertEqual(1, len(resp.rows))
        self.assertEqual('myID2', resp.id)

        # Dictionary form
        req = self.client.sql_request(sql)
        req.with_context({'sqlQueryId': 'myID3'})
        resp = self.client.sql_query(req)
        self.assertEqual(1, len(resp.rows))
        self.assertEqual('myID3', resp.id)

    def test_explain(self):
        sql = '''
        SELECT *
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = 'druid'
        '''
        result = self.client.explain_sql(sql)
        plan = result['PLAN']
        self.assertNotEqual(-1, plan.find('BindableProject'))

    def test_metadata(self):
        rows = self.client.tables(consts.INFORMATION_SCHEMA)
        self.assertGreater(len(rows), 2)

        rows = self.client.schemas()
        self.assertGreater(len(rows),2)

        rows = self.client.table_schema(consts.SCHEMA_TABLE)
        self.assertGreater(len(rows), 5)
