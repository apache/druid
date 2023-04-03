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

class TestSqlClient(unittest.TestCase):
    '''
    Tests for MSQ-related query APIs and the rather-complex Python API support.
    At present, MSQ SELECT query support is experimental: much ad-hoc Python
    code is needed to work around current limitations.
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
    
    def test_msq(self):
        ds_client = self.druid.datasources
        ds_client.drop('myWiki', True)
        sql = '''
        REPLACE INTO "myWiki" OVERWRITE ALL
        SELECT
            TIME_PARSE("timestamp") AS "__time",
            namespace,
            page,
            channel,
            "user",
            countryName,
            CASE WHEN isRobot = 'true' THEN 1 ELSE 0 END AS isRobot,
            "added",
            "delta",
            CASE WHEN isNew = 'true' THEN 1 ELSE 0 END AS isNew,
            CAST("deltaBucket" AS DOUBLE) AS deltaBucket,
            "deleted"
        FROM TABLE(
            EXTERN(
                '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
                '{"type":"json"}',
                '[{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"timestamp","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"long"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
            )
        )
        PARTITIONED BY DAY
        CLUSTERED BY namespace, page
        '''
        task = self.client.task(sql)        
        self.assertTrue(task.ok)
        self.assertGreater(len(task.id), 10)
        self.assertFalse(task.done)
        self.assertFalse(task.succeeded)
        self.assertEqual(consts.RUNNING_STATE, task.state)
        self.assertIsNone(task.error)

        # Wait for completion, which does polling internally
        task.wait_until_done()
        self.assertTrue(task.done)
        self.assertTrue(task.succeeded)
        self.assertEqual(consts.SUCCESS_STATE, task.state)
        self.assertIsNone(task.error)

        reports = task.reports
        self.assertIs(dict, type(reports))

        self.client.wait_until_ready('myWiki')

        # Now that we have a table, run a query against it.
        sql = '''
        SELECT page, added from myWiki LIMIT 3
        '''
        task = self.client.task(sql)
        self.assertTrue(task.ok)
        task.wait_until_done()
        self.assertTrue(task.succeeded)
        rows = task.rows
        self.assertIs(list, type(rows))
        self.assertEqual(3, len(rows))
        self.assertEqual(2, len(rows[0]))
        schema = task.schema
        self.assertEqual(2, len(schema))
        self.assertIs(ColumnSchema, type(schema[0]))
        # ds_client.drop('myWiki', True)
