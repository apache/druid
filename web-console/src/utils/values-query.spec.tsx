/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { QueryResult, sane } from 'druid-query-toolkit';

import { queryResultToValuesQuery } from './values-query';

describe('queryResultToValuesQuery', () => {
  it('works', () => {
    const result = QueryResult.fromRawResult(
      [
        ['__time', 'host', 'service', 'msg', 'language', 'nums', 'nulls'],
        ['LONG', 'STRING', 'STRING', 'COMPLEX<json>', 'ARRAY<STRING>', 'ARRAY<LONG>', 'STRING'],
        ['TIMESTAMP', 'VARCHAR', 'VARCHAR', 'OTHER', 'ARRAY', 'ARRAY', 'VARCHAR'],
        [
          '2022-02-01T00:00:00.000Z',
          'brokerA.internal',
          'broker',
          '{"type":"sys","swap/free":1223334,"swap/max":3223334}',
          ['es', 'es-419'],
          [1],
          null,
        ],
        [
          '2022-02-01T00:00:00.000Z',
          'brokerA.internal',
          'broker',
          '{"type":"query","time":1223,"bytes":2434234}',
          ['en', 'es', 'es-419'],
          [2, 3],
          null,
        ],
        [null, null, null, null, null, null, null],
      ],
      false,
      true,
      true,
      true,
    );

    expect(queryResultToValuesQuery(result).toString()).toEqual(sane`
      SELECT
        CAST("c1" AS TIMESTAMP) AS "__time",
        CAST("c2" AS VARCHAR) AS "host",
        CAST("c3" AS VARCHAR) AS "service",
        PARSE_JSON("c4") AS "msg",
        STRING_TO_ARRAY("c5", '<#>') AS "language",
        CAST(STRING_TO_ARRAY("c6", '<#>') AS BIGINT ARRAY) AS "nums",
        CAST(NULL AS VARCHAR) AS "nulls"
      FROM (
        VALUES
        ('2022-02-01T00:00:00.000Z', 'brokerA.internal', 'broker', '{"type":"sys","swap/free":1223334,"swap/max":3223334}', 'es<#>es-419', '1', NULL),
        ('2022-02-01T00:00:00.000Z', 'brokerA.internal', 'broker', '{"type":"query","time":1223,"bytes":2434234}', 'en<#>es<#>es-419', '2<#>3', NULL),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL)
      ) AS "t" ("c1", "c2", "c3", "c4", "c5", "c6", "c7")
    `);
  });
});
