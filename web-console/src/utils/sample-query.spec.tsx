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

import { sampleDataToQuery } from './sample-query';

describe('sample-query', () => {
  it('works', () => {
    const result = QueryResult.fromRawResult(
      [
        ['__time', 'host', 'service', 'msg'],
        ['LONG', 'STRING', 'STRING', 'COMPLEX<json>'],
        ['TIMESTAMP', 'VARCHAR', 'VARCHAR', 'OTHER'],
        [
          '2022-02-01T00:00:00.000Z',
          'brokerA.internal',
          'broker',
          '{"type":"sys","swap/free":1223334,"swap/max":3223334}',
        ],
        [
          '2022-02-01T00:00:00.000Z',
          'brokerA.internal',
          'broker',
          '{"type":"query","time":1223,"bytes":2434234}',
        ],
      ],
      false,
      true,
      true,
      true,
    );

    expect(sampleDataToQuery(result).toString()).toEqual(sane`
      SELECT
        CAST(c0 AS TIMESTAMP) AS "__time",
        CAST(c1 AS VARCHAR) AS "host",
        CAST(c2 AS VARCHAR) AS "service",
        PARSE_JSON(c3) AS "msg"
      FROM (VALUES
      ('2022-02-01T00:00:00.000Z', 'brokerA.internal', 'broker', '"{\\"type\\":\\"sys\\",\\"swap/free\\":1223334,\\"swap/max\\":3223334}"'),
      ('2022-02-01T00:00:00.000Z', 'brokerA.internal', 'broker', '"{\\"type\\":\\"query\\",\\"time\\":1223,\\"bytes\\":2434234}"')) AS t ("c0", "c1", "c2", "c3")
    `);
  });
});
