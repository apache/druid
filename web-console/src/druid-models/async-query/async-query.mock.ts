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

import type { AsyncStatusResponse } from './async-query';

/*
SELECT
  "channel",
  COUNT(*) AS "Count"
FROM "wikipedia"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 2
 */

export const SUCCESS_ASYNC_STATUS: AsyncStatusResponse = {
  queryId: 'query-ad84d20a-c331-4ee9-ac59-83024e369cf1',
  state: 'SUCCESS',
  createdAt: '2023-07-05T21:33:19.147Z',
  schema: [
    {
      name: 'channel',
      type: 'VARCHAR',
      nativeType: 'STRING',
    },
    {
      name: 'Count',
      type: 'BIGINT',
      nativeType: 'LONG',
    },
  ],
  durationMs: 29168,
  result: {
    numTotalRows: 2,
    totalSizeInBytes: 116,
    dataSource: '__query_select',
    sampleRecords: [
      ['#en.wikipedia', 6650],
      ['#sh.wikipedia', 3969],
    ],
    pages: [
      {
        numRows: 2,
        sizeInBytes: 116,
        id: 0,
      },
    ],
  },
};

/*
REPLACE INTO "k" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"local","filter":"blah.json_","baseDir":"/"}',
      '{"type":"json"}'
    )
  ) EXTEND ("timestamp" VARCHAR, "session" VARCHAR)
)
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "session"
FROM "ext"
PARTITIONED BY DAY
 */

export const FAILED_ASYNC_STATUS: AsyncStatusResponse = {
  queryId: 'query-36ea273a-bd6d-48de-b890-2d853d879bf8',
  state: 'FAILED',
  createdAt: '2023-07-05T21:40:39.986Z',
  durationMs: 11217,
  errorDetails: {
    error: 'druidException',
    errorCode: 'UnknownError',
    persona: 'USER',
    category: 'UNCATEGORIZED',
    errorMessage: 'java.io.UncheckedIOException: /',
    context: {
      message: 'java.io.UncheckedIOException: /',
    },
  },
};
