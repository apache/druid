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
  queryId: 'query-35fe5ca2-ffb6-41a0-8f30-93694aa55fcc',
  state: 'RUNNING',
  createdAt: '2024-07-26T11:37:23.765Z',
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
  durationMs: -1,
  stages: [
    {
      stageNumber: 0,
      definition: {
        id: 'query-35fe5ca2-ffb6-41a0-8f30-93694aa55fcc_0',
        input: [
          {
            type: 'table',
            dataSource: 'wikipedia',
            intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
          },
        ],
        processor: {
          type: 'groupByPreShuffle',
          query: {
            queryType: 'groupBy',
            dataSource: {
              type: 'inputNumber',
              inputNumber: 0,
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            granularity: {
              type: 'all',
            },
            dimensions: [
              {
                type: 'default',
                dimension: 'channel',
                outputName: 'd0',
                outputType: 'STRING',
              },
            ],
            aggregations: [
              {
                type: 'count',
                name: 'a0',
              },
            ],
            limitSpec: {
              type: 'default',
              columns: [
                {
                  dimension: 'a0',
                  direction: 'descending',
                  dimensionOrder: {
                    type: 'numeric',
                  },
                },
              ],
              limit: 2,
            },
            context: {
              __resultFormat: 'array',
              __user: 'allowAll',
              executionMode: 'async',
              finalize: true,
              maxNumTasks: 8,
              maxParseExceptions: 0,
              queryId: '35fe5ca2-ffb6-41a0-8f30-93694aa55fcc',
              sqlOuterLimit: 1001,
              sqlQueryId: '35fe5ca2-ffb6-41a0-8f30-93694aa55fcc',
              sqlStringifyArrays: false,
            },
          },
        },
        signature: [
          {
            name: 'd0',
            type: 'STRING',
          },
          {
            name: 'a0',
            type: 'LONG',
          },
        ],
        shuffleSpec: {
          type: 'maxCount',
          clusterBy: {
            columns: [
              {
                columnName: 'd0',
                order: 'ASCENDING',
              },
            ],
          },
          partitions: 7,
          aggregate: true,
        },
        maxWorkerCount: 7,
      },
      shuffle: 'globalSort',
      startTime: '2024-07-26T11:37:25.849Z',
      duration: 2406,
      sort: true,
    },
    {
      stageNumber: 1,
      definition: {
        id: 'query-35fe5ca2-ffb6-41a0-8f30-93694aa55fcc_1',
        input: [
          {
            type: 'stage',
            stage: 0,
          },
        ],
        processor: {
          type: 'groupByPostShuffle',
          query: {
            queryType: 'groupBy',
            dataSource: {
              type: 'inputNumber',
              inputNumber: 0,
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            granularity: {
              type: 'all',
            },
            dimensions: [
              {
                type: 'default',
                dimension: 'channel',
                outputName: 'd0',
                outputType: 'STRING',
              },
            ],
            aggregations: [
              {
                type: 'count',
                name: 'a0',
              },
            ],
            limitSpec: {
              type: 'default',
              columns: [
                {
                  dimension: 'a0',
                  direction: 'descending',
                  dimensionOrder: {
                    type: 'numeric',
                  },
                },
              ],
              limit: 2,
            },
            context: {
              __resultFormat: 'array',
              __user: 'allowAll',
              executionMode: 'async',
              finalize: true,
              maxNumTasks: 8,
              maxParseExceptions: 0,
              queryId: '35fe5ca2-ffb6-41a0-8f30-93694aa55fcc',
              sqlOuterLimit: 1001,
              sqlQueryId: '35fe5ca2-ffb6-41a0-8f30-93694aa55fcc',
              sqlStringifyArrays: false,
            },
          },
        },
        signature: [
          {
            name: 'a0',
            type: 'LONG',
          },
          {
            name: '__boost',
            type: 'LONG',
          },
          {
            name: 'd0',
            type: 'STRING',
          },
        ],
        shuffleSpec: {
          type: 'maxCount',
          clusterBy: {
            columns: [
              {
                columnName: 'a0',
                order: 'DESCENDING',
              },
              {
                columnName: '__boost',
                order: 'ASCENDING',
              },
            ],
          },
          partitions: 1,
        },
        maxWorkerCount: 7,
      },
      shuffle: 'globalSort',
      sort: true,
    },
    {
      stageNumber: 2,
      definition: {
        id: 'query-35fe5ca2-ffb6-41a0-8f30-93694aa55fcc_2',
        input: [
          {
            type: 'stage',
            stage: 1,
          },
        ],
        processor: {
          type: 'limit',
          limit: 2,
        },
        signature: [
          {
            name: 'a0',
            type: 'LONG',
          },
          {
            name: '__boost',
            type: 'LONG',
          },
          {
            name: 'd0',
            type: 'STRING',
          },
        ],
        shuffleSpec: {
          type: 'maxCount',
          clusterBy: {
            columns: [
              {
                columnName: 'a0',
                order: 'DESCENDING',
              },
              {
                columnName: '__boost',
                order: 'ASCENDING',
              },
            ],
          },
          partitions: 1,
        },
        maxWorkerCount: 1,
      },
      shuffle: 'globalSort',
      sort: true,
    },
  ],
  counters: {},
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
