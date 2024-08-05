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
  queryId: 'query-45f1dafd-8a52-4eb7-9a6c-77840cddd349',
  state: 'SUCCESS',
  createdAt: '2024-07-27T02:39:22.230Z',
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
  durationMs: 7183,
  result: {
    numTotalRows: 2,
    totalSizeInBytes: 150,
    dataSource: '__query_select',
    sampleRecords: [
      ['#en.wikipedia', 6650],
      ['#sh.wikipedia', 3969],
    ],
    pages: [
      {
        id: 0,
        numRows: 2,
        sizeInBytes: 150,
      },
    ],
  },
  stages: [
    {
      stageNumber: 0,
      definition: {
        id: 'query-45f1dafd-8a52-4eb7-9a6c-77840cddd349_0',
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
              maxNumTasks: 2,
              maxParseExceptions: 0,
              queryId: '45f1dafd-8a52-4eb7-9a6c-77840cddd349',
              sqlOuterLimit: 1001,
              sqlQueryId: '45f1dafd-8a52-4eb7-9a6c-77840cddd349',
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
          partitions: 1,
          aggregate: true,
        },
        maxWorkerCount: 1,
      },
      phase: 'FINISHED',
      workerCount: 1,
      partitionCount: 1,
      shuffle: 'globalSort',
      output: 'localStorage',
      startTime: '2024-07-27T02:39:24.713Z',
      duration: 3384,
      sort: true,
    },
    {
      stageNumber: 1,
      definition: {
        id: 'query-45f1dafd-8a52-4eb7-9a6c-77840cddd349_1',
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
              maxNumTasks: 2,
              maxParseExceptions: 0,
              queryId: '45f1dafd-8a52-4eb7-9a6c-77840cddd349',
              sqlOuterLimit: 1001,
              sqlQueryId: '45f1dafd-8a52-4eb7-9a6c-77840cddd349',
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
        maxWorkerCount: 1,
      },
      phase: 'FINISHED',
      workerCount: 1,
      partitionCount: 1,
      shuffle: 'globalSort',
      output: 'localStorage',
      startTime: '2024-07-27T02:39:28.089Z',
      duration: 26,
      sort: true,
    },
    {
      stageNumber: 2,
      definition: {
        id: 'query-45f1dafd-8a52-4eb7-9a6c-77840cddd349_2',
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
      phase: 'FINISHED',
      workerCount: 1,
      partitionCount: 1,
      shuffle: 'globalSort',
      output: 'localStorage',
      startTime: '2024-07-27T02:39:28.112Z',
      duration: 12,
      sort: true,
    },
  ],
  counters: {
    '0': {
      '0': {
        input0: {
          type: 'channel',
          rows: [24433],
          bytes: [6525055],
          files: [1],
          totalFiles: [1],
        },
        output: {
          type: 'channel',
          rows: [51],
          bytes: [2335],
          frames: [1],
        },
        shuffle: {
          type: 'channel',
          rows: [51],
          bytes: [2131],
          frames: [1],
        },
        sortProgress: {
          type: 'sortProgress',
          totalMergingLevels: 3,
          levelToTotalBatches: {
            '0': 1,
            '1': 1,
            '2': 1,
          },
          levelToMergedBatches: {
            '0': 1,
            '1': 1,
            '2': 1,
          },
          totalMergersForUltimateLevel: 1,
          progressDigest: 1.0,
        },
      },
    },
    '1': {
      '0': {
        input0: {
          type: 'channel',
          rows: [51],
          bytes: [2131],
          frames: [1],
        },
        output: {
          type: 'channel',
          rows: [51],
          bytes: [2998],
          frames: [1],
        },
        shuffle: {
          type: 'channel',
          rows: [51],
          bytes: [2794],
          frames: [1],
        },
        sortProgress: {
          type: 'sortProgress',
          totalMergingLevels: 3,
          levelToTotalBatches: {
            '0': 1,
            '1': 1,
            '2': 1,
          },
          levelToMergedBatches: {
            '0': 1,
            '1': 1,
            '2': 1,
          },
          totalMergersForUltimateLevel: 1,
          progressDigest: 1.0,
        },
      },
    },
    '2': {
      '0': {
        input0: {
          type: 'channel',
          rows: [51],
          bytes: [2794],
          frames: [1],
        },
        output: {
          type: 'channel',
          rows: [2],
          bytes: [150],
          frames: [1],
        },
        shuffle: {
          type: 'channel',
          rows: [2],
          bytes: [142],
          frames: [1],
        },
        sortProgress: {
          type: 'sortProgress',
          totalMergingLevels: 3,
          levelToTotalBatches: {
            '0': 1,
            '1': 1,
            '2': 1,
          },
          levelToMergedBatches: {
            '0': 1,
            '1': 1,
            '2': 1,
          },
          totalMergersForUltimateLevel: 1,
          progressDigest: 1.0,
        },
      },
    },
  },
  warnings: [],
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
  queryId: 'query-ea3e36df-ad67-4870-b136-f5616b17d9c4',
  state: 'FAILED',
  createdAt: '2024-07-26T18:04:59.873Z',
  durationMs: 6954,
  errorDetails: {
    error: 'druidException',
    errorCode: 'TooManyWarnings',
    persona: 'USER',
    category: 'UNCATEGORIZED',
    errorMessage: 'Too many warnings of type CannotParseExternalData generated (max = 2)',
    context: {
      maxWarnings: '2',
      rootErrorCode: 'CannotParseExternalData',
    },
  },
  stages: [
    {
      stageNumber: 0,
      definition: {
        id: 'query-ea3e36df-ad67-4870-b136-f5616b17d9c4_0',
        input: [
          {
            type: 'external',
            inputSource: {
              type: 'http',
              uris: ['https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json'],
            },
            inputFormat: {
              type: 'json',
            },
            signature: [
              {
                name: 'timestamp',
                type: 'STRING',
              },
              {
                name: 'agent_type',
                type: 'STRING',
              },
            ],
          },
        ],
        processor: {
          type: 'scan',
          query: {
            queryType: 'scan',
            dataSource: {
              type: 'external',
              inputSource: {
                type: 'http',
                uris: [
                  'https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json',
                ],
              },
              inputFormat: {
                type: 'json',
              },
              signature: [
                {
                  name: 'timestamp',
                  type: 'STRING',
                },
                {
                  name: 'agent_type',
                  type: 'STRING',
                },
              ],
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            virtualColumns: [
              {
                type: 'expression',
                name: 'v0',
                expression: 'timestamp_parse("timestamp",null,\'UTC\')',
                outputType: 'LONG',
              },
            ],
            resultFormat: 'compactedList',
            columns: ['agent_type', 'v0'],
            context: {
              __resultFormat: 'array',
              __timeColumn: 'v0',
              __user: 'allowAll',
              executionMode: 'async',
              finalize: false,
              finalizeAggregations: false,
              groupByEnableMultiValueUnnesting: false,
              maxNumTasks: 2,
              maxParseExceptions: 2,
              queryId: 'ea3e36df-ad67-4870-b136-f5616b17d9c4',
              scanSignature: '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
              sqlInsertSegmentGranularity: '"DAY"',
              sqlQueryId: 'ea3e36df-ad67-4870-b136-f5616b17d9c4',
              sqlReplaceTimeChunks: 'all',
              sqlStringifyArrays: false,
              waitUntilSegmentsLoad: true,
            },
            columnTypes: ['STRING', 'LONG'],
            granularity: {
              type: 'all',
            },
            legacy: false,
          },
        },
        signature: [
          {
            name: '__bucket',
            type: 'LONG',
          },
          {
            name: '__boost',
            type: 'LONG',
          },
          {
            name: 'agent_type',
            type: 'STRING',
          },
          {
            name: 'v0',
            type: 'LONG',
          },
        ],
        shuffleSpec: {
          type: 'targetSize',
          clusterBy: {
            columns: [
              {
                columnName: '__bucket',
                order: 'ASCENDING',
              },
              {
                columnName: '__boost',
                order: 'ASCENDING',
              },
            ],
            bucketByCount: 1,
          },
          targetSize: 3000000,
        },
        maxWorkerCount: 1,
        shuffleCheckHasMultipleValues: true,
      },
      phase: 'FAILED',
      workerCount: 1,
      shuffle: 'globalSort',
      output: 'localStorage',
      startTime: '2024-07-26T18:05:02.399Z',
      duration: 4056,
      sort: true,
    },
    {
      stageNumber: 1,
      definition: {
        id: 'query-ea3e36df-ad67-4870-b136-f5616b17d9c4_1',
        input: [
          {
            type: 'stage',
            stage: 0,
          },
        ],
        processor: {
          type: 'segmentGenerator',
          dataSchema: {
            dataSource: 'kttm-blank-lines',
            timestampSpec: {
              column: '__time',
              format: 'millis',
              missingValue: null,
            },
            dimensionsSpec: {
              dimensions: [
                {
                  type: 'string',
                  name: 'agent_type',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
              ],
              dimensionExclusions: ['__time'],
              includeAllDimensions: false,
              useSchemaDiscovery: false,
            },
            metricsSpec: [],
            granularitySpec: {
              type: 'arbitrary',
              queryGranularity: {
                type: 'none',
              },
              rollup: false,
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            transformSpec: {
              filter: null,
              transforms: [],
            },
          },
          columnMappings: [
            {
              queryColumn: 'v0',
              outputColumn: '__time',
            },
            {
              queryColumn: 'agent_type',
              outputColumn: 'agent_type',
            },
          ],
          tuningConfig: {
            maxNumWorkers: 1,
            maxRowsInMemory: 100000,
            rowsPerSegment: 3000000,
          },
        },
        signature: [],
        maxWorkerCount: 1,
      },
    },
  ],
  counters: {
    '0': {
      '0': {
        input0: {
          type: 'channel',
          rows: [10],
          bytes: [7658],
          files: [1],
          totalFiles: [1],
        },
        output: {
          type: 'channel',
          rows: [10],
          bytes: [712],
          frames: [1],
        },
        sortProgress: {
          type: 'sortProgress',
          totalMergingLevels: 3,
          levelToTotalBatches: {
            '0': 1,
            '1': 1,
            '2': -1,
          },
          levelToMergedBatches: {},
          totalMergersForUltimateLevel: -1,
          progressDigest: 0.0,
        },
        warnings: {
          type: 'warnings',
          CannotParseExternalData: 3,
        },
      },
    },
  },
  warnings: [
    {
      taskId: 'query-ea3e36df-ad67-4870-b136-f5616b17d9c4-worker0_0',
      host: 'localhost:8101',
      stageNumber: 0,
      error: {
        errorCode: 'CannotParseExternalData',
        errorMessage:
          'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 3, Line: 3)',
      },
      exceptionStackTrace:
        'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 3, Line: 3)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:80)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:72)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.hasNext(CloseableIterator.java:93)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)\n\tat org.apache.druid.msq.input.external.ExternalSegment$1$1.hasNext(ExternalSegment.java:94)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:75)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:374)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeededWithExceptionHandling(ScanQueryFrameProcessor.java:334)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:273)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:88)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:157)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:75)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:230)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:138)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:838)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:259)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)\n\tat java.base/java.lang.Thread.run(Thread.java:840)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (byte[])""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4688)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4586)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3609)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:75)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 24 more\n',
    },
    {
      taskId: 'query-ea3e36df-ad67-4870-b136-f5616b17d9c4-worker0_0',
      host: 'localhost:8101',
      stageNumber: 0,
      error: {
        errorCode: 'CannotParseExternalData',
        errorMessage:
          'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 6, Line: 7)',
      },
      exceptionStackTrace:
        'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 6, Line: 7)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:80)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:72)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.hasNext(CloseableIterator.java:93)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)\n\tat org.apache.druid.msq.input.external.ExternalSegment$1$1.hasNext(ExternalSegment.java:94)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:75)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:374)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeededWithExceptionHandling(ScanQueryFrameProcessor.java:334)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:273)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:88)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:157)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:75)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:230)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:138)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:838)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:259)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)\n\tat java.base/java.lang.Thread.run(Thread.java:840)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (byte[])""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4688)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4586)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3609)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:75)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 24 more\n',
    },
  ],
};
