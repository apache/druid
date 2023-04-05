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

import { Execution } from './execution';

/*
===== Query =====

REPLACE INTO "kttm-blank-lines" OVERWRITE ALL
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "agent_type"
FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json"]}',
    '{"type":"json"}'
  )
) EXTEND ("timestamp" VARCHAR, "agent_type" VARCHAR)
PARTITIONED BY DAY

===== Context =====

{
  "maxParseExceptions": 2,
  "maxNumTasks": 2
}
*/

export const EXECUTION_INGEST_ERROR = Execution.fromTaskPayloadAndReport(
  {
    task: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765',
    payload: {
      type: 'query_controller',
      id: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765',
      spec: {
        query: {
          queryType: 'scan',
          dataSource: {
            type: 'external',
            inputSource: {
              type: 'http',
              uris: ['https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json'],
            },
            inputFormat: {
              type: 'json',
              keepNullColumns: false,
              assumeNewlineDelimited: false,
              useJsonNodeReader: false,
            },
            signature: [
              { name: 'timestamp', type: 'STRING' },
              { name: 'agent_type', type: 'STRING' },
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
          legacy: false,
          context: {
            finalize: false,
            finalizeAggregations: false,
            groupByEnableMultiValueUnnesting: false,
            maxNumTasks: 2,
            maxParseExceptions: 2,
            queryId: '614dc100-a4b9-40a3-95ce-1227fa7ea765',
            scanSignature: '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
            sqlInsertSegmentGranularity: '"DAY"',
            sqlQueryId: '614dc100-a4b9-40a3-95ce-1227fa7ea765',
            sqlReplaceTimeChunks: 'all',
          },
          granularity: { type: 'all' },
        },
        columnMappings: [
          { queryColumn: 'v0', outputColumn: '__time' },
          { queryColumn: 'agent_type', outputColumn: 'agent_type' },
        ],
        destination: {
          type: 'dataSource',
          dataSource: 'kttm-blank-lines',
          segmentGranularity: 'DAY',
          replaceTimeChunks: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
        },
        assignmentStrategy: 'max',
        tuningConfig: { maxNumWorkers: 1, maxRowsInMemory: 100000, rowsPerSegment: 3000000 },
      },
      sqlQuery:
        'REPLACE INTO "kttm-blank-lines" OVERWRITE ALL\nSELECT\n  TIME_PARSE("timestamp") AS "__time",\n  "agent_type"\nFROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json"]}\',\n    \'{"type":"json"}\'\n  )\n) EXTEND ("timestamp" VARCHAR, "agent_type" VARCHAR)\nPARTITIONED BY DAY',
      sqlQueryContext: {
        maxParseExceptions: 2,
        finalizeAggregations: false,
        sqlQueryId: '614dc100-a4b9-40a3-95ce-1227fa7ea765',
        groupByEnableMultiValueUnnesting: false,
        sqlInsertSegmentGranularity: '"DAY"',
        maxNumTasks: 2,
        sqlReplaceTimeChunks: 'all',
        queryId: '614dc100-a4b9-40a3-95ce-1227fa7ea765',
      },
      sqlTypeNames: ['TIMESTAMP', 'VARCHAR'],
      context: { forceTimeChunkLock: true, useLineageBasedSegmentAllocation: true },
      groupId: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765',
      dataSource: 'kttm-blank-lines',
      resource: {
        availabilityGroup: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765',
        requiredCapacity: 1,
      },
    },
  },

  {
    multiStageQuery: {
      type: 'multiStageQuery',
      taskId: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765',
      payload: {
        status: {
          status: 'FAILED',
          errorReport: {
            taskId: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765-worker0_0',
            host: 'localhost',
            error: {
              errorCode: 'TooManyWarnings',
              maxWarnings: 2,
              rootErrorCode: 'CannotParseExternalData',
              errorMessage: 'Too many warnings of type CannotParseExternalData generated (max = 2)',
            },
          },
          warnings: [
            {
              taskId: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765-worker0_0',
              host: 'localhost:8101',
              stageNumber: 0,
              error: {
                errorCode: 'CannotParseExternalData',
                errorMessage:
                  'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 3, Line: 3)',
              },
              exceptionStackTrace:
                'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 3, Line: 3)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:79)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:74)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.next(CloseableIterator.java:108)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.next(CloseableIterator.java:52)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1$1.hasNext(ExternalInputSliceReader.java:182)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:70)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:248)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:175)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:164)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:140)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:75)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:801)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (String)""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4360)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4205)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3214)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3182)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:75)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 22 more\n',
            },
            {
              taskId: 'query-614dc100-a4b9-40a3-95ce-1227fa7ea765-worker0_0',
              host: 'localhost:8101',
              stageNumber: 0,
              error: {
                errorCode: 'CannotParseExternalData',
                errorMessage:
                  'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 6, Line: 7)',
              },
              exceptionStackTrace:
                'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 6, Line: 7)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:79)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:74)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.next(CloseableIterator.java:108)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.next(CloseableIterator.java:52)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1$1.hasNext(ExternalInputSliceReader.java:182)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:70)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:248)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:175)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:164)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:140)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:75)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:801)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (String)""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4360)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4205)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3214)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3182)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:75)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 22 more\n',
            },
          ],
          startTime: '2023-03-27T22:11:24.945Z',
          durationMs: 14106,
          pendingTasks: 0,
          runningTasks: 2,
        },
        stages: [
          {
            stageNumber: 0,
            definition: {
              id: '0f627be4-63b6-4249-ba3d-71cd4a78faa2_0',
              input: [
                {
                  type: 'external',
                  inputSource: {
                    type: 'http',
                    uris: [
                      'https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json',
                    ],
                  },
                  inputFormat: {
                    type: 'json',
                    keepNullColumns: false,
                    assumeNewlineDelimited: false,
                    useJsonNodeReader: false,
                  },
                  signature: [
                    { name: 'timestamp', type: 'STRING' },
                    { name: 'agent_type', type: 'STRING' },
                  ],
                },
              ],
              processor: {
                type: 'scan',
                query: {
                  queryType: 'scan',
                  dataSource: { type: 'inputNumber', inputNumber: 0 },
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
                  legacy: false,
                  context: {
                    __timeColumn: 'v0',
                    finalize: false,
                    finalizeAggregations: false,
                    groupByEnableMultiValueUnnesting: false,
                    maxNumTasks: 2,
                    maxParseExceptions: 2,
                    queryId: '614dc100-a4b9-40a3-95ce-1227fa7ea765',
                    scanSignature:
                      '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
                    sqlInsertSegmentGranularity: '"DAY"',
                    sqlQueryId: '614dc100-a4b9-40a3-95ce-1227fa7ea765',
                    sqlReplaceTimeChunks: 'all',
                  },
                  granularity: { type: 'all' },
                },
              },
              signature: [
                { name: '__bucket', type: 'LONG' },
                { name: '__boost', type: 'LONG' },
                { name: 'agent_type', type: 'STRING' },
                { name: 'v0', type: 'LONG' },
              ],
              shuffleSpec: {
                type: 'targetSize',
                clusterBy: {
                  columns: [
                    { columnName: '__bucket', order: 'ASCENDING' },
                    { columnName: '__boost', order: 'ASCENDING' },
                  ],
                  bucketByCount: 1,
                },
                targetSize: 3000000,
              },
              maxWorkerCount: 1,
              shuffleCheckHasMultipleValues: true,
              maxInputBytesPerWorker: 10737418240,
            },
            phase: 'FAILED',
            workerCount: 1,
            startTime: '2023-03-27T22:11:25.310Z',
            duration: 13741,
            sort: true,
          },
          {
            stageNumber: 1,
            definition: {
              id: '0f627be4-63b6-4249-ba3d-71cd4a78faa2_1',
              input: [{ type: 'stage', stage: 0 }],
              processor: {
                type: 'segmentGenerator',
                dataSchema: {
                  dataSource: 'kttm-blank-lines',
                  timestampSpec: { column: '__time', format: 'millis', missingValue: null },
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
                    queryGranularity: { type: 'none' },
                    rollup: false,
                    intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
                  },
                  transformSpec: { filter: null, transforms: [] },
                },
                columnMappings: [
                  { queryColumn: 'v0', outputColumn: '__time' },
                  { queryColumn: 'agent_type', outputColumn: 'agent_type' },
                ],
                tuningConfig: {
                  maxNumWorkers: 1,
                  maxRowsInMemory: 100000,
                  rowsPerSegment: 3000000,
                },
              },
              signature: [],
              maxWorkerCount: 1,
              maxInputBytesPerWorker: 10737418240,
            },
          },
        ],
        counters: {
          '0': {
            '0': {
              input0: { type: 'channel', rows: [10], bytes: [7658], files: [1], totalFiles: [1] },
              output: { type: 'channel', rows: [10], bytes: [712], frames: [1] },
              sortProgress: {
                type: 'sortProgress',
                totalMergingLevels: 3,
                levelToTotalBatches: { '0': 1, '1': 1, '2': -1 },
                levelToMergedBatches: {},
                totalMergersForUltimateLevel: -1,
                progressDigest: 0.0,
              },
              warnings: { type: 'warnings', CannotParseExternalData: 3 },
            },
          },
        },
      },
    },
  },
);
