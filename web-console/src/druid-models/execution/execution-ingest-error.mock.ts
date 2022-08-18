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
For query:

REPLACE INTO "kttm_simple" OVERWRITE ALL
SELECT TIME_PARSE("timestamp") AS "__time", agent_type
FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_type","type":"string"}]'
  )
)
PARTITIONED BY ALL
*/

export const EXECUTION_INGEST_ERROR = Execution.fromTaskPayloadAndReport(
  {
    task: 'query-9f7d2aa1-72b8-49cf-9350-a1d04c539479',
    payload: {
      type: 'query_controller',
      id: 'query-9f7d2aa1-72b8-49cf-9350-a1d04c539479',
      spec: {
        query: {
          queryType: 'scan',
          dataSource: {
            type: 'external',
            inputSource: {
              type: 'http',
              uris: ['https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_'],
              httpAuthenticationUsername: null,
              httpAuthenticationPassword: null,
            },
            inputFormat: {
              type: 'json',
              flattenSpec: null,
              featureSpec: {},
              keepNullColumns: false,
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
            __timeColumn: 'v0',
            finalize: false,
            finalizeAggregations: false,
            groupByEnableMultiValueUnnesting: false,
            scanSignature: '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
            sqlInsertSegmentGranularity: '{"type":"all"}',
            sqlQueryId: '9f7d2aa1-72b8-49cf-9350-a1d04c539479',
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
          dataSource: 'kttm_simple',
          segmentGranularity: { type: 'all' },
          replaceTimeChunks: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
        },
        assignmentStrategy: 'max',
        tuningConfig: { maxNumWorkers: 1, maxRowsInMemory: 100000, rowsPerSegment: 3000000 },
      },
      sqlQuery:
        'REPLACE INTO "kttm_simple" OVERWRITE ALL\nSELECT TIME_PARSE("timestamp") AS "__time", agent_type\nFROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_"]}\',\n    \'{"type":"json"}\',\n    \'[{"name":"timestamp","type":"string"},{"name":"agent_type","type":"string"}]\'\n  )\n)\nPARTITIONED BY ALL',
      sqlQueryContext: {
        finalizeAggregations: false,
        groupByEnableMultiValueUnnesting: false,
        maxParseExceptions: 0,
        sqlInsertSegmentGranularity: '{"type":"all"}',
        sqlQueryId: '9f7d2aa1-72b8-49cf-9350-a1d04c539479',
        sqlReplaceTimeChunks: 'all',
      },
      sqlTypeNames: ['TIMESTAMP', 'VARCHAR'],
      context: { forceTimeChunkLock: true, useLineageBasedSegmentAllocation: true },
      groupId: 'query-9f7d2aa1-72b8-49cf-9350-a1d04c539479',
      dataSource: 'kttm_simple',
      resource: {
        availabilityGroup: 'query-9f7d2aa1-72b8-49cf-9350-a1d04c539479',
        requiredCapacity: 1,
      },
    },
  },

  {
    multiStageQuery: {
      taskId: 'query-9f7d2aa1-72b8-49cf-9350-a1d04c539479',
      payload: {
        status: {
          status: 'FAILED',
          startTime: '2022-08-18T05:32:31.128Z',
          durationMs: 18261,
          errorReport: {
            taskId: 'query-9f7d2aa1-72b8-49cf-9350-a1d04c539479-worker0',
            host: 'localhost:8101',
            stageNumber: 0,
            error: {
              errorCode: 'UnknownError',
              message:
                'java.lang.RuntimeException: java.lang.RuntimeException: Error occured while trying to read uri: https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_',
              errorMessage:
                'java.lang.RuntimeException: java.lang.RuntimeException: Error occured while trying to read uri: https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_',
            },
            exceptionStackTrace:
              'java.lang.RuntimeException: java.lang.RuntimeException: Error occured while trying to read uri: https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_\n\tat org.apache.druid.java.util.common.Either.valueOrThrow(Either.java:91)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:258)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:615)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: java.lang.RuntimeException: Error occured while trying to read uri: https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_\n\tat org.apache.druid.data.input.impl.InputEntityIteratingReader.lambda$read$0(InputEntityIteratingReader.java:83)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:84)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.<init>(CloseableIterator.java:69)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator.flatMap(CloseableIterator.java:67)\n\tat org.apache.druid.data.input.impl.InputEntityIteratingReader.createIterator(InputEntityIteratingReader.java:105)\n\tat org.apache.druid.data.input.impl.InputEntityIteratingReader.read(InputEntityIteratingReader.java:74)\n\tat org.apache.druid.msq.indexing.CountableInputSourceReader.read(CountableInputSourceReader.java:47)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1.make(ExternalInputSliceReader.java:162)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1.make(ExternalInputSliceReader.java:157)\n\tat org.apache.druid.java.util.common.guava.BaseSequence.toYielder(BaseSequence.java:66)\n\tat org.apache.druid.java.util.common.guava.Yielders.each(Yielders.java:32)\n\tat org.apache.druid.segment.RowWalker.<init>(RowWalker.java:48)\n\tat org.apache.druid.segment.RowBasedStorageAdapter.makeCursors(RowBasedStorageAdapter.java:175)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.makeCursors(ScanQueryFrameProcessor.java:307)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:166)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:111)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:148)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:70)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\t... 8 more\nCaused by: java.io.IOException: Server returned HTTP response code: 403 for URL: https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_\n\tat java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)\n\tat java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)\n\tat java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)\n\tat java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:490)\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection$10.run(HttpURLConnection.java:1974)\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection$10.run(HttpURLConnection.java:1969)\n\tat java.base/java.security.AccessController.doPrivileged(Native Method)\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection.getChainedException(HttpURLConnection.java:1968)\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream0(HttpURLConnection.java:1536)\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1520)\n\tat java.base/sun.net.www.protocol.https.HttpsURLConnectionImpl.getInputStream(HttpsURLConnectionImpl.java:250)\n\tat org.apache.druid.data.input.impl.HttpEntity.openInputStream(HttpEntity.java:108)\n\tat org.apache.druid.data.input.impl.HttpEntity.readFrom(HttpEntity.java:68)\n\tat org.apache.druid.data.input.RetryingInputEntity.readFromStart(RetryingInputEntity.java:60)\n\tat org.apache.druid.data.input.RetryingInputEntity$RetryingInputEntityOpenFunction.open(RetryingInputEntity.java:84)\n\tat org.apache.druid.data.input.RetryingInputEntity$RetryingInputEntityOpenFunction.open(RetryingInputEntity.java:79)\n\tat org.apache.druid.data.input.impl.RetryingInputStream.<init>(RetryingInputStream.java:78)\n\tat org.apache.druid.data.input.RetryingInputEntity.open(RetryingInputEntity.java:43)\n\tat org.apache.druid.data.input.TextReader.intermediateRowIteratorWithMetadata(TextReader.java:59)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader.read(IntermediateRowParsingReader.java:49)\n\tat org.apache.druid.data.input.impl.InputEntityIteratingReader.lambda$read$0(InputEntityIteratingReader.java:78)\n\t... 26 more\nCaused by: java.io.IOException: Server returned HTTP response code: 403 for URL: https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream0(HttpURLConnection.java:1924)\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1520)\n\tat java.base/sun.net.www.protocol.http.HttpURLConnection.getHeaderField(HttpURLConnection.java:3099)\n\tat java.base/sun.net.www.protocol.https.HttpsURLConnectionImpl.getHeaderField(HttpsURLConnectionImpl.java:287)\n\tat org.apache.druid.data.input.impl.HttpEntity.openInputStream(HttpEntity.java:96)\n\t... 35 more\n',
          },
          warningReports: [],
        },
        stages: [
          {
            stageNumber: 0,
            definition: {
              id: '3245124f-c886-40c0-90c3-7c43df57f7b0_0',
              input: [
                {
                  type: 'external',
                  inputSource: {
                    type: 'http',
                    uris: [
                      'https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz_',
                    ],
                    httpAuthenticationUsername: null,
                    httpAuthenticationPassword: null,
                  },
                  inputFormat: {
                    type: 'json',
                    flattenSpec: null,
                    featureSpec: {},
                    keepNullColumns: false,
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
                    scanSignature:
                      '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
                    sqlInsertSegmentGranularity: '{"type":"all"}',
                    sqlQueryId: '9f7d2aa1-72b8-49cf-9350-a1d04c539479',
                    sqlReplaceTimeChunks: 'all',
                  },
                  granularity: { type: 'all' },
                },
              },
              signature: [
                { name: '__boost', type: 'LONG' },
                { name: 'agent_type', type: 'STRING' },
                { name: 'v0', type: 'LONG' },
              ],
              shuffleSpec: {
                type: 'targetSize',
                clusterBy: { columns: [{ columnName: '__boost' }] },
                targetSize: 3000000,
              },
              maxWorkerCount: 1,
              shuffleCheckHasMultipleValues: true,
            },
            workerCount: 1,
            startTime: '2022-08-18T05:32:33.537Z',
            duration: 15852,
            sort: true,
          },
          {
            stageNumber: 1,
            definition: {
              id: '3245124f-c886-40c0-90c3-7c43df57f7b0_1',
              input: [{ type: 'stage', stage: 0 }],
              processor: {
                type: 'segmentGenerator',
                dataSchema: {
                  dataSource: 'kttm_simple',
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
            },
          },
        ],
        counters: {
          '0': {
            '0': {
              input0: { type: 'channel', totalFiles: [1] },
              sortProgress: {
                type: 'sortProgress',
                totalMergingLevels: -1,
                levelToTotalBatches: {},
                levelToMergedBatches: {},
                totalMergersForUltimateLevel: -1,
              },
            },
          },
        },
      },
    },
  },
);
