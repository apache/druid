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

REPLACE INTO "kttm_simple" OVERWRITE ALL
SELECT TIME_PARSE("timestamp") AS "__time", agent_type
FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_type","type":"string"}]'
  )
)
PARTITIONED BY ALL

===== Context =====

{
  "maxParseExceptions": 10
}
*/

export const EXECUTION_INGEST_ERROR = Execution.fromTaskPayloadAndReport(
  {
    task: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
    payload: {
      type: 'query_controller',
      id: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
      spec: {
        query: {
          queryType: 'scan',
          dataSource: {
            type: 'external',
            inputSource: {
              type: 'http',
              uris: ['https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz'],
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
            maxParseExceptions: 10,
            queryId: '955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
            scanSignature: '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
            sqlInsertSegmentGranularity: '{"type":"all"}',
            sqlQueryId: '955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
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
        'REPLACE INTO "kttm_simple" OVERWRITE ALL\nSELECT TIME_PARSE("timestamp") AS "__time", agent_type\nFROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz"]}\',\n    \'{"type":"json"}\',\n    \'[{"name":"timestamp","type":"string"},{"name":"agent_type","type":"string"}]\'\n  )\n)\nPARTITIONED BY ALL',
      sqlQueryContext: {
        maxParseExceptions: 10,
        finalizeAggregations: false,
        sqlQueryId: '955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
        groupByEnableMultiValueUnnesting: false,
        sqlInsertSegmentGranularity: '{"type":"all"}',
        sqlReplaceTimeChunks: 'all',
        queryId: '955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
      },
      sqlTypeNames: ['TIMESTAMP', 'VARCHAR'],
      context: { forceTimeChunkLock: true, useLineageBasedSegmentAllocation: true },
      groupId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
      dataSource: 'kttm_simple',
      resource: {
        availabilityGroup: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
        requiredCapacity: 1,
      },
    },
  },

  {
    multiStageQuery: {
      type: 'multiStageQuery',
      taskId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
      payload: {
        status: {
          status: 'FAILED',
          errorReport: {
            taskId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
            host: 'localhost',
            error: {
              errorCode: 'TooManyWarnings',
              maxWarnings: 10,
              rootErrorCode: 'CannotParseExternalData',
              errorMessage:
                'Too many warnings of type CannotParseExternalData generated (max = 10)',
            },
          },
          warnings: [
            {
              taskId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a-worker0',
              host: 'localhost:8091',
              stageNumber: 0,
              error: {
                errorCode: 'CannotParseExternalData',
                errorMessage:
                  'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 13588, Line: 13588)',
              },
              exceptionStackTrace:
                'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 13588, Line: 13588)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:79)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:74)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.next(CloseableIterator.java:108)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.next(CloseableIterator.java:52)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1$1.hasNext(ExternalInputSliceReader.java:179)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:70)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:245)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:172)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:164)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:137)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:70)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:666)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (String)""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4360)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4205)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3214)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3182)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:69)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 22 more\n',
            },
            {
              taskId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a-worker0',
              host: 'localhost:8091',
              stageNumber: 0,
              error: {
                errorCode: 'CannotParseExternalData',
                errorMessage:
                  'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 27029, Line: 27030)',
              },
              exceptionStackTrace:
                'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 27029, Line: 27030)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:79)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:74)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.next(CloseableIterator.java:108)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.next(CloseableIterator.java:52)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1$1.hasNext(ExternalInputSliceReader.java:179)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:70)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:245)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:172)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:164)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:137)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:70)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:666)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (String)""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4360)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4205)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3214)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3182)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:69)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 22 more\n',
            },
            {
              taskId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a-worker0',
              host: 'localhost:8091',
              stageNumber: 0,
              error: {
                errorCode: 'CannotParseExternalData',
                errorMessage:
                  'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 42034, Line: 42036)',
              },
              exceptionStackTrace:
                'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 42034, Line: 42036)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:79)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:74)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.next(CloseableIterator.java:108)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.next(CloseableIterator.java:52)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1$1.hasNext(ExternalInputSliceReader.java:179)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:70)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:245)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:172)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:164)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:137)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:70)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:666)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (String)""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4360)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4205)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3214)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3182)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:69)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 22 more\n',
            },
            {
              taskId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a-worker0',
              host: 'localhost:8091',
              stageNumber: 0,
              error: {
                errorCode: 'CannotParseExternalData',
                errorMessage:
                  'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 54912, Line: 54915)',
              },
              exceptionStackTrace:
                'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 54912, Line: 54915)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:79)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:74)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.next(CloseableIterator.java:108)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.next(CloseableIterator.java:52)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1$1.hasNext(ExternalInputSliceReader.java:179)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:70)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:245)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:172)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:164)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:137)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:70)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:666)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (String)""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4360)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4205)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3214)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3182)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:69)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 22 more\n',
            },
            {
              taskId: 'query-955fa5a2-0ae6-4912-bc39-cc9fc012de5a-worker0',
              host: 'localhost:8091',
              stageNumber: 0,
              error: {
                errorCode: 'CannotParseExternalData',
                errorMessage:
                  'Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 63995, Line: 63999)',
              },
              exceptionStackTrace:
                'org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz, Record: 63995, Line: 63999)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:79)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:74)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$2.next(CloseableIterator.java:108)\n\tat org.apache.druid.java.util.common.parsers.CloseableIterator$1.next(CloseableIterator.java:52)\n\tat org.apache.druid.msq.input.external.ExternalInputSliceReader$1$1.hasNext(ExternalInputSliceReader.java:179)\n\tat org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)\n\tat org.apache.druid.segment.RowWalker.advance(RowWalker.java:70)\n\tat org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)\n\tat org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:245)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:172)\n\tat org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:164)\n\tat org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:137)\n\tat org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:70)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:229)\n\tat org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:137)\n\tat org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:666)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\nCaused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input\n at [Source: (String)""; line: 1, column: 0]\n\tat com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)\n\tat com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4360)\n\tat com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4205)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3214)\n\tat com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3182)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:69)\n\tat org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)\n\tat org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)\n\t... 22 more\n',
            },
          ],
          startTime: '2022-10-31T16:16:22.464Z',
          durationMs: 7229,
          pendingTasks: 0,
          runningTasks: 2,
        },
        stages: [
          {
            stageNumber: 0,
            definition: {
              id: 'fc6c3715-9211-4f6e-9d7a-a7e6a8e6b297_0',
              input: [
                {
                  type: 'external',
                  inputSource: {
                    type: 'http',
                    uris: ['https://static.imply.io/example-data/kttm/kttm-2019-08-25.json.gz'],
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
                    maxParseExceptions: 10,
                    queryId: '955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
                    scanSignature:
                      '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
                    sqlInsertSegmentGranularity: '{"type":"all"}',
                    sqlQueryId: '955fa5a2-0ae6-4912-bc39-cc9fc012de5a',
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
            phase: 'FAILED',
            workerCount: 1,
            startTime: '2022-10-31T16:16:24.680Z',
            duration: 5013,
            sort: true,
          },
          {
            stageNumber: 1,
            definition: {
              id: 'fc6c3715-9211-4f6e-9d7a-a7e6a8e6b297_1',
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
              input0: { type: 'channel', rows: [237883], totalFiles: [1] },
              output: { type: 'channel', rows: [141660], bytes: [7685544], frames: [1] },
              sortProgress: {
                type: 'sortProgress',
                totalMergingLevels: -1,
                levelToTotalBatches: {},
                levelToMergedBatches: {},
                totalMergersForUltimateLevel: -1,
              },
              warnings: { type: 'warnings', CannotParseExternalData: 17 },
            },
          },
        },
      },
    },
  },
);
