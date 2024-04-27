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

import { Stages } from './stages';

/*
===== Query =====

-- This demo has a query that is identical to the previous demo except instead of reading the fact data from an external
-- file, it reads the "kttm_simple" datasource that was created in Demo 1.
-- This shows you that you can mix and match data already stored in Druid with external data and transform as needed.
--
-- In the next demo you will look at another type of data transformation.

REPLACE INTO "kttm_reingest" OVERWRITE ALL
WITH
country_lookup AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/lookup/countries.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}'
  )
) EXTEND ("Country" VARCHAR, "Capital" VARCHAR, "ISO3" VARCHAR, "ISO2" VARCHAR))

SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  CAST(REGEXP_EXTRACT(browser_version, '^(\d+)') AS BIGINT) AS browser_major,
  MV_TO_ARRAY("language") AS "language",
  os,
  city,
  country,
  country_lookup.Capital AS capital,
  country_lookup.ISO3 AS iso3,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_simple
LEFT JOIN country_lookup ON country_lookup.Country = kttm_simple.country
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
PARTITIONED BY HOUR
CLUSTERED BY browser, session

===== Context =====

{
  "maxNumTasks": 2
}
 */

export const STAGES = new Stages(
  [
    {
      stageNumber: 0,
      definition: {
        id: '73775b03-43e9-41fc-be5a-bb320528b4f5_0',
        input: [
          {
            type: 'external',
            inputSource: {
              type: 'http',
              uris: ['https://static.imply.io/example-data/lookup/countries.tsv'],
            },
            inputFormat: {
              type: 'tsv',
              delimiter: '\t',
              findColumnsFromHeader: true,
            },
            signature: [
              {
                name: 'Country',
                type: 'STRING',
              },
              {
                name: 'Capital',
                type: 'STRING',
              },
              {
                name: 'ISO3',
                type: 'STRING',
              },
              {
                name: 'ISO2',
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
                uris: ['https://static.imply.io/example-data/lookup/countries.tsv'],
              },
              inputFormat: {
                type: 'tsv',
                delimiter: '\t',
                findColumnsFromHeader: true,
              },
              signature: [
                {
                  name: 'Country',
                  type: 'STRING',
                },
                {
                  name: 'Capital',
                  type: 'STRING',
                },
                {
                  name: 'ISO3',
                  type: 'STRING',
                },
                {
                  name: 'ISO2',
                  type: 'STRING',
                },
              ],
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            resultFormat: 'compactedList',
            columns: ['Capital', 'Country', 'ISO3'],
            legacy: false,
            context: {
              __resultFormat: 'array',
              executionMode: 'async',
              finalizeAggregations: false,
              groupByEnableMultiValueUnnesting: false,
              maxNumTasks: 2,
              queryId: '63529263-1b27-4e4b-bc77-aa93647949f1',
              scanSignature:
                '[{"name":"Capital","type":"STRING"},{"name":"Country","type":"STRING"},{"name":"ISO3","type":"STRING"}]',
              sqlInsertSegmentGranularity: null,
              sqlQueryId: '63529263-1b27-4e4b-bc77-aa93647949f1',
              sqlReplaceTimeChunks: 'all',
              waitUntilSegmentsLoad: true,
            },
            granularity: {
              type: 'all',
            },
          },
        },
        signature: [
          {
            name: '__boost',
            type: 'LONG',
          },
          {
            name: 'Capital',
            type: 'STRING',
          },
          {
            name: 'Country',
            type: 'STRING',
          },
          {
            name: 'ISO3',
            type: 'STRING',
          },
        ],
        shuffleSpec: {
          type: 'maxCount',
          clusterBy: {
            columns: [
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
      startTime: '2024-01-23T18:58:43.197Z',
      duration: 4115,
      sort: true,
    },
    {
      stageNumber: 1,
      definition: {
        id: '73775b03-43e9-41fc-be5a-bb320528b4f5_1',
        input: [
          {
            type: 'table',
            dataSource: 'kttm_simple',
            intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            filter: {
              type: 'equals',
              column: 'os',
              matchValueType: 'STRING',
              matchValue: 'iOS',
            },
            filterFields: ['os'],
          },
          {
            type: 'stage',
            stage: 0,
          },
        ],
        broadcast: [1],
        processor: {
          type: 'groupByPreShuffle',
          query: {
            queryType: 'groupBy',
            dataSource: {
              type: 'join',
              left: {
                type: 'inputNumber',
                inputNumber: 0,
              },
              right: {
                type: 'inputNumber',
                inputNumber: 1,
              },
              rightPrefix: 'j0.',
              condition: '("country" == "j0.Country")',
              joinType: 'LEFT',
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            virtualColumns: [
              {
                type: 'expression',
                name: 'v0',
                expression:
                  "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'PT1M',null,'UTC')",
                outputType: 'LONG',
              },
              {
                type: 'expression',
                name: 'v1',
                expression: 'mv_to_array("language")',
                outputType: 'ARRAY<STRING>',
              },
              {
                type: 'expression',
                name: 'v2',
                expression: "'iOS'",
                outputType: 'STRING',
              },
            ],
            filter: {
              type: 'equals',
              column: 'os',
              matchValueType: 'STRING',
              matchValue: 'iOS',
            },
            granularity: {
              type: 'all',
            },
            dimensions: [
              {
                type: 'default',
                dimension: 'v0',
                outputName: 'd0',
                outputType: 'LONG',
              },
              {
                type: 'default',
                dimension: 'session',
                outputName: 'd1',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'agent_category',
                outputName: 'd2',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'agent_type',
                outputName: 'd3',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'browser',
                outputName: 'd4',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'browser_version',
                outputName: 'd5',
                outputType: 'STRING',
              },
              {
                type: 'extraction',
                dimension: 'browser_version',
                outputName: 'd6',
                outputType: 'LONG',
                extractionFn: {
                  type: 'regex',
                  expr: '^(\\d+)',
                  index: 0,
                  replaceMissingValue: true,
                  replaceMissingValueWith: null,
                },
              },
              {
                type: 'default',
                dimension: 'v1',
                outputName: 'd7',
                outputType: 'ARRAY<STRING>',
              },
              {
                type: 'default',
                dimension: 'v2',
                outputName: 'd8',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'city',
                outputName: 'd9',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'country',
                outputName: 'd10',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'j0.Capital',
                outputName: 'd11',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'j0.ISO3',
                outputName: 'd12',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'forwarded_for',
                outputName: 'd13',
                outputType: 'STRING',
              },
            ],
            aggregations: [
              {
                type: 'count',
                name: 'a0',
              },
              {
                type: 'longSum',
                name: 'a1',
                fieldName: 'session_length',
              },
              {
                type: 'HLLSketchBuild',
                name: 'a2',
                fieldName: 'event_type',
                lgK: 12,
                tgtHllType: 'HLL_4',
                round: true,
              },
            ],
            limitSpec: {
              type: 'default',
              columns: [
                {
                  dimension: 'd4',
                  direction: 'ascending',
                  dimensionOrder: {
                    type: 'lexicographic',
                  },
                },
                {
                  dimension: 'd1',
                  direction: 'ascending',
                  dimensionOrder: {
                    type: 'lexicographic',
                  },
                },
              ],
            },
            context: {
              __resultFormat: 'array',
              __timeColumn: 'd0',
              __user: 'allowAll',
              executionMode: 'async',
              finalize: false,
              finalizeAggregations: false,
              groupByEnableMultiValueUnnesting: false,
              maxNumTasks: 2,
              maxParseExceptions: 0,
              queryId: '63529263-1b27-4e4b-bc77-aa93647949f1',
              sqlInsertSegmentGranularity: '"HOUR"',
              sqlQueryId: '63529263-1b27-4e4b-bc77-aa93647949f1',
              sqlReplaceTimeChunks: 'all',
              waitUntilSegmentsLoad: true,
            },
          },
        },
        signature: [
          {
            name: 'd0',
            type: 'LONG',
          },
          {
            name: 'd1',
            type: 'STRING',
          },
          {
            name: 'd2',
            type: 'STRING',
          },
          {
            name: 'd3',
            type: 'STRING',
          },
          {
            name: 'd4',
            type: 'STRING',
          },
          {
            name: 'd5',
            type: 'STRING',
          },
          {
            name: 'd6',
            type: 'LONG',
          },
          {
            name: 'd7',
            type: 'ARRAY<STRING>',
          },
          {
            name: 'd8',
            type: 'STRING',
          },
          {
            name: 'd9',
            type: 'STRING',
          },
          {
            name: 'd10',
            type: 'STRING',
          },
          {
            name: 'd11',
            type: 'STRING',
          },
          {
            name: 'd12',
            type: 'STRING',
          },
          {
            name: 'd13',
            type: 'STRING',
          },
          {
            name: 'a0',
            type: 'LONG',
          },
          {
            name: 'a1',
            type: 'LONG',
          },
          {
            name: 'a2',
            type: 'COMPLEX<HLLSketchBuild>',
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
              {
                columnName: 'd1',
                order: 'ASCENDING',
              },
              {
                columnName: 'd2',
                order: 'ASCENDING',
              },
              {
                columnName: 'd3',
                order: 'ASCENDING',
              },
              {
                columnName: 'd4',
                order: 'ASCENDING',
              },
              {
                columnName: 'd5',
                order: 'ASCENDING',
              },
              {
                columnName: 'd6',
                order: 'ASCENDING',
              },
              {
                columnName: 'd7',
                order: 'ASCENDING',
              },
              {
                columnName: 'd8',
                order: 'ASCENDING',
              },
              {
                columnName: 'd9',
                order: 'ASCENDING',
              },
              {
                columnName: 'd10',
                order: 'ASCENDING',
              },
              {
                columnName: 'd11',
                order: 'ASCENDING',
              },
              {
                columnName: 'd12',
                order: 'ASCENDING',
              },
              {
                columnName: 'd13',
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
      startTime: '2024-01-23T18:58:47.300Z',
      duration: 911,
      sort: true,
    },
    {
      stageNumber: 2,
      definition: {
        id: '73775b03-43e9-41fc-be5a-bb320528b4f5_2',
        input: [
          {
            type: 'stage',
            stage: 1,
          },
        ],
        processor: {
          type: 'groupByPostShuffle',
          query: {
            queryType: 'groupBy',
            dataSource: {
              type: 'join',
              left: {
                type: 'inputNumber',
                inputNumber: 0,
              },
              right: {
                type: 'inputNumber',
                inputNumber: 1,
              },
              rightPrefix: 'j0.',
              condition: '("country" == "j0.Country")',
              joinType: 'LEFT',
            },
            intervals: {
              type: 'intervals',
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            virtualColumns: [
              {
                type: 'expression',
                name: 'v0',
                expression:
                  "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'PT1M',null,'UTC')",
                outputType: 'LONG',
              },
              {
                type: 'expression',
                name: 'v1',
                expression: 'mv_to_array("language")',
                outputType: 'ARRAY<STRING>',
              },
              {
                type: 'expression',
                name: 'v2',
                expression: "'iOS'",
                outputType: 'STRING',
              },
            ],
            filter: {
              type: 'equals',
              column: 'os',
              matchValueType: 'STRING',
              matchValue: 'iOS',
            },
            granularity: {
              type: 'all',
            },
            dimensions: [
              {
                type: 'default',
                dimension: 'v0',
                outputName: 'd0',
                outputType: 'LONG',
              },
              {
                type: 'default',
                dimension: 'session',
                outputName: 'd1',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'agent_category',
                outputName: 'd2',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'agent_type',
                outputName: 'd3',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'browser',
                outputName: 'd4',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'browser_version',
                outputName: 'd5',
                outputType: 'STRING',
              },
              {
                type: 'extraction',
                dimension: 'browser_version',
                outputName: 'd6',
                outputType: 'LONG',
                extractionFn: {
                  type: 'regex',
                  expr: '^(\\d+)',
                  index: 0,
                  replaceMissingValue: true,
                  replaceMissingValueWith: null,
                },
              },
              {
                type: 'default',
                dimension: 'v1',
                outputName: 'd7',
                outputType: 'ARRAY<STRING>',
              },
              {
                type: 'default',
                dimension: 'v2',
                outputName: 'd8',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'city',
                outputName: 'd9',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'country',
                outputName: 'd10',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'j0.Capital',
                outputName: 'd11',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'j0.ISO3',
                outputName: 'd12',
                outputType: 'STRING',
              },
              {
                type: 'default',
                dimension: 'forwarded_for',
                outputName: 'd13',
                outputType: 'STRING',
              },
            ],
            aggregations: [
              {
                type: 'count',
                name: 'a0',
              },
              {
                type: 'longSum',
                name: 'a1',
                fieldName: 'session_length',
              },
              {
                type: 'HLLSketchBuild',
                name: 'a2',
                fieldName: 'event_type',
                lgK: 12,
                tgtHllType: 'HLL_4',
                round: true,
              },
            ],
            limitSpec: {
              type: 'default',
              columns: [
                {
                  dimension: 'd4',
                  direction: 'ascending',
                  dimensionOrder: {
                    type: 'lexicographic',
                  },
                },
                {
                  dimension: 'd1',
                  direction: 'ascending',
                  dimensionOrder: {
                    type: 'lexicographic',
                  },
                },
              ],
            },
            context: {
              __resultFormat: 'array',
              __timeColumn: 'd0',
              __user: 'allowAll',
              executionMode: 'async',
              finalize: false,
              finalizeAggregations: false,
              groupByEnableMultiValueUnnesting: false,
              maxNumTasks: 2,
              maxParseExceptions: 0,
              queryId: '63529263-1b27-4e4b-bc77-aa93647949f1',
              sqlInsertSegmentGranularity: '"HOUR"',
              sqlQueryId: '63529263-1b27-4e4b-bc77-aa93647949f1',
              sqlReplaceTimeChunks: 'all',
              waitUntilSegmentsLoad: true,
            },
          },
        },
        signature: [
          {
            name: '__bucket',
            type: 'LONG',
          },
          {
            name: 'd4',
            type: 'STRING',
          },
          {
            name: 'd1',
            type: 'STRING',
          },
          {
            name: 'd0',
            type: 'LONG',
          },
          {
            name: 'd2',
            type: 'STRING',
          },
          {
            name: 'd3',
            type: 'STRING',
          },
          {
            name: 'd5',
            type: 'STRING',
          },
          {
            name: 'd6',
            type: 'LONG',
          },
          {
            name: 'd7',
            type: 'ARRAY<STRING>',
          },
          {
            name: 'd8',
            type: 'STRING',
          },
          {
            name: 'd9',
            type: 'STRING',
          },
          {
            name: 'd10',
            type: 'STRING',
          },
          {
            name: 'd11',
            type: 'STRING',
          },
          {
            name: 'd12',
            type: 'STRING',
          },
          {
            name: 'd13',
            type: 'STRING',
          },
          {
            name: 'a0',
            type: 'LONG',
          },
          {
            name: 'a1',
            type: 'LONG',
          },
          {
            name: 'a2',
            type: 'COMPLEX<HLLSketchBuild>',
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
                columnName: 'd4',
                order: 'ASCENDING',
              },
              {
                columnName: 'd1',
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
      phase: 'FINISHED',
      workerCount: 1,
      partitionCount: 24,
      startTime: '2024-01-23T18:58:48.203Z',
      duration: 522,
      sort: true,
    },
    {
      stageNumber: 3,
      definition: {
        id: '73775b03-43e9-41fc-be5a-bb320528b4f5_3',
        input: [
          {
            type: 'stage',
            stage: 2,
          },
        ],
        processor: {
          type: 'segmentGenerator',
          dataSchema: {
            dataSource: 'kttm_reingest',
            timestampSpec: {
              column: '__time',
              format: 'millis',
              missingValue: null,
            },
            dimensionsSpec: {
              dimensions: [
                {
                  type: 'string',
                  name: 'browser',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'session',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'agent_category',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'agent_type',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'browser_version',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'long',
                  name: 'browser_major',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: false,
                },
                {
                  type: 'string',
                  name: 'language',
                  multiValueHandling: 'ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'os',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'city',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'country',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'capital',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'iso3',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
                {
                  type: 'string',
                  name: 'ip_address',
                  multiValueHandling: 'SORTED_ARRAY',
                  createBitmapIndex: true,
                },
              ],
              dimensionExclusions: ['__time', 'unique_event_types', 'cnt', 'session_length'],
              includeAllDimensions: false,
              useSchemaDiscovery: false,
            },
            metricsSpec: [
              {
                type: 'longSum',
                name: 'cnt',
                fieldName: 'cnt',
              },
              {
                type: 'longSum',
                name: 'session_length',
                fieldName: 'session_length',
              },
              {
                type: 'HLLSketchMerge',
                name: 'unique_event_types',
                fieldName: 'unique_event_types',
                lgK: 12,
                tgtHllType: 'HLL_4',
                round: true,
              },
            ],
            granularitySpec: {
              type: 'arbitrary',
              queryGranularity: {
                type: 'none',
              },
              rollup: true,
              intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
            },
            transformSpec: {
              filter: null,
              transforms: [],
            },
          },
          columnMappings: [
            {
              queryColumn: 'd0',
              outputColumn: '__time',
            },
            {
              queryColumn: 'd1',
              outputColumn: 'session',
            },
            {
              queryColumn: 'd2',
              outputColumn: 'agent_category',
            },
            {
              queryColumn: 'd3',
              outputColumn: 'agent_type',
            },
            {
              queryColumn: 'd4',
              outputColumn: 'browser',
            },
            {
              queryColumn: 'd5',
              outputColumn: 'browser_version',
            },
            {
              queryColumn: 'd6',
              outputColumn: 'browser_major',
            },
            {
              queryColumn: 'd7',
              outputColumn: 'language',
            },
            {
              queryColumn: 'd8',
              outputColumn: 'os',
            },
            {
              queryColumn: 'd9',
              outputColumn: 'city',
            },
            {
              queryColumn: 'd10',
              outputColumn: 'country',
            },
            {
              queryColumn: 'd11',
              outputColumn: 'capital',
            },
            {
              queryColumn: 'd12',
              outputColumn: 'iso3',
            },
            {
              queryColumn: 'd13',
              outputColumn: 'ip_address',
            },
            {
              queryColumn: 'a0',
              outputColumn: 'cnt',
            },
            {
              queryColumn: 'a1',
              outputColumn: 'session_length',
            },
            {
              queryColumn: 'a2',
              outputColumn: 'unique_event_types',
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
      phase: 'FINISHED',
      workerCount: 1,
      partitionCount: 24,
      startTime: '2024-01-23T18:58:48.711Z',
      duration: 1669,
    },
  ],
  {
    '0': {
      '0': {
        input0: {
          type: 'channel',
          rows: [197],
          bytes: [5018],
          files: [1],
          totalFiles: [1],
        },
        output: {
          type: 'channel',
          rows: [197],
          bytes: [12907],
          frames: [1],
        },
        shuffle: {
          type: 'channel',
          rows: [197],
          bytes: [12119],
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
          progressDigest: 1,
        },
      },
    },
    '1': {
      '0': {
        input0: {
          type: 'channel',
          rows: [465346],
          bytes: [58995494],
          files: [1],
          totalFiles: [1],
        },
        input1: {
          type: 'channel',
          rows: [197],
          bytes: [12119],
          frames: [1],
        },
        output: {
          type: 'channel',
          rows: [39742],
          bytes: [11101944],
          frames: [2],
        },
        shuffle: {
          type: 'channel',
          rows: [39742],
          bytes: [10943622],
          frames: [21],
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
          progressDigest: 1,
        },
      },
    },
    '2': {
      '0': {
        input0: {
          type: 'channel',
          rows: [39742],
          bytes: [10943622],
          frames: [21],
        },
        output: {
          type: 'channel',
          rows: [39725],
          bytes: [11613687],
          frames: [2],
        },
        shuffle: {
          type: 'channel',
          rows: [
            888, 995, 1419, 905, 590, 652, 322, 247, 236, 309, 256, 260, 440, 876, 1394, 892, 3595,
            6522, 4525, 4326, 4149, 2561, 1914, 1452,
          ],
          bytes: [
            257524, 289731, 412396, 262388, 170554, 188324, 92275, 69531, 65844, 85875, 71852,
            72512, 123204, 249217, 399583, 256916, 1039927, 1887893, 1307287, 1248166, 1195593,
            738804, 552485, 418062,
          ],
          frames: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4, 3, 3, 3, 2, 2, 1],
        },
        sortProgress: {
          type: 'sortProgress',
          totalMergingLevels: 3,
          levelToTotalBatches: {
            '0': 1,
            '1': 1,
            '2': 24,
          },
          levelToMergedBatches: {
            '0': 1,
            '1': 1,
            '2': 24,
          },
          totalMergersForUltimateLevel: 24,
          progressDigest: 1,
        },
      },
    },
    '3': {
      '0': {
        input0: {
          type: 'channel',
          rows: [
            888, 995, 1419, 905, 590, 652, 322, 247, 236, 309, 256, 260, 440, 876, 1394, 892, 3595,
            6522, 4525, 4326, 4149, 2561, 1914, 1452,
          ],
          bytes: [
            257524, 289731, 412396, 262388, 170554, 188324, 92275, 69531, 65844, 85875, 71852,
            72512, 123204, 249217, 399583, 256916, 1039927, 1887893, 1307287, 1248166, 1195593,
            738804, 552485, 418062,
          ],
          frames: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4, 3, 3, 3, 2, 2, 1],
        },
        segmentGenerationProgress: {
          type: 'segmentGenerationProgress',
          rowsProcessed: 39725,
          rowsPersisted: 39725,
          rowsMerged: 39725,
          rowsPushed: 39725,
        },
      },
    },
  },
);
