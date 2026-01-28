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

import { FAILED_ASYNC_STATUS, SUCCESS_ASYNC_STATUS } from '../async-query/async-query.mock';

import { Execution } from './execution';
import { EXECUTION_INGEST_COMPLETE } from './execution-ingest-complete.mock';

describe('Execution', () => {
  describe('.fromTaskReport', () => {
    it('fails for bad status (error: null)', () => {
      expect(() =>
        Execution.fromTaskReport({
          asyncResultId: 'multi-stage-query-sql-1392d806-c17f-4937-94ee-8fa0a3ce1566',
          error: null,
        } as any),
      ).toThrowError('Invalid payload');
    });

    it('works in a general case', () => {
      expect(EXECUTION_INGEST_COMPLETE).toMatchInlineSnapshot(`
        Execution {
          "_payload": {
            "payload": {
              "context": {
                "forceTimeChunkLock": true,
                "useLineageBasedSegmentAllocation": true,
              },
              "dataSource": "kttm_simple",
              "groupId": "query-346b9ac6-4912-46e4-9b98-75f11071af87",
              "id": "query-346b9ac6-4912-46e4-9b98-75f11071af87",
              "nativeTypeNames": [
                "LONG",
                "STRING",
              ],
              "resource": {
                "availabilityGroup": "query-346b9ac6-4912-46e4-9b98-75f11071af87",
                "requiredCapacity": 1,
              },
              "spec": {
                "assignmentStrategy": "max",
                "columnMappings": [
                  {
                    "outputColumn": "__time",
                    "queryColumn": "v0",
                  },
                  {
                    "outputColumn": "agent_type",
                    "queryColumn": "agent_type",
                  },
                ],
                "destination": {
                  "dataSource": "kttm_simple",
                  "replaceTimeChunks": [
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                  ],
                  "segmentGranularity": {
                    "type": "all",
                  },
                  "type": "dataSource",
                },
                "query": {
                  "columns": [
                    "agent_type",
                    "v0",
                  ],
                  "context": {
                    "__resultFormat": "array",
                    "__user": "allowAll",
                    "executionMode": "async",
                    "finalize": false,
                    "finalizeAggregations": false,
                    "groupByEnableMultiValueUnnesting": false,
                    "maxNumTasks": 2,
                    "maxParseExceptions": 0,
                    "queryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
                    "scanSignature": "[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]",
                    "sqlInsertSegmentGranularity": "{"type":"all"}",
                    "sqlQueryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
                    "sqlReplaceTimeChunks": "all",
                    "waitUntilSegmentsLoad": true,
                  },
                  "dataSource": {
                    "inputFormat": {
                      "assumeNewlineDelimited": false,
                      "keepNullColumns": false,
                      "type": "json",
                      "useJsonNodeReader": false,
                    },
                    "inputSource": {
                      "type": "http",
                      "uris": [
                        "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                      ],
                    },
                    "signature": [
                      {
                        "name": "timestamp",
                        "type": "STRING",
                      },
                      {
                        "name": "agent_type",
                        "type": "STRING",
                      },
                    ],
                    "type": "external",
                  },
                  "granularity": {
                    "type": "all",
                  },
                  "intervals": {
                    "intervals": [
                      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                    ],
                    "type": "intervals",
                  },
                  "queryType": "scan",
                  "resultFormat": "compactedList",
                  "virtualColumns": [
                    {
                      "expression": "timestamp_parse("timestamp",null,'UTC')",
                      "name": "v0",
                      "outputType": "LONG",
                      "type": "expression",
                    },
                  ],
                },
                "tuningConfig": {
                  "maxNumWorkers": 1,
                  "maxRowsInMemory": 100000,
                  "rowsPerSegment": 3000000,
                },
              },
              "sqlQuery": "REPLACE INTO "kttm_simple" OVERWRITE ALL
        SELECT
          TIME_PARSE("timestamp") AS "__time",
          "agent_type"
        FROM TABLE(
          EXTERN(
            '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "agent_type" VARCHAR)
        PARTITIONED BY ALL TIME",
              "sqlQueryContext": {
                "__resultFormat": "array",
                "executionMode": "async",
                "finalizeAggregations": false,
                "groupByEnableMultiValueUnnesting": false,
                "maxNumTasks": 2,
                "queryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
                "sqlInsertSegmentGranularity": "{"type":"all"}",
                "sqlQueryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
                "sqlReplaceTimeChunks": "all",
                "waitUntilSegmentsLoad": true,
              },
              "sqlResultsContext": {
                "stringifyArrays": true,
                "timeZone": "UTC",
              },
              "sqlTypeNames": [
                "TIMESTAMP",
                "VARCHAR",
              ],
              "type": "query_controller",
            },
            "task": "query-346b9ac6-4912-46e4-9b98-75f11071af87",
          },
          "capacityInfo": undefined,
          "destination": {
            "dataSource": "kttm_simple",
            "numTotalRows": 465346,
            "replaceTimeChunks": [
              "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
            ],
            "segmentGranularity": {
              "type": "all",
            },
            "type": "dataSource",
          },
          "destinationPages": undefined,
          "duration": 14208,
          "engine": "sql-msq-task",
          "error": undefined,
          "id": "query-346b9ac6-4912-46e4-9b98-75f11071af87",
          "nativeQuery": {
            "columns": [
              "agent_type",
              "v0",
            ],
            "context": {
              "__resultFormat": "array",
              "__user": "allowAll",
              "executionMode": "async",
              "finalize": false,
              "finalizeAggregations": false,
              "groupByEnableMultiValueUnnesting": false,
              "maxNumTasks": 2,
              "maxParseExceptions": 0,
              "queryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
              "scanSignature": "[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]",
              "sqlInsertSegmentGranularity": "{"type":"all"}",
              "sqlQueryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
              "sqlReplaceTimeChunks": "all",
              "waitUntilSegmentsLoad": true,
            },
            "dataSource": {
              "inputFormat": {
                "assumeNewlineDelimited": false,
                "keepNullColumns": false,
                "type": "json",
                "useJsonNodeReader": false,
              },
              "inputSource": {
                "type": "http",
                "uris": [
                  "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                ],
              },
              "signature": [
                {
                  "name": "timestamp",
                  "type": "STRING",
                },
                {
                  "name": "agent_type",
                  "type": "STRING",
                },
              ],
              "type": "external",
            },
            "granularity": {
              "type": "all",
            },
            "intervals": {
              "intervals": [
                "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
              ],
              "type": "intervals",
            },
            "queryType": "scan",
            "resultFormat": "compactedList",
            "virtualColumns": [
              {
                "expression": "timestamp_parse("timestamp",null,'UTC')",
                "name": "v0",
                "outputType": "LONG",
                "type": "expression",
              },
            ],
          },
          "queryContext": {
            "__resultFormat": "array",
            "executionMode": "async",
            "finalizeAggregations": false,
            "groupByEnableMultiValueUnnesting": false,
            "maxNumTasks": 2,
            "waitUntilSegmentsLoad": true,
          },
          "result": undefined,
          "segmentStatus": {
            "duration": 5092,
            "onDemandSegments": 0,
            "pendingSegments": 0,
            "precachedSegments": 1,
            "startTime": "2024-01-23T19:45:52.189Z",
            "state": "SUCCESS",
            "totalSegments": 1,
            "unknownSegments": 0,
            "usedSegments": 1,
          },
          "sqlQuery": "REPLACE INTO "kttm_simple" OVERWRITE ALL
        SELECT
          TIME_PARSE("timestamp") AS "__time",
          "agent_type"
        FROM TABLE(
          EXTERN(
            '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "agent_type" VARCHAR)
        PARTITIONED BY ALL TIME",
          "stages": Stages {
            "counters": {
              "0": {
                "0": {
                  "input0": {
                    "bytes": [
                      360464067,
                    ],
                    "files": [
                      1,
                    ],
                    "rows": [
                      465346,
                    ],
                    "totalFiles": [
                      1,
                    ],
                    "type": "channel",
                  },
                  "output": {
                    "bytes": [
                      25430674,
                    ],
                    "frames": [
                      4,
                    ],
                    "rows": [
                      465346,
                    ],
                    "type": "channel",
                  },
                  "shuffle": {
                    "bytes": [
                      23570446,
                    ],
                    "frames": [
                      38,
                    ],
                    "rows": [
                      465346,
                    ],
                    "type": "channel",
                  },
                  "sortProgress": {
                    "levelToMergedBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "levelToTotalBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "progressDigest": 1,
                    "totalMergersForUltimateLevel": 1,
                    "totalMergingLevels": 3,
                    "type": "sortProgress",
                  },
                },
              },
              "1": {
                "0": {
                  "input0": {
                    "bytes": [
                      23570446,
                    ],
                    "frames": [
                      38,
                    ],
                    "rows": [
                      465346,
                    ],
                    "type": "channel",
                  },
                  "segmentGenerationProgress": {
                    "rowsMerged": 465346,
                    "rowsPersisted": 465346,
                    "rowsProcessed": 465346,
                    "rowsPushed": 465346,
                    "type": "segmentGenerationProgress",
                  },
                },
              },
            },
            "stages": [
              {
                "definition": {
                  "id": "7f62fa91-f49a-4053-adec-5aa09c251ee3_0",
                  "input": [
                    {
                      "inputFormat": {
                        "assumeNewlineDelimited": false,
                        "keepNullColumns": false,
                        "type": "json",
                        "useJsonNodeReader": false,
                      },
                      "inputSource": {
                        "type": "http",
                        "uris": [
                          "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                        ],
                      },
                      "signature": [
                        {
                          "name": "timestamp",
                          "type": "STRING",
                        },
                        {
                          "name": "agent_type",
                          "type": "STRING",
                        },
                      ],
                      "type": "external",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": {
                    "query": {
                      "columns": [
                        "agent_type",
                        "v0",
                      ],
                      "context": {
                        "__resultFormat": "array",
                        "__timeColumn": "v0",
                        "__user": "allowAll",
                        "executionMode": "async",
                        "finalize": false,
                        "finalizeAggregations": false,
                        "groupByEnableMultiValueUnnesting": false,
                        "maxNumTasks": 2,
                        "maxParseExceptions": 0,
                        "queryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
                        "scanSignature": "[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]",
                        "sqlInsertSegmentGranularity": "{"type":"all"}",
                        "sqlQueryId": "346b9ac6-4912-46e4-9b98-75f11071af87",
                        "sqlReplaceTimeChunks": "all",
                        "waitUntilSegmentsLoad": true,
                      },
                      "dataSource": {
                        "inputFormat": {
                          "assumeNewlineDelimited": false,
                          "keepNullColumns": false,
                          "type": "json",
                          "useJsonNodeReader": false,
                        },
                        "inputSource": {
                          "type": "http",
                          "uris": [
                            "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                          ],
                        },
                        "signature": [
                          {
                            "name": "timestamp",
                            "type": "STRING",
                          },
                          {
                            "name": "agent_type",
                            "type": "STRING",
                          },
                        ],
                        "type": "external",
                      },
                      "granularity": {
                        "type": "all",
                      },
                      "intervals": {
                        "intervals": [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "type": "intervals",
                      },
                      "queryType": "scan",
                      "resultFormat": "compactedList",
                      "virtualColumns": [
                        {
                          "expression": "timestamp_parse("timestamp",null,'UTC')",
                          "name": "v0",
                          "outputType": "LONG",
                          "type": "expression",
                        },
                      ],
                    },
                    "type": "scan",
                  },
                  "shuffleCheckHasMultipleValues": true,
                  "shuffleSpec": {
                    "clusterBy": {
                      "columns": [
                        {
                          "columnName": "__boost",
                          "order": "ASCENDING",
                        },
                      ],
                    },
                    "targetSize": 3000000,
                    "type": "targetSize",
                  },
                  "signature": [
                    {
                      "name": "__boost",
                      "type": "LONG",
                    },
                    {
                      "name": "agent_type",
                      "type": "STRING",
                    },
                    {
                      "name": "v0",
                      "type": "LONG",
                    },
                  ],
                },
                "duration": 6884,
                "partitionCount": 1,
                "phase": "FINISHED",
                "sort": true,
                "stageNumber": 0,
                "startTime": "2024-01-23T19:45:43.302Z",
                "workerCount": 1,
              },
              {
                "definition": {
                  "id": "7f62fa91-f49a-4053-adec-5aa09c251ee3_1",
                  "input": [
                    {
                      "stage": 0,
                      "type": "stage",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": {
                    "columnMappings": [
                      {
                        "outputColumn": "__time",
                        "queryColumn": "v0",
                      },
                      {
                        "outputColumn": "agent_type",
                        "queryColumn": "agent_type",
                      },
                    ],
                    "dataSchema": {
                      "dataSource": "kttm_simple",
                      "dimensionsSpec": {
                        "dimensionExclusions": [
                          "__time",
                        ],
                        "dimensions": [
                          {
                            "createBitmapIndex": true,
                            "multiValueHandling": "SORTED_ARRAY",
                            "name": "agent_type",
                            "type": "string",
                          },
                        ],
                        "includeAllDimensions": false,
                        "useSchemaDiscovery": false,
                      },
                      "granularitySpec": {
                        "intervals": [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "queryGranularity": {
                          "type": "none",
                        },
                        "rollup": false,
                        "type": "arbitrary",
                      },
                      "metricsSpec": [],
                      "timestampSpec": {
                        "column": "__time",
                        "format": "millis",
                        "missingValue": null,
                      },
                      "transformSpec": {
                        "filter": null,
                        "transforms": [],
                      },
                    },
                    "tuningConfig": {
                      "maxNumWorkers": 1,
                      "maxRowsInMemory": 100000,
                      "rowsPerSegment": 3000000,
                    },
                    "type": "segmentGenerator",
                  },
                  "signature": [],
                },
                "duration": 1263,
                "partitionCount": 1,
                "phase": "FINISHED",
                "stageNumber": 1,
                "startTime": "2024-01-23T19:45:50.170Z",
                "workerCount": 1,
              },
            ],
          },
          "startTime": 2024-01-23T19:45:43.073Z,
          "status": "SUCCESS",
          "usageInfo": {
            "pendingTasks": 0,
            "runningTasks": 1,
          },
          "warnings": undefined,
        }
      `);
    });
  });

  describe('.fromAsyncStatus', () => {
    it('works on SUCCESS', () => {
      expect(Execution.fromAsyncStatus(SUCCESS_ASYNC_STATUS)).toMatchInlineSnapshot(`
        Execution {
          "_payload": undefined,
          "capacityInfo": undefined,
          "destination": {
            "numTotalRows": 2,
            "type": "taskReport",
          },
          "destinationPages": [
            {
              "id": 0,
              "numRows": 2,
              "sizeInBytes": 150,
            },
          ],
          "duration": 7183,
          "engine": "sql-msq-task",
          "error": undefined,
          "id": "query-45f1dafd-8a52-4eb7-9a6c-77840cddd349",
          "nativeQuery": undefined,
          "queryContext": undefined,
          "result": _QueryResult {
            "header": [
              Column {
                "name": "channel",
                "nativeType": "STRING",
                "sqlType": "VARCHAR",
              },
              Column {
                "name": "Count",
                "nativeType": "LONG",
                "sqlType": "BIGINT",
              },
            ],
            "query": undefined,
            "queryDuration": undefined,
            "queryId": undefined,
            "resultContext": undefined,
            "rows": [
              [
                "#en.wikipedia",
                6650,
              ],
              [
                "#sh.wikipedia",
                3969,
              ],
            ],
            "sqlQuery": undefined,
            "sqlQueryId": undefined,
          },
          "segmentStatus": undefined,
          "sqlQuery": undefined,
          "stages": Stages {
            "counters": {
              "0": {
                "0": {
                  "input0": {
                    "bytes": [
                      6525055,
                    ],
                    "files": [
                      1,
                    ],
                    "rows": [
                      24433,
                    ],
                    "totalFiles": [
                      1,
                    ],
                    "type": "channel",
                  },
                  "output": {
                    "bytes": [
                      2335,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      51,
                    ],
                    "type": "channel",
                  },
                  "shuffle": {
                    "bytes": [
                      2131,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      51,
                    ],
                    "type": "channel",
                  },
                  "sortProgress": {
                    "levelToMergedBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "levelToTotalBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "progressDigest": 1,
                    "totalMergersForUltimateLevel": 1,
                    "totalMergingLevels": 3,
                    "type": "sortProgress",
                  },
                },
              },
              "1": {
                "0": {
                  "input0": {
                    "bytes": [
                      2131,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      51,
                    ],
                    "type": "channel",
                  },
                  "output": {
                    "bytes": [
                      2998,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      51,
                    ],
                    "type": "channel",
                  },
                  "shuffle": {
                    "bytes": [
                      2794,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      51,
                    ],
                    "type": "channel",
                  },
                  "sortProgress": {
                    "levelToMergedBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "levelToTotalBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "progressDigest": 1,
                    "totalMergersForUltimateLevel": 1,
                    "totalMergingLevels": 3,
                    "type": "sortProgress",
                  },
                },
              },
              "2": {
                "0": {
                  "input0": {
                    "bytes": [
                      2794,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      51,
                    ],
                    "type": "channel",
                  },
                  "output": {
                    "bytes": [
                      150,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      2,
                    ],
                    "type": "channel",
                  },
                  "shuffle": {
                    "bytes": [
                      142,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      2,
                    ],
                    "type": "channel",
                  },
                  "sortProgress": {
                    "levelToMergedBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "levelToTotalBatches": {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "progressDigest": 1,
                    "totalMergersForUltimateLevel": 1,
                    "totalMergingLevels": 3,
                    "type": "sortProgress",
                  },
                },
              },
            },
            "stages": [
              {
                "definition": {
                  "id": "query-45f1dafd-8a52-4eb7-9a6c-77840cddd349_0",
                  "input": [
                    {
                      "dataSource": "wikipedia",
                      "intervals": [
                        "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                      ],
                      "type": "table",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": {
                    "query": {
                      "aggregations": [
                        {
                          "name": "a0",
                          "type": "count",
                        },
                      ],
                      "context": {
                        "__resultFormat": "array",
                        "__user": "allowAll",
                        "executionMode": "async",
                        "finalize": true,
                        "maxNumTasks": 2,
                        "maxParseExceptions": 0,
                        "queryId": "45f1dafd-8a52-4eb7-9a6c-77840cddd349",
                        "sqlOuterLimit": 1001,
                        "sqlQueryId": "45f1dafd-8a52-4eb7-9a6c-77840cddd349",
                        "sqlStringifyArrays": false,
                      },
                      "dataSource": {
                        "inputNumber": 0,
                        "type": "inputNumber",
                      },
                      "dimensions": [
                        {
                          "dimension": "channel",
                          "outputName": "d0",
                          "outputType": "STRING",
                          "type": "default",
                        },
                      ],
                      "granularity": {
                        "type": "all",
                      },
                      "intervals": {
                        "intervals": [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "type": "intervals",
                      },
                      "limitSpec": {
                        "columns": [
                          {
                            "dimension": "a0",
                            "dimensionOrder": {
                              "type": "numeric",
                            },
                            "direction": "descending",
                          },
                        ],
                        "limit": 2,
                        "type": "default",
                      },
                      "queryType": "groupBy",
                    },
                    "type": "groupByPreShuffle",
                  },
                  "shuffleSpec": {
                    "aggregate": true,
                    "clusterBy": {
                      "columns": [
                        {
                          "columnName": "d0",
                          "order": "ASCENDING",
                        },
                      ],
                    },
                    "partitions": 1,
                    "type": "maxCount",
                  },
                  "signature": [
                    {
                      "name": "d0",
                      "type": "STRING",
                    },
                    {
                      "name": "a0",
                      "type": "LONG",
                    },
                  ],
                },
                "duration": 3384,
                "output": "localStorage",
                "partitionCount": 1,
                "phase": "FINISHED",
                "shuffle": "globalSort",
                "sort": true,
                "stageNumber": 0,
                "startTime": "2024-07-27T02:39:24.713Z",
                "workerCount": 1,
              },
              {
                "definition": {
                  "id": "query-45f1dafd-8a52-4eb7-9a6c-77840cddd349_1",
                  "input": [
                    {
                      "stage": 0,
                      "type": "stage",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": {
                    "query": {
                      "aggregations": [
                        {
                          "name": "a0",
                          "type": "count",
                        },
                      ],
                      "context": {
                        "__resultFormat": "array",
                        "__user": "allowAll",
                        "executionMode": "async",
                        "finalize": true,
                        "maxNumTasks": 2,
                        "maxParseExceptions": 0,
                        "queryId": "45f1dafd-8a52-4eb7-9a6c-77840cddd349",
                        "sqlOuterLimit": 1001,
                        "sqlQueryId": "45f1dafd-8a52-4eb7-9a6c-77840cddd349",
                        "sqlStringifyArrays": false,
                      },
                      "dataSource": {
                        "inputNumber": 0,
                        "type": "inputNumber",
                      },
                      "dimensions": [
                        {
                          "dimension": "channel",
                          "outputName": "d0",
                          "outputType": "STRING",
                          "type": "default",
                        },
                      ],
                      "granularity": {
                        "type": "all",
                      },
                      "intervals": {
                        "intervals": [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "type": "intervals",
                      },
                      "limitSpec": {
                        "columns": [
                          {
                            "dimension": "a0",
                            "dimensionOrder": {
                              "type": "numeric",
                            },
                            "direction": "descending",
                          },
                        ],
                        "limit": 2,
                        "type": "default",
                      },
                      "queryType": "groupBy",
                    },
                    "type": "groupByPostShuffle",
                  },
                  "shuffleSpec": {
                    "clusterBy": {
                      "columns": [
                        {
                          "columnName": "a0",
                          "order": "DESCENDING",
                        },
                        {
                          "columnName": "__boost",
                          "order": "ASCENDING",
                        },
                      ],
                    },
                    "partitions": 1,
                    "type": "maxCount",
                  },
                  "signature": [
                    {
                      "name": "a0",
                      "type": "LONG",
                    },
                    {
                      "name": "__boost",
                      "type": "LONG",
                    },
                    {
                      "name": "d0",
                      "type": "STRING",
                    },
                  ],
                },
                "duration": 26,
                "output": "localStorage",
                "partitionCount": 1,
                "phase": "FINISHED",
                "shuffle": "globalSort",
                "sort": true,
                "stageNumber": 1,
                "startTime": "2024-07-27T02:39:28.089Z",
                "workerCount": 1,
              },
              {
                "definition": {
                  "id": "query-45f1dafd-8a52-4eb7-9a6c-77840cddd349_2",
                  "input": [
                    {
                      "stage": 1,
                      "type": "stage",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": {
                    "limit": 2,
                    "type": "limit",
                  },
                  "shuffleSpec": {
                    "clusterBy": {
                      "columns": [
                        {
                          "columnName": "a0",
                          "order": "DESCENDING",
                        },
                        {
                          "columnName": "__boost",
                          "order": "ASCENDING",
                        },
                      ],
                    },
                    "partitions": 1,
                    "type": "maxCount",
                  },
                  "signature": [
                    {
                      "name": "a0",
                      "type": "LONG",
                    },
                    {
                      "name": "__boost",
                      "type": "LONG",
                    },
                    {
                      "name": "d0",
                      "type": "STRING",
                    },
                  ],
                },
                "duration": 12,
                "output": "localStorage",
                "partitionCount": 1,
                "phase": "FINISHED",
                "shuffle": "globalSort",
                "sort": true,
                "stageNumber": 2,
                "startTime": "2024-07-27T02:39:28.112Z",
                "workerCount": 1,
              },
            ],
          },
          "startTime": 2024-07-27T02:39:22.230Z,
          "status": "SUCCESS",
          "usageInfo": undefined,
          "warnings": undefined,
        }
      `);
    });

    it('works on FAILED', () => {
      expect(Execution.fromAsyncStatus(FAILED_ASYNC_STATUS)).toMatchInlineSnapshot(`
        Execution {
          "_payload": undefined,
          "capacityInfo": undefined,
          "destination": undefined,
          "destinationPages": undefined,
          "duration": 6954,
          "engine": "sql-msq-task",
          "error": {
            "error": {
              "category": "UNCATEGORIZED",
              "context": {
                "maxWarnings": "2",
                "rootErrorCode": "CannotParseExternalData",
              },
              "error": "druidException",
              "errorCode": "TooManyWarnings",
              "errorMessage": "Too many warnings of type CannotParseExternalData generated (max = 2)",
              "persona": "USER",
            },
            "taskId": "query-ea3e36df-ad67-4870-b136-f5616b17d9c4",
          },
          "id": "query-ea3e36df-ad67-4870-b136-f5616b17d9c4",
          "nativeQuery": undefined,
          "queryContext": undefined,
          "result": undefined,
          "segmentStatus": undefined,
          "sqlQuery": undefined,
          "stages": Stages {
            "counters": {
              "0": {
                "0": {
                  "input0": {
                    "bytes": [
                      7658,
                    ],
                    "files": [
                      1,
                    ],
                    "rows": [
                      10,
                    ],
                    "totalFiles": [
                      1,
                    ],
                    "type": "channel",
                  },
                  "output": {
                    "bytes": [
                      712,
                    ],
                    "frames": [
                      1,
                    ],
                    "rows": [
                      10,
                    ],
                    "type": "channel",
                  },
                  "sortProgress": {
                    "levelToMergedBatches": {},
                    "levelToTotalBatches": {
                      "0": 1,
                      "1": 1,
                      "2": -1,
                    },
                    "progressDigest": 0,
                    "totalMergersForUltimateLevel": -1,
                    "totalMergingLevels": 3,
                    "type": "sortProgress",
                  },
                  "warnings": {
                    "CannotParseExternalData": 3,
                    "type": "warnings",
                  },
                },
              },
            },
            "stages": [
              {
                "definition": {
                  "id": "query-ea3e36df-ad67-4870-b136-f5616b17d9c4_0",
                  "input": [
                    {
                      "inputFormat": {
                        "type": "json",
                      },
                      "inputSource": {
                        "type": "http",
                        "uris": [
                          "https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json",
                        ],
                      },
                      "signature": [
                        {
                          "name": "timestamp",
                          "type": "STRING",
                        },
                        {
                          "name": "agent_type",
                          "type": "STRING",
                        },
                      ],
                      "type": "external",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": {
                    "query": {
                      "columnTypes": [
                        "STRING",
                        "LONG",
                      ],
                      "columns": [
                        "agent_type",
                        "v0",
                      ],
                      "context": {
                        "__resultFormat": "array",
                        "__timeColumn": "v0",
                        "__user": "allowAll",
                        "executionMode": "async",
                        "finalize": false,
                        "finalizeAggregations": false,
                        "groupByEnableMultiValueUnnesting": false,
                        "maxNumTasks": 2,
                        "maxParseExceptions": 2,
                        "queryId": "ea3e36df-ad67-4870-b136-f5616b17d9c4",
                        "scanSignature": "[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]",
                        "sqlInsertSegmentGranularity": ""DAY"",
                        "sqlQueryId": "ea3e36df-ad67-4870-b136-f5616b17d9c4",
                        "sqlReplaceTimeChunks": "all",
                        "sqlStringifyArrays": false,
                        "waitUntilSegmentsLoad": true,
                      },
                      "dataSource": {
                        "inputFormat": {
                          "type": "json",
                        },
                        "inputSource": {
                          "type": "http",
                          "uris": [
                            "https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json",
                          ],
                        },
                        "signature": [
                          {
                            "name": "timestamp",
                            "type": "STRING",
                          },
                          {
                            "name": "agent_type",
                            "type": "STRING",
                          },
                        ],
                        "type": "external",
                      },
                      "granularity": {
                        "type": "all",
                      },
                      "intervals": {
                        "intervals": [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "type": "intervals",
                      },
                      "legacy": false,
                      "queryType": "scan",
                      "resultFormat": "compactedList",
                      "virtualColumns": [
                        {
                          "expression": "timestamp_parse("timestamp",null,'UTC')",
                          "name": "v0",
                          "outputType": "LONG",
                          "type": "expression",
                        },
                      ],
                    },
                    "type": "scan",
                  },
                  "shuffleCheckHasMultipleValues": true,
                  "shuffleSpec": {
                    "clusterBy": {
                      "bucketByCount": 1,
                      "columns": [
                        {
                          "columnName": "__bucket",
                          "order": "ASCENDING",
                        },
                        {
                          "columnName": "__boost",
                          "order": "ASCENDING",
                        },
                      ],
                    },
                    "targetSize": 3000000,
                    "type": "targetSize",
                  },
                  "signature": [
                    {
                      "name": "__bucket",
                      "type": "LONG",
                    },
                    {
                      "name": "__boost",
                      "type": "LONG",
                    },
                    {
                      "name": "agent_type",
                      "type": "STRING",
                    },
                    {
                      "name": "v0",
                      "type": "LONG",
                    },
                  ],
                },
                "duration": 4056,
                "output": "localStorage",
                "phase": "FAILED",
                "shuffle": "globalSort",
                "sort": true,
                "stageNumber": 0,
                "startTime": "2024-07-26T18:05:02.399Z",
                "workerCount": 1,
              },
              {
                "definition": {
                  "id": "query-ea3e36df-ad67-4870-b136-f5616b17d9c4_1",
                  "input": [
                    {
                      "stage": 0,
                      "type": "stage",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": {
                    "columnMappings": [
                      {
                        "outputColumn": "__time",
                        "queryColumn": "v0",
                      },
                      {
                        "outputColumn": "agent_type",
                        "queryColumn": "agent_type",
                      },
                    ],
                    "dataSchema": {
                      "dataSource": "kttm-blank-lines",
                      "dimensionsSpec": {
                        "dimensionExclusions": [
                          "__time",
                        ],
                        "dimensions": [
                          {
                            "createBitmapIndex": true,
                            "multiValueHandling": "SORTED_ARRAY",
                            "name": "agent_type",
                            "type": "string",
                          },
                        ],
                        "includeAllDimensions": false,
                        "useSchemaDiscovery": false,
                      },
                      "granularitySpec": {
                        "intervals": [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "queryGranularity": {
                          "type": "none",
                        },
                        "rollup": false,
                        "type": "arbitrary",
                      },
                      "metricsSpec": [],
                      "timestampSpec": {
                        "column": "__time",
                        "format": "millis",
                        "missingValue": null,
                      },
                      "transformSpec": {
                        "filter": null,
                        "transforms": [],
                      },
                    },
                    "tuningConfig": {
                      "maxNumWorkers": 1,
                      "maxRowsInMemory": 100000,
                      "rowsPerSegment": 3000000,
                    },
                    "type": "segmentGenerator",
                  },
                  "signature": [],
                },
                "stageNumber": 1,
              },
            ],
          },
          "startTime": 2024-07-26T18:04:59.873Z,
          "status": "FAILED",
          "usageInfo": undefined,
          "warnings": [
            {
              "error": {
                "errorCode": "CannotParseExternalData",
                "errorMessage": "Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 3, Line: 3)",
              },
              "exceptionStackTrace": "org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 3, Line: 3)
        	at org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:80)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:72)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$2.hasNext(CloseableIterator.java:93)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)
        	at org.apache.druid.msq.input.external.ExternalSegment$1$1.hasNext(ExternalSegment.java:94)
        	at org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)
        	at org.apache.druid.segment.RowWalker.advance(RowWalker.java:75)
        	at org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)
        	at org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:374)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeededWithExceptionHandling(ScanQueryFrameProcessor.java:334)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:273)
        	at org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:88)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:157)
        	at org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:75)
        	at org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:230)
        	at org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:138)
        	at org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:838)
        	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539)
        	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
        	at org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:259)
        	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        	at java.base/java.lang.Thread.run(Thread.java:840)
        Caused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input
         at [Source: (byte[])""; line: 1, column: 0]
        	at com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)
        	at com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4688)
        	at com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4586)
        	at com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3609)
        	at org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:75)
        	at org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)
        	at org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)
        	... 24 more
        ",
              "host": "localhost:8101",
              "stageNumber": 0,
              "taskId": "query-ea3e36df-ad67-4870-b136-f5616b17d9c4-worker0_0",
            },
            {
              "error": {
                "errorCode": "CannotParseExternalData",
                "errorMessage": "Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 6, Line: 7)",
              },
              "exceptionStackTrace": "org.apache.druid.java.util.common.parsers.ParseException: Unable to parse row [] (Path: https://static.imply.io/example-data/kttm-with-issues/kttm-blank-lines.json, Record: 6, Line: 7)
        	at org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:80)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$2.findNextIteratorIfNecessary(CloseableIterator.java:72)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$2.hasNext(CloseableIterator.java:93)
        	at org.apache.druid.java.util.common.parsers.CloseableIterator$1.hasNext(CloseableIterator.java:42)
        	at org.apache.druid.msq.input.external.ExternalSegment$1$1.hasNext(ExternalSegment.java:94)
        	at org.apache.druid.java.util.common.guava.BaseSequence$1.next(BaseSequence.java:115)
        	at org.apache.druid.segment.RowWalker.advance(RowWalker.java:75)
        	at org.apache.druid.segment.RowBasedCursor.advanceUninterruptibly(RowBasedCursor.java:110)
        	at org.apache.druid.segment.RowBasedCursor.advance(RowBasedCursor.java:103)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeeded(ScanQueryFrameProcessor.java:374)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.populateFrameWriterAndFlushIfNeededWithExceptionHandling(ScanQueryFrameProcessor.java:334)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runWithSegment(ScanQueryFrameProcessor.java:273)
        	at org.apache.druid.msq.querykit.BaseLeafFrameProcessor.runIncrementally(BaseLeafFrameProcessor.java:88)
        	at org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor.runIncrementally(ScanQueryFrameProcessor.java:157)
        	at org.apache.druid.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:75)
        	at org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:230)
        	at org.apache.druid.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:138)
        	at org.apache.druid.msq.exec.WorkerImpl$1$2.run(WorkerImpl.java:838)
        	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539)
        	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
        	at org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:259)
        	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        	at java.base/java.lang.Thread.run(Thread.java:840)
        Caused by: com.fasterxml.jackson.databind.exc.MismatchedInputException: No content to map due to end-of-input
         at [Source: (byte[])""; line: 1, column: 0]
        	at com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)
        	at com.fasterxml.jackson.databind.ObjectMapper._initForReading(ObjectMapper.java:4688)
        	at com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4586)
        	at com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3609)
        	at org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:75)
        	at org.apache.druid.data.input.impl.JsonLineReader.parseInputRows(JsonLineReader.java:48)
        	at org.apache.druid.data.input.IntermediateRowParsingReader$1.hasNext(IntermediateRowParsingReader.java:71)
        	... 24 more
        ",
              "host": "localhost:8101",
              "stageNumber": 0,
              "taskId": "query-ea3e36df-ad67-4870-b136-f5616b17d9c4-worker0_0",
            },
          ],
        }
      `);
    });
  });
});
