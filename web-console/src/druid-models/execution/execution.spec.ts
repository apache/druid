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
          "_payload": Object {
            "payload": Object {
              "context": Object {
                "forceTimeChunkLock": true,
                "useLineageBasedSegmentAllocation": true,
              },
              "dataSource": "kttm_simple",
              "groupId": "query-93a855fa-c35a-48df-b596-6bc98eed1101",
              "id": "query-93a855fa-c35a-48df-b596-6bc98eed1101",
              "nativeTypeNames": Array [
                "LONG",
                "STRING",
              ],
              "resource": Object {
                "availabilityGroup": "query-93a855fa-c35a-48df-b596-6bc98eed1101",
                "requiredCapacity": 1,
              },
              "spec": Object {
                "assignmentStrategy": "max",
                "columnMappings": Array [
                  Object {
                    "outputColumn": "__time",
                    "queryColumn": "v0",
                  },
                  Object {
                    "outputColumn": "agent_type",
                    "queryColumn": "agent_type",
                  },
                ],
                "destination": Object {
                  "dataSource": "kttm_simple",
                  "replaceTimeChunks": Array [
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                  ],
                  "segmentGranularity": Object {
                    "type": "all",
                  },
                  "type": "dataSource",
                },
                "query": Object {
                  "columns": Array [
                    "agent_type",
                    "v0",
                  ],
                  "context": Object {
                    "__user": "allowAll",
                    "executionMode": "async",
                    "finalize": false,
                    "finalizeAggregations": false,
                    "groupByEnableMultiValueUnnesting": false,
                    "maxNumTasks": 2,
                    "maxParseExceptions": 0,
                    "queryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
                    "scanSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
                    "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                    "sqlQueryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
                    "sqlReplaceTimeChunks": "all",
                  },
                  "dataSource": Object {
                    "inputFormat": Object {
                      "assumeNewlineDelimited": false,
                      "keepNullColumns": false,
                      "type": "json",
                      "useJsonNodeReader": false,
                    },
                    "inputSource": Object {
                      "type": "http",
                      "uris": Array [
                        "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                      ],
                    },
                    "signature": Array [
                      Object {
                        "name": "timestamp",
                        "type": "STRING",
                      },
                      Object {
                        "name": "agent_type",
                        "type": "STRING",
                      },
                    ],
                    "type": "external",
                  },
                  "granularity": Object {
                    "type": "all",
                  },
                  "intervals": Object {
                    "intervals": Array [
                      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                    ],
                    "type": "intervals",
                  },
                  "legacy": false,
                  "queryType": "scan",
                  "resultFormat": "compactedList",
                  "virtualColumns": Array [
                    Object {
                      "expression": "timestamp_parse(\\"timestamp\\",null,'UTC')",
                      "name": "v0",
                      "outputType": "LONG",
                      "type": "expression",
                    },
                  ],
                },
                "tuningConfig": Object {
                  "maxNumWorkers": 1,
                  "maxRowsInMemory": 100000,
                  "rowsPerSegment": 3000000,
                },
              },
              "sqlQuery": "REPLACE INTO \\"kttm_simple\\" OVERWRITE ALL
        SELECT
          TIME_PARSE(\\"timestamp\\") AS \\"__time\\",
          \\"agent_type\\"
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}'
          )
        ) EXTEND (\\"timestamp\\" VARCHAR, \\"agent_type\\" VARCHAR)
        PARTITIONED BY ALL TIME",
              "sqlQueryContext": Object {
                "executionMode": "async",
                "finalizeAggregations": false,
                "groupByEnableMultiValueUnnesting": false,
                "maxNumTasks": 2,
                "queryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
                "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                "sqlQueryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
                "sqlReplaceTimeChunks": "all",
              },
              "sqlResultsContext": Object {
                "serializeComplexValues": true,
                "stringifyArrays": true,
                "timeZone": "UTC",
              },
              "sqlTypeNames": Array [
                "TIMESTAMP",
                "VARCHAR",
              ],
              "type": "query_controller",
            },
            "task": "query-93a855fa-c35a-48df-b596-6bc98eed1101",
          },
          "capacityInfo": undefined,
          "destination": Object {
            "dataSource": "kttm_simple",
            "numTotalRows": 465346,
            "replaceTimeChunks": Array [
              "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
            ],
            "segmentGranularity": Object {
              "type": "all",
            },
            "type": "dataSource",
          },
          "destinationPages": undefined,
          "duration": 23699,
          "engine": "sql-msq-task",
          "error": undefined,
          "id": "query-93a855fa-c35a-48df-b596-6bc98eed1101",
          "nativeQuery": Object {
            "columns": Array [
              "agent_type",
              "v0",
            ],
            "context": Object {
              "__user": "allowAll",
              "executionMode": "async",
              "finalize": false,
              "finalizeAggregations": false,
              "groupByEnableMultiValueUnnesting": false,
              "maxNumTasks": 2,
              "maxParseExceptions": 0,
              "queryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
              "scanSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
              "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
              "sqlQueryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
              "sqlReplaceTimeChunks": "all",
            },
            "dataSource": Object {
              "inputFormat": Object {
                "assumeNewlineDelimited": false,
                "keepNullColumns": false,
                "type": "json",
                "useJsonNodeReader": false,
              },
              "inputSource": Object {
                "type": "http",
                "uris": Array [
                  "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                ],
              },
              "signature": Array [
                Object {
                  "name": "timestamp",
                  "type": "STRING",
                },
                Object {
                  "name": "agent_type",
                  "type": "STRING",
                },
              ],
              "type": "external",
            },
            "granularity": Object {
              "type": "all",
            },
            "intervals": Object {
              "intervals": Array [
                "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
              ],
              "type": "intervals",
            },
            "legacy": false,
            "queryType": "scan",
            "resultFormat": "compactedList",
            "virtualColumns": Array [
              Object {
                "expression": "timestamp_parse(\\"timestamp\\",null,'UTC')",
                "name": "v0",
                "outputType": "LONG",
                "type": "expression",
              },
            ],
          },
          "queryContext": Object {
            "executionMode": "async",
            "finalizeAggregations": false,
            "groupByEnableMultiValueUnnesting": false,
            "maxNumTasks": 2,
          },
          "result": undefined,
          "sqlQuery": "REPLACE INTO \\"kttm_simple\\" OVERWRITE ALL
        SELECT
          TIME_PARSE(\\"timestamp\\") AS \\"__time\\",
          \\"agent_type\\"
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}'
          )
        ) EXTEND (\\"timestamp\\" VARCHAR, \\"agent_type\\" VARCHAR)
        PARTITIONED BY ALL TIME",
          "stages": Stages {
            "counters": Object {
              "0": Object {
                "0": Object {
                  "input0": Object {
                    "bytes": Array [
                      360464067,
                    ],
                    "files": Array [
                      1,
                    ],
                    "rows": Array [
                      465346,
                    ],
                    "totalFiles": Array [
                      1,
                    ],
                    "type": "channel",
                  },
                  "output": Object {
                    "bytes": Array [
                      25430674,
                    ],
                    "frames": Array [
                      4,
                    ],
                    "rows": Array [
                      465346,
                    ],
                    "type": "channel",
                  },
                  "shuffle": Object {
                    "bytes": Array [
                      23570446,
                    ],
                    "frames": Array [
                      38,
                    ],
                    "rows": Array [
                      465346,
                    ],
                    "type": "channel",
                  },
                  "sortProgress": Object {
                    "levelToMergedBatches": Object {
                      "0": 1,
                      "1": 1,
                      "2": 1,
                    },
                    "levelToTotalBatches": Object {
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
              "1": Object {
                "0": Object {
                  "input0": Object {
                    "bytes": Array [
                      23570446,
                    ],
                    "frames": Array [
                      38,
                    ],
                    "rows": Array [
                      465346,
                    ],
                    "type": "channel",
                  },
                  "segmentGenerationProgress": Object {
                    "rowsMerged": 465346,
                    "rowsPersisted": 465346,
                    "rowsProcessed": 465346,
                    "rowsPushed": 465346,
                    "type": "segmentGenerationProgress",
                  },
                },
              },
            },
            "stages": Array [
              Object {
                "definition": Object {
                  "id": "ad318360-2ccf-4afc-b221-27c8704bf4fe_0",
                  "input": Array [
                    Object {
                      "inputFormat": Object {
                        "assumeNewlineDelimited": false,
                        "keepNullColumns": false,
                        "type": "json",
                        "useJsonNodeReader": false,
                      },
                      "inputSource": Object {
                        "type": "http",
                        "uris": Array [
                          "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                        ],
                      },
                      "signature": Array [
                        Object {
                          "name": "timestamp",
                          "type": "STRING",
                        },
                        Object {
                          "name": "agent_type",
                          "type": "STRING",
                        },
                      ],
                      "type": "external",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": Object {
                    "query": Object {
                      "columns": Array [
                        "agent_type",
                        "v0",
                      ],
                      "context": Object {
                        "__timeColumn": "v0",
                        "__user": "allowAll",
                        "executionMode": "async",
                        "finalize": false,
                        "finalizeAggregations": false,
                        "groupByEnableMultiValueUnnesting": false,
                        "maxNumTasks": 2,
                        "maxParseExceptions": 0,
                        "queryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
                        "scanSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
                        "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                        "sqlQueryId": "93a855fa-c35a-48df-b596-6bc98eed1101",
                        "sqlReplaceTimeChunks": "all",
                      },
                      "dataSource": Object {
                        "inputFormat": Object {
                          "assumeNewlineDelimited": false,
                          "keepNullColumns": false,
                          "type": "json",
                          "useJsonNodeReader": false,
                        },
                        "inputSource": Object {
                          "type": "http",
                          "uris": Array [
                            "https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz",
                          ],
                        },
                        "signature": Array [
                          Object {
                            "name": "timestamp",
                            "type": "STRING",
                          },
                          Object {
                            "name": "agent_type",
                            "type": "STRING",
                          },
                        ],
                        "type": "external",
                      },
                      "granularity": Object {
                        "type": "all",
                      },
                      "intervals": Object {
                        "intervals": Array [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "type": "intervals",
                      },
                      "legacy": false,
                      "queryType": "scan",
                      "resultFormat": "compactedList",
                      "virtualColumns": Array [
                        Object {
                          "expression": "timestamp_parse(\\"timestamp\\",null,'UTC')",
                          "name": "v0",
                          "outputType": "LONG",
                          "type": "expression",
                        },
                      ],
                    },
                    "type": "scan",
                  },
                  "shuffleCheckHasMultipleValues": true,
                  "shuffleSpec": Object {
                    "clusterBy": Object {
                      "columns": Array [
                        Object {
                          "columnName": "__boost",
                          "order": "ASCENDING",
                        },
                      ],
                    },
                    "targetSize": 3000000,
                    "type": "targetSize",
                  },
                  "signature": Array [
                    Object {
                      "name": "__boost",
                      "type": "LONG",
                    },
                    Object {
                      "name": "agent_type",
                      "type": "STRING",
                    },
                    Object {
                      "name": "v0",
                      "type": "LONG",
                    },
                  ],
                },
                "duration": 21324,
                "partitionCount": 1,
                "phase": "FINISHED",
                "sort": true,
                "stageNumber": 0,
                "startTime": "2023-08-01T03:12:59.865Z",
                "workerCount": 1,
              },
              Object {
                "definition": Object {
                  "id": "ad318360-2ccf-4afc-b221-27c8704bf4fe_1",
                  "input": Array [
                    Object {
                      "stage": 0,
                      "type": "stage",
                    },
                  ],
                  "maxWorkerCount": 1,
                  "processor": Object {
                    "columnMappings": Array [
                      Object {
                        "outputColumn": "__time",
                        "queryColumn": "v0",
                      },
                      Object {
                        "outputColumn": "agent_type",
                        "queryColumn": "agent_type",
                      },
                    ],
                    "dataSchema": Object {
                      "dataSource": "kttm_simple",
                      "dimensionsSpec": Object {
                        "dimensionExclusions": Array [
                          "__time",
                        ],
                        "dimensions": Array [
                          Object {
                            "createBitmapIndex": true,
                            "multiValueHandling": "SORTED_ARRAY",
                            "name": "agent_type",
                            "type": "string",
                          },
                        ],
                        "includeAllDimensions": false,
                        "useSchemaDiscovery": false,
                      },
                      "granularitySpec": Object {
                        "intervals": Array [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "queryGranularity": Object {
                          "type": "none",
                        },
                        "rollup": false,
                        "type": "arbitrary",
                      },
                      "metricsSpec": Array [],
                      "timestampSpec": Object {
                        "column": "__time",
                        "format": "millis",
                        "missingValue": null,
                      },
                      "transformSpec": Object {
                        "filter": null,
                        "transforms": Array [],
                      },
                    },
                    "tuningConfig": Object {
                      "maxNumWorkers": 1,
                      "maxRowsInMemory": 100000,
                      "rowsPerSegment": 3000000,
                    },
                    "type": "segmentGenerator",
                  },
                  "signature": Array [],
                },
                "duration": 2070,
                "partitionCount": 1,
                "phase": "FINISHED",
                "stageNumber": 1,
                "startTime": "2023-08-01T03:13:21.156Z",
                "workerCount": 1,
              },
            ],
          },
          "startTime": 2023-08-01T03:12:59.527Z,
          "status": "SUCCESS",
          "usageInfo": Object {
            "pendingTasks": 0,
            "runningTasks": 2,
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
          "destination": Object {
            "numTotalRows": 2,
            "type": "taskReport",
          },
          "destinationPages": Array [
            Object {
              "id": 0,
              "numRows": 2,
              "sizeInBytes": 116,
            },
          ],
          "duration": 29168,
          "engine": "sql-msq-task",
          "error": undefined,
          "id": "query-ad84d20a-c331-4ee9-ac59-83024e369cf1",
          "nativeQuery": undefined,
          "queryContext": undefined,
          "result": _QueryResult {
            "header": Array [
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
            "rows": Array [
              Array [
                "#en.wikipedia",
                6650,
              ],
              Array [
                "#sh.wikipedia",
                3969,
              ],
            ],
            "sqlQuery": undefined,
            "sqlQueryId": undefined,
          },
          "sqlQuery": undefined,
          "stages": undefined,
          "startTime": 2023-07-05T21:33:19.147Z,
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
          "duration": 11217,
          "engine": "sql-msq-task",
          "error": Object {
            "error": Object {
              "category": "UNCATEGORIZED",
              "context": Object {
                "message": "java.io.UncheckedIOException: /",
              },
              "error": "druidException",
              "errorCode": "UnknownError",
              "errorMessage": "java.io.UncheckedIOException: /",
              "persona": "USER",
            },
            "taskId": "query-36ea273a-bd6d-48de-b890-2d853d879bf8",
          },
          "id": "query-36ea273a-bd6d-48de-b890-2d853d879bf8",
          "nativeQuery": undefined,
          "queryContext": undefined,
          "result": undefined,
          "sqlQuery": undefined,
          "stages": undefined,
          "startTime": 2023-07-05T21:40:39.986Z,
          "status": "FAILED",
          "usageInfo": undefined,
          "warnings": undefined,
        }
      `);
    });
  });
});
