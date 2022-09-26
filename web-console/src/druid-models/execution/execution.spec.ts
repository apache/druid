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
import { EXECUTION_INGEST_COMPLETE } from './execution-ingest-complete.mock';

describe('Execution', () => {
  describe('.fromTaskDetail', () => {
    it('fails for bad status (error: null)', () => {
      expect(() =>
        Execution.fromTaskPayloadAndReport(
          {} as any,
          {
            asyncResultId: 'multi-stage-query-sql-1392d806-c17f-4937-94ee-8fa0a3ce1566',
            error: null,
          } as any,
        ),
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
              "groupId": "query-32ced762-7679-4a25-9220-3915c5976961",
              "id": "query-32ced762-7679-4a25-9220-3915c5976961",
              "resource": Object {
                "availabilityGroup": "query-32ced762-7679-4a25-9220-3915c5976961",
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
                    "finalize": false,
                    "finalizeAggregations": false,
                    "groupByEnableMultiValueUnnesting": false,
                    "scanSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
                    "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                    "sqlQueryId": "32ced762-7679-4a25-9220-3915c5976961",
                    "sqlReplaceTimeChunks": "all",
                  },
                  "dataSource": Object {
                    "inputFormat": Object {
                      "featureSpec": Object {},
                      "flattenSpec": null,
                      "keepNullColumns": false,
                      "type": "json",
                    },
                    "inputSource": Object {
                      "httpAuthenticationPassword": null,
                      "httpAuthenticationUsername": null,
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
        SELECT TIME_PARSE(\\"timestamp\\") AS \\"__time\\", agent_type
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_type\\",\\"type\\":\\"string\\"}]'
          )
        )
        PARTITIONED BY ALL TIME",
              "sqlQueryContext": Object {
                "finalizeAggregations": false,
                "groupByEnableMultiValueUnnesting": false,
                "maxParseExceptions": 0,
                "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                "sqlQueryId": "32ced762-7679-4a25-9220-3915c5976961",
                "sqlReplaceTimeChunks": "all",
              },
              "sqlTypeNames": Array [
                "TIMESTAMP",
                "VARCHAR",
              ],
              "type": "query_controller",
            },
            "task": "query-32ced762-7679-4a25-9220-3915c5976961",
          },
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
          "duration": 25097,
          "engine": "sql-msq-task",
          "error": undefined,
          "id": "query-32ced762-7679-4a25-9220-3915c5976961",
          "nativeQuery": Object {
            "columns": Array [
              "agent_type",
              "v0",
            ],
            "context": Object {
              "finalize": false,
              "finalizeAggregations": false,
              "groupByEnableMultiValueUnnesting": false,
              "scanSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
              "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
              "sqlQueryId": "32ced762-7679-4a25-9220-3915c5976961",
              "sqlReplaceTimeChunks": "all",
            },
            "dataSource": Object {
              "inputFormat": Object {
                "featureSpec": Object {},
                "flattenSpec": null,
                "keepNullColumns": false,
                "type": "json",
              },
              "inputSource": Object {
                "httpAuthenticationPassword": null,
                "httpAuthenticationUsername": null,
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
            "finalizeAggregations": false,
            "groupByEnableMultiValueUnnesting": false,
            "maxParseExceptions": 0,
          },
          "result": undefined,
          "sqlQuery": "REPLACE INTO \\"kttm_simple\\" OVERWRITE ALL
        SELECT TIME_PARSE(\\"timestamp\\") AS \\"__time\\", agent_type
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_type\\",\\"type\\":\\"string\\"}]'
          )
        )
        PARTITIONED BY ALL TIME",
          "stages": Stages {
            "counters": Object {
              "0": Object {
                "0": Object {
                  "input0": Object {
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
                },
              },
            },
            "stages": Array [
              Object {
                "definition": Object {
                  "id": "0b353011-6ea1-480a-8ca8-386771621672_0",
                  "input": Array [
                    Object {
                      "inputFormat": Object {
                        "featureSpec": Object {},
                        "flattenSpec": null,
                        "keepNullColumns": false,
                        "type": "json",
                      },
                      "inputSource": Object {
                        "httpAuthenticationPassword": null,
                        "httpAuthenticationUsername": null,
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
                        "finalize": false,
                        "finalizeAggregations": false,
                        "groupByEnableMultiValueUnnesting": false,
                        "scanSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
                        "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                        "sqlQueryId": "32ced762-7679-4a25-9220-3915c5976961",
                        "sqlReplaceTimeChunks": "all",
                      },
                      "dataSource": Object {
                        "inputNumber": 0,
                        "type": "inputNumber",
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
                "duration": 20229,
                "partitionCount": 1,
                "phase": "FINISHED",
                "sort": true,
                "stageNumber": 0,
                "startTime": "2022-08-22T20:12:53.790Z",
                "workerCount": 1,
              },
              Object {
                "definition": Object {
                  "id": "0b353011-6ea1-480a-8ca8-386771621672_1",
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
                "duration": 2497,
                "partitionCount": 1,
                "phase": "FINISHED",
                "stageNumber": 1,
                "startTime": "2022-08-22T20:13:13.991Z",
                "workerCount": 1,
              },
            ],
          },
          "startTime": 2022-08-22T20:12:51.391Z,
          "status": "SUCCESS",
          "warnings": undefined,
        }
      `);
    });
  });
});
