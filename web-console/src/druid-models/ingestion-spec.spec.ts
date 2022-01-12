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

import {
  adjustId,
  cleanSpec,
  getColumnTypeFromHeaderAndRows,
  guessInputFormat,
  guessTypeFromSample,
  IngestionSpec,
  updateSchemaWithSample,
  upgradeSpec,
} from './ingestion-spec';

describe('ingestion-spec', () => {
  it('upgrades / downgrades task spec 1', () => {
    const oldTaskSpec = {
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          firehose: {
            type: 'http',
            uris: ['https://website.com/wikipedia.json.gz'],
          },
        },
        tuningConfig: {
          type: 'index_parallel',
        },
        dataSchema: {
          dataSource: 'wikipedia',
          granularitySpec: {
            segmentGranularity: 'day',
            queryGranularity: 'hour',
            rollup: true,
          },
          parser: {
            type: 'string',
            parseSpec: {
              format: 'json',
              timestampSpec: {
                column: 'timestamp',
                format: 'iso',
              },
              dimensionsSpec: {
                dimensions: ['channel', 'cityName', 'comment'],
              },
              flattenSpec: {
                fields: [
                  {
                    type: 'path',
                    name: 'cityNameAlt',
                    expr: '$.cityName',
                  },
                ],
              },
            },
          },
          transformSpec: {
            transforms: [
              {
                type: 'expression',
                name: 'channel',
                expression: 'concat("channel", \'lol\')',
              },
            ],
            filter: {
              type: 'selector',
              dimension: 'commentLength',
              value: '35',
            },
          },
          metricsSpec: [
            {
              name: 'count',
              type: 'count',
            },
            {
              name: 'sum_added',
              type: 'longSum',
              fieldName: 'added',
            },
          ],
        },
      },
    };

    expect(upgradeSpec(oldTaskSpec)).toEqual({
      spec: {
        dataSchema: {
          dataSource: 'wikipedia',
          dimensionsSpec: {
            dimensions: ['channel', 'cityName', 'comment'],
          },
          granularitySpec: {
            queryGranularity: 'hour',
            rollup: true,
            segmentGranularity: 'day',
          },
          metricsSpec: [
            {
              name: 'count',
              type: 'count',
            },
            {
              fieldName: 'added',
              name: 'sum_added',
              type: 'longSum',
            },
          ],
          timestampSpec: {
            column: 'timestamp',
            format: 'iso',
          },
          transformSpec: {
            filter: {
              dimension: 'commentLength',
              type: 'selector',
              value: '35',
            },
            transforms: [
              {
                expression: 'concat("channel", \'lol\')',
                name: 'channel',
                type: 'expression',
              },
            ],
          },
        },
        ioConfig: {
          inputFormat: {
            flattenSpec: {
              fields: [
                {
                  expr: '$.cityName',
                  name: 'cityNameAlt',
                  type: 'path',
                },
              ],
            },
            type: 'json',
          },
          inputSource: {
            type: 'http',
            uris: ['https://website.com/wikipedia.json.gz'],
          },
          type: 'index_parallel',
        },
        tuningConfig: {
          type: 'index_parallel',
        },
      },
      type: 'index_parallel',
    });
  });

  it('upgrades / downgrades task spec (without parser)', () => {
    const oldTaskSpec = {
      type: 'index_parallel',
      ioConfig: {
        type: 'index_parallel',
        firehose: { type: 'http', uris: ['https://website.com/wikipedia.json.gz'] },
      },
      tuningConfig: { type: 'index_parallel' },
      dataSchema: {
        dataSource: 'new-data-source',
        granularitySpec: { type: 'uniform', segmentGranularity: 'DAY', queryGranularity: 'HOUR' },
      },
    };

    expect(upgradeSpec(oldTaskSpec)).toEqual({
      spec: {
        dataSchema: {
          dataSource: 'new-data-source',
          granularitySpec: {
            queryGranularity: 'HOUR',
            segmentGranularity: 'DAY',
            type: 'uniform',
          },
        },
        ioConfig: {
          inputSource: {
            type: 'http',
            uris: ['https://website.com/wikipedia.json.gz'],
          },
          type: 'index_parallel',
        },
        tuningConfig: {
          type: 'index_parallel',
        },
      },
      type: 'index_parallel',
    });
  });

  it('upgrades / downgrades supervisor spec', () => {
    const oldSupervisorSpec = {
      type: 'kafka',
      dataSchema: {
        dataSource: 'metrics-kafka',
        parser: {
          type: 'string',
          parseSpec: {
            format: 'json',
            timestampSpec: {
              column: 'timestamp',
              format: 'auto',
            },
            dimensionsSpec: {
              dimensions: [],
              dimensionExclusions: ['timestamp', 'value'],
            },
          },
        },
        metricsSpec: [
          {
            name: 'count',
            type: 'count',
          },
          {
            name: 'value_sum',
            fieldName: 'value',
            type: 'doubleSum',
          },
          {
            name: 'value_min',
            fieldName: 'value',
            type: 'doubleMin',
          },
          {
            name: 'value_max',
            fieldName: 'value',
            type: 'doubleMax',
          },
        ],
        granularitySpec: {
          type: 'uniform',
          segmentGranularity: 'HOUR',
          queryGranularity: 'NONE',
        },
      },
      tuningConfig: {
        type: 'kafka',
        maxRowsPerSegment: 5000000,
      },
      ioConfig: {
        topic: 'metrics',
        consumerProperties: {
          'bootstrap.servers': 'localhost:9092',
        },
        taskCount: 1,
        replicas: 1,
        taskDuration: 'PT1H',
      },
    };

    expect(upgradeSpec(oldSupervisorSpec)).toEqual({
      spec: {
        dataSchema: {
          dataSource: 'metrics-kafka',
          dimensionsSpec: {
            dimensionExclusions: ['timestamp', 'value'],
            dimensions: [],
          },
          granularitySpec: {
            queryGranularity: 'NONE',
            segmentGranularity: 'HOUR',
            type: 'uniform',
          },
          metricsSpec: [
            {
              name: 'count',
              type: 'count',
            },
            {
              fieldName: 'value',
              name: 'value_sum',
              type: 'doubleSum',
            },
            {
              fieldName: 'value',
              name: 'value_min',
              type: 'doubleMin',
            },
            {
              fieldName: 'value',
              name: 'value_max',
              type: 'doubleMax',
            },
          ],
          timestampSpec: {
            column: 'timestamp',
            format: 'auto',
          },
        },
        ioConfig: {
          consumerProperties: {
            'bootstrap.servers': 'localhost:9092',
          },
          inputFormat: {
            type: 'json',
          },
          replicas: 1,
          taskCount: 1,
          taskDuration: 'PT1H',
          topic: 'metrics',
        },
        tuningConfig: {
          maxRowsPerSegment: 5000000,
          type: 'kafka',
        },
      },
      type: 'kafka',
    });
  });

  it('cleanSpec', () => {
    expect(
      cleanSpec({
        type: 'index_parallel',
        id: 'index_parallel_coronavirus_hamlcmea_2020-03-19T00:56:12.175Z',
        groupId: 'index_parallel_coronavirus_hamlcmea_2020-03-19T00:56:12.175Z',
        resource: {
          availabilityGroup: 'index_parallel_coronavirus_hamlcmea_2020-03-19T00:56:12.175Z',
          requiredCapacity: 1,
        },
        spec: {
          dataSchema: {},
        },
      } as any),
    ).toEqual({
      type: 'index_parallel',
      spec: {
        dataSchema: {},
      },
    });
  });

  describe('guessInputFormat', () => {
    it('works for parquet', () => {
      expect(guessInputFormat(['PAR1lol']).type).toEqual('parquet');
    });

    it('works for orc', () => {
      expect(guessInputFormat(['ORClol']).type).toEqual('orc');
    });

    it('works for AVRO', () => {
      expect(guessInputFormat(['Obj\x01lol']).type).toEqual('avro_ocf');
      expect(guessInputFormat(['Obj1lol']).type).toEqual('regex');
    });

    it('works for JSON', () => {
      expect(guessInputFormat(['{"a":1}']).type).toEqual('json');
    });

    it('works for TSV', () => {
      expect(guessInputFormat(['A\tB\tX\tY']).type).toEqual('tsv');
    });

    it('works for CSV', () => {
      expect(guessInputFormat(['A,B,X,Y']).type).toEqual('csv');
    });

    it('works for TSV with ;', () => {
      const inputFormat = guessInputFormat(['A;B;X;Y']);
      expect(inputFormat.type).toEqual('tsv');
      expect(inputFormat.delimiter).toEqual(';');
    });

    it('works for TSV with |', () => {
      const inputFormat = guessInputFormat(['A|B|X|Y']);
      expect(inputFormat.type).toEqual('tsv');
      expect(inputFormat.delimiter).toEqual('|');
    });

    it('works for regex', () => {
      expect(guessInputFormat(['A/B/X/Y']).type).toEqual('regex');
    });
  });
});

describe('spec utils', () => {
  const ingestionSpec: IngestionSpec = {
    type: 'index_parallel',
    spec: {
      ioConfig: {
        type: 'index_parallel',
        inputSource: {
          type: 'http',
          uris: ['https://website.com/wikipedia.json.gz'],
        },
        inputFormat: {
          type: 'json',
        },
      },
      tuningConfig: {
        type: 'index_parallel',
      },
      dataSchema: {
        dataSource: 'wikipedia',
        granularitySpec: {
          segmentGranularity: 'day',
          queryGranularity: 'hour',
        },
        timestampSpec: {
          column: 'timestamp',
          format: 'iso',
        },
        dimensionsSpec: {},
      },
    },
  };

  it('guessTypeFromSample', () => {
    expect(guessTypeFromSample([])).toMatchInlineSnapshot(`"string"`);
  });

  it('getColumnTypeFromHeaderAndRows', () => {
    expect(
      getColumnTypeFromHeaderAndRows({ header: ['header'], rows: [] }, 'header'),
    ).toMatchInlineSnapshot(`"string"`);
  });

  it('updateSchemaWithSample', () => {
    const withRollup = updateSchemaWithSample(
      ingestionSpec,
      { header: ['header'], rows: [] },
      'specific',
      true,
    );

    expect(withRollup).toMatchInlineSnapshot(`
      Object {
        "spec": Object {
          "dataSchema": Object {
            "dataSource": "wikipedia",
            "dimensionsSpec": Object {
              "dimensions": Array [
                "header",
              ],
            },
            "granularitySpec": Object {
              "queryGranularity": "hour",
              "rollup": true,
              "segmentGranularity": "day",
            },
            "metricsSpec": Array [
              Object {
                "name": "count",
                "type": "count",
              },
            ],
            "timestampSpec": Object {
              "column": "timestamp",
              "format": "iso",
            },
          },
          "ioConfig": Object {
            "inputFormat": Object {
              "type": "json",
            },
            "inputSource": Object {
              "type": "http",
              "uris": Array [
                "https://website.com/wikipedia.json.gz",
              ],
            },
            "type": "index_parallel",
          },
          "tuningConfig": Object {
            "forceGuaranteedRollup": true,
            "partitionsSpec": Object {
              "type": "hashed",
            },
            "type": "index_parallel",
          },
        },
        "type": "index_parallel",
      }
    `);

    const noRollup = updateSchemaWithSample(
      ingestionSpec,
      { header: ['header'], rows: [] },
      'specific',
      false,
    );

    expect(noRollup).toMatchInlineSnapshot(`
      Object {
        "spec": Object {
          "dataSchema": Object {
            "dataSource": "wikipedia",
            "dimensionsSpec": Object {
              "dimensions": Array [
                "header",
              ],
            },
            "granularitySpec": Object {
              "queryGranularity": "none",
              "rollup": false,
              "segmentGranularity": "day",
            },
            "timestampSpec": Object {
              "column": "timestamp",
              "format": "iso",
            },
          },
          "ioConfig": Object {
            "inputFormat": Object {
              "type": "json",
            },
            "inputSource": Object {
              "type": "http",
              "uris": Array [
                "https://website.com/wikipedia.json.gz",
              ],
            },
            "type": "index_parallel",
          },
          "tuningConfig": Object {
            "partitionsSpec": Object {
              "type": "dynamic",
            },
            "type": "index_parallel",
          },
        },
        "type": "index_parallel",
      }
    `);
  });

  it('adjustId', () => {
    expect(adjustId('')).toEqual('');
    expect(adjustId('lol')).toEqual('lol');
    expect(adjustId('.l/o/l')).toEqual('lol');
    expect(adjustId('l\t \nl')).toEqual('l l');
  });
});
