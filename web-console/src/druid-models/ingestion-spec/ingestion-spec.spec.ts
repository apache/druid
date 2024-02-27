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

import { CSV_SAMPLE, JSON_SAMPLE } from '../../utils/sampler.mock';

import type { IngestionSpec } from './ingestion-spec';
import {
  adjustId,
  cleanSpec,
  guessColumnTypeFromInput,
  guessColumnTypeFromSampleResponse,
  guessSimpleInputFormat,
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

  it('does not mangle a custom parser', () => {
    expect(() =>
      upgradeSpec({
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
              type: 'super_cool_custom_parser',
            },
          },
        },
      }),
    ).toThrow(
      "Can not rewrite parser of type 'super_cool_custom_parser', only 'string' is supported",
    );
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

  it('upgrades / downgrades back compat supervisor spec', () => {
    const backCompatSupervisorSpec = {
      type: 'kafka',
      spec: {
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
      },
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

    expect(cleanSpec(upgradeSpec(backCompatSupervisorSpec))).toEqual({
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

  describe('guessSimpleInputFormat', () => {
    it('works for parquet', () => {
      expect(guessSimpleInputFormat(['PAR1lol']).type).toEqual('parquet');
    });

    it('works for orc', () => {
      expect(guessSimpleInputFormat(['ORClol']).type).toEqual('orc');
    });

    it('works for AVRO', () => {
      expect(guessSimpleInputFormat(['Obj\x01lol']).type).toEqual('avro_ocf');
      expect(guessSimpleInputFormat(['Obj1lol']).type).toEqual('regex');
    });

    it('works for JSON (strict)', () => {
      expect(guessSimpleInputFormat(['{"a":1}'])).toEqual({ type: 'json' });
    });

    it('works for JSON (lax)', () => {
      expect(guessSimpleInputFormat([`{hello:'world'}`])).toEqual({
        type: 'json',
        featureSpec: {
          ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER: true,
          ALLOW_COMMENTS: true,
          ALLOW_MISSING_VALUES: true,
          ALLOW_NON_NUMERIC_NUMBERS: true,
          ALLOW_NUMERIC_LEADING_ZEROS: true,
          ALLOW_SINGLE_QUOTES: true,
          ALLOW_TRAILING_COMMA: true,
          ALLOW_UNQUOTED_CONTROL_CHARS: true,
          ALLOW_UNQUOTED_FIELD_NAMES: true,
          ALLOW_YAML_COMMENTS: true,
        },
      });
    });

    it('works for CSV (with header)', () => {
      expect(guessSimpleInputFormat(['A,B,"X,1",Y'])).toEqual({
        type: 'csv',
        findColumnsFromHeader: true,
      });
    });

    it('works for CSV (no header)', () => {
      expect(guessSimpleInputFormat(['"A,1","B,2",1,2'])).toEqual({
        type: 'csv',
        findColumnsFromHeader: false,
        columns: ['column1', 'column2', 'column3', 'column4'],
      });
    });

    it('works for TSV (with header)', () => {
      expect(guessSimpleInputFormat(['A\tB\tX\tY'])).toEqual({
        type: 'tsv',
        findColumnsFromHeader: true,
      });
    });

    it('works for TSV (no header)', () => {
      expect(guessSimpleInputFormat(['A\tB\t1\t2\t3\t4\t5\t6\t7\t8\t9'])).toEqual({
        type: 'tsv',
        findColumnsFromHeader: false,
        columns: [
          'column01',
          'column02',
          'column03',
          'column04',
          'column05',
          'column06',
          'column07',
          'column08',
          'column09',
          'column10',
          'column11',
        ],
      });
    });

    it('works for TSV with ;', () => {
      const inputFormat = guessSimpleInputFormat(['A;B;X;Y']);
      expect(inputFormat).toEqual({
        type: 'tsv',
        delimiter: ';',
        findColumnsFromHeader: true,
      });
    });

    it('works for TSV with |', () => {
      const inputFormat = guessSimpleInputFormat(['A|B|X|Y']);
      expect(inputFormat).toEqual({
        type: 'tsv',
        delimiter: '|',
        findColumnsFromHeader: true,
      });
    });

    it('works for regex', () => {
      expect(guessSimpleInputFormat(['A/B/X/Y'])).toEqual({
        type: 'regex',
        pattern: '([\\s\\S]*)',
        columns: ['line'],
      });
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

  describe('guessColumnTypeFromInput', () => {
    it('works for empty', () => {
      expect(guessColumnTypeFromInput([], false)).toEqual('string');
    });

    it('works for long', () => {
      expect(guessColumnTypeFromInput([null, 1, 2, 3], false)).toEqual('long');
      expect(guessColumnTypeFromInput([null, 1, 2, 3], true)).toEqual('long');
      expect(guessColumnTypeFromInput([null, '1', '2', '3'], false)).toEqual('string');
      expect(guessColumnTypeFromInput([null, '1', '2', '3'], true)).toEqual('long');
    });

    it('works for double', () => {
      expect(guessColumnTypeFromInput([null, 1, 2.1, 3], false)).toEqual('double');
      expect(guessColumnTypeFromInput([null, 1, 2.1, 3], true)).toEqual('double');
      expect(guessColumnTypeFromInput([null, '1', '2.1', '3'], false)).toEqual('string');
      expect(guessColumnTypeFromInput([null, '1', '2.1', '3'], true)).toEqual('double');
    });

    it('works for ARRAY<string>', () => {
      expect(
        guessColumnTypeFromInput(
          [
            ['A', 'B'],
            ['A', 'C'],
          ],
          false,
        ),
      ).toEqual('ARRAY<string>');
    });

    it('works for ARRAY<long>', () => {
      expect(
        guessColumnTypeFromInput(
          [
            [1, 2],
            [3, 4],
          ],
          false,
        ),
      ).toEqual('ARRAY<long>');

      expect(
        guessColumnTypeFromInput(
          [
            ['1', '2'],
            ['3', '4'],
          ],
          false,
        ),
      ).toEqual('ARRAY<string>');

      expect(
        guessColumnTypeFromInput(
          [
            ['1', '2'],
            ['3', '4'],
          ],
          true,
        ),
      ).toEqual('ARRAY<long>');
    });

    it('works for ARRAY<double>', () => {
      expect(
        guessColumnTypeFromInput(
          [
            [1.1, 2.2],
            [3.3, 4.4],
          ],
          false,
        ),
      ).toEqual('ARRAY<double>');

      expect(
        guessColumnTypeFromInput(
          [
            ['1.1', '2.2'],
            ['3.3', '4.4'],
          ],
          false,
        ),
      ).toEqual('ARRAY<string>');

      expect(
        guessColumnTypeFromInput(
          [
            ['1.1', '2.2'],
            ['3.3', '4.4'],
          ],
          true,
        ),
      ).toEqual('ARRAY<double>');
    });

    it('works for complex arrays', () => {
      expect(guessColumnTypeFromInput([{ type: 'Dogs' }, { type: 'JavaScript' }], false)).toEqual(
        'COMPLEX<json>',
      );
    });

    it('works for strange json', () => {
      expect(guessColumnTypeFromInput([1, { hello: 'world' }, 3], false)).toEqual('COMPLEX<json>');
    });

    it('works for strange input (object with no prototype)', () => {
      expect(guessColumnTypeFromInput([1, Object.create(null), 3], false)).toEqual('COMPLEX<json>');
    });
  });

  describe('guessColumnTypeFromSampleResponse', () => {
    it('works for generic dataset', () => {
      expect(guessColumnTypeFromSampleResponse(CSV_SAMPLE, 'user', false)).toEqual('string');
      expect(guessColumnTypeFromSampleResponse(CSV_SAMPLE, 'followers', false)).toEqual('string');
      expect(guessColumnTypeFromSampleResponse(CSV_SAMPLE, 'followers', true)).toEqual('long');
      expect(guessColumnTypeFromSampleResponse(CSV_SAMPLE, 'spend', true)).toEqual('double');
      expect(guessColumnTypeFromSampleResponse(CSV_SAMPLE, 'nums', false)).toEqual('ARRAY<string>');
      expect(guessColumnTypeFromSampleResponse(CSV_SAMPLE, 'nums', true)).toEqual('ARRAY<long>');
    });
  });

  describe('updateSchemaWithSample', () => {
    it('works with rollup, arrays', () => {
      const updateSpec = updateSchemaWithSample(
        ingestionSpec,
        JSON_SAMPLE,
        'fixed',
        'arrays',
        true,
      );
      expect(updateSpec.spec).toMatchInlineSnapshot(`
        Object {
          "dataSchema": Object {
            "dataSource": "wikipedia",
            "dimensionsSpec": Object {
              "dimensions": Array [
                "user",
                "id",
                Object {
                  "castToType": "ARRAY<STRING>",
                  "name": "tags",
                  "type": "auto",
                },
                Object {
                  "castToType": "ARRAY<LONG>",
                  "name": "nums",
                  "type": "auto",
                },
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
              Object {
                "fieldName": "followers",
                "name": "sum_followers",
                "type": "longSum",
              },
              Object {
                "fieldName": "spend",
                "name": "sum_spend",
                "type": "doubleSum",
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
        }
      `);
    });

    it('works with rollup, MVDs', () => {
      const updateSpec = updateSchemaWithSample(
        ingestionSpec,
        JSON_SAMPLE,
        'fixed',
        'multi-values',
        true,
      );
      expect(updateSpec.spec).toMatchInlineSnapshot(`
        Object {
          "dataSchema": Object {
            "dataSource": "wikipedia",
            "dimensionsSpec": Object {
              "dimensions": Array [
                "user",
                "id",
                Object {
                  "multiValueHandling": "SORTED_ARRAY",
                  "name": "tags",
                  "type": "string",
                },
                Object {
                  "multiValueHandling": "SORTED_ARRAY",
                  "name": "nums",
                  "type": "string",
                },
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
              Object {
                "fieldName": "followers",
                "name": "sum_followers",
                "type": "longSum",
              },
              Object {
                "fieldName": "spend",
                "name": "sum_spend",
                "type": "doubleSum",
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
        }
      `);
    });

    it('works without rollup, arrays', () => {
      const updatedSpec = updateSchemaWithSample(
        ingestionSpec,
        JSON_SAMPLE,
        'fixed',
        'arrays',
        false,
      );
      expect(updatedSpec.spec).toMatchInlineSnapshot(`
        Object {
          "dataSchema": Object {
            "dataSource": "wikipedia",
            "dimensionsSpec": Object {
              "dimensions": Array [
                "user",
                Object {
                  "name": "followers",
                  "type": "long",
                },
                Object {
                  "name": "spend",
                  "type": "double",
                },
                "id",
                Object {
                  "castToType": "ARRAY<STRING>",
                  "name": "tags",
                  "type": "auto",
                },
                Object {
                  "castToType": "ARRAY<LONG>",
                  "name": "nums",
                  "type": "auto",
                },
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
        }
      `);
    });

    it('works without rollup, MVDs', () => {
      const updatedSpec = updateSchemaWithSample(
        ingestionSpec,
        JSON_SAMPLE,
        'fixed',
        'multi-values',
        false,
      );
      expect(updatedSpec.spec).toMatchInlineSnapshot(`
        Object {
          "dataSchema": Object {
            "dataSource": "wikipedia",
            "dimensionsSpec": Object {
              "dimensions": Array [
                "user",
                Object {
                  "name": "followers",
                  "type": "long",
                },
                Object {
                  "name": "spend",
                  "type": "double",
                },
                "id",
                Object {
                  "multiValueHandling": "SORTED_ARRAY",
                  "name": "tags",
                  "type": "string",
                },
                Object {
                  "multiValueHandling": "SORTED_ARRAY",
                  "name": "nums",
                  "type": "string",
                },
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
        }
      `);
    });
  });

  it('adjustId', () => {
    expect(adjustId('')).toEqual('');
    expect(adjustId('lol')).toEqual('lol');
    expect(adjustId('.l/o/l')).toEqual('lol');
    expect(adjustId('l\t \nl')).toEqual('l l');
  });
});
