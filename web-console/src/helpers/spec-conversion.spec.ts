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

import { convertSpecToSql } from './spec-conversion';

expect.addSnapshotSerializer({
  test: val => typeof val === 'string',
  print: String,
});

describe('spec conversion', () => {
  it('converts index_parallel spec (without rollup)', () => {
    const converted = convertSpecToSql({
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
          },
          inputFormat: {
            type: 'json',
          },
        },
        dataSchema: {
          granularitySpec: {
            segmentGranularity: 'hour',
            queryGranularity: 'none',
            rollup: false,
          },
          dataSource: 'wikipedia',
          transformSpec: {
            filter: {
              type: 'not',
              field: {
                type: 'selector',
                dimension: 'channel',
                value: 'xxx',
              },
            },
          },
          timestampSpec: {
            column: 'timestamp',
            format: 'auto',
          },
          dimensionsSpec: {
            dimensions: [
              'isRobot',
              'channel',
              'flags',
              'isUnpatrolled',
              'page',
              'diffUrl',
              {
                type: 'long',
                name: 'added',
              },
              'comment',
              {
                type: 'long',
                name: 'commentLength',
              },
              'isNew',
              'isMinor',
              {
                type: 'long',
                name: 'delta',
              },
              'isAnonymous',
              'user',
              {
                type: 'long',
                name: 'deltaBucket',
              },
              {
                type: 'long',
                name: 'deleted',
              },
              'namespace',
              'cityName',
              'countryName',
              'regionIsoCode',
              'metroCode',
              'countryIsoCode',
              'regionName',
              { name: 'event', type: 'json' },
            ],
          },
        },
        tuningConfig: {
          type: 'index_parallel',
          partitionsSpec: {
            type: 'single_dim',
            partitionDimension: 'isRobot',
            targetRowsPerSegment: 150000,
          },
          indexSpec: {
            dimensionCompression: 'lzf',
          },
          forceGuaranteedRollup: true,
          maxNumConcurrentSubTasks: 4,
          maxParseExceptions: 3,
        },
      },
    });

    expect(converted.queryString).toMatchSnapshot();

    expect(converted.queryContext).toEqual({
      arrayIngestMode: 'array',
      groupByEnableMultiValueUnnesting: false,
      maxParseExceptions: 3,
      finalizeAggregations: false,
      maxNumTasks: 5,
      indexSpec: {
        dimensionCompression: 'lzf',
      },
    });
  });

  it('converts index_parallel spec (with rollup)', () => {
    const converted = convertSpecToSql({
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
          },
          inputFormat: {
            type: 'json',
          },
        },
        dataSchema: {
          granularitySpec: {
            segmentGranularity: 'hour',
            queryGranularity: 'hour',
          },
          dataSource: 'wikipedia_rollup',
          timestampSpec: {
            column: 'timestamp',
            format: 'iso',
          },
          dimensionsSpec: {
            dimensions: [
              'isRobot',
              'channel',
              'flags',
              'isUnpatrolled',
              'comment',
              'isNew',
              'isMinor',
              'isAnonymous',
              'user',
              'namespace',
              'cityName',
              'countryName',
              'regionIsoCode',
              'metroCode',
              'countryIsoCode',
              'regionName',
            ],
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
            {
              name: 'sum_commentLength',
              type: 'longSum',
              fieldName: 'commentLength',
            },
            {
              name: 'max_commentLength',
              type: 'longMax',
              fieldName: 'commentLength',
            },
            {
              name: 'sum_delta',
              type: 'longSum',
              fieldName: 'delta',
            },
            {
              name: 'sum_deltaBucket',
              type: 'longSum',
              fieldName: 'deltaBucket',
            },
            {
              name: 'sum_deleted',
              type: 'longSum',
              fieldName: 'deleted',
            },
            {
              name: 'page_theta',
              type: 'thetaSketch',
              fieldName: 'page',
            },
          ],
        },
        tuningConfig: {
          type: 'index_parallel',
          partitionsSpec: {
            type: 'hashed',
          },
          forceGuaranteedRollup: true,
        },
      },
    });

    expect(converted.queryString).toMatchSnapshot();

    expect(converted.queryContext).toEqual({
      arrayIngestMode: 'array',
      groupByEnableMultiValueUnnesting: false,
      finalizeAggregations: false,
    });
  });

  it('converts index_hadoop spec (with rollup)', () => {
    const converted = convertSpecToSql({
      type: 'index_hadoop',
      spec: {
        dataSchema: {
          dataSource: 'newSource',
          hadoopDependencyCoordinates: [
            'org.apache.hadoop:hadoop-client:2.7.3',
            'org.apache.hadoop:hadoop-aws:2.7.3',
          ],
          parser: {
            type: 'parquet',
            parseSpec: {
              format: 'timeAndDims',
              timestampSpec: {
                column: 'event_ts',
                format: 'auto',
              },
              columns: [
                'col1',
                'col2',
                'col3',
                'col4',
                'metric1',
                'metric2',
                'metric3',
                'metric4',
                'metric5',
                'metric6',
                'metric7',
              ],
              dimensionsSpec: {
                dimensions: ['col1', 'col2', 'col3', 'col4'],
                dimensionExclusions: [],
                spatialDimensions: [],
              },
            },
          },
          metricsSpec: [
            {
              type: 'doubleSum',
              name: 'metric1',
              fieldName: 'field1',
            },
            {
              type: 'doubleMax',
              name: 'metric2',
              fieldName: 'field2',
            },
            {
              type: 'doubleMin',
              name: 'metric3',
              fieldName: 'field3',
            },
            {
              type: 'hyperUnique',
              name: 'metric4',
              fieldName: 'field4',
              isInputHyperUnique: true,
            },
            {
              type: 'hyperUnique',
              name: 'metric5',
              fieldName: 'field5',
              isInputHyperUnique: true,
            },
            {
              type: 'longSum',
              name: 'metric6',
              fieldName: 'field6',
            },
            {
              type: 'doubleSum',
              name: 'metric7',
              fieldName: 'field7',
            },
          ],
          granularitySpec: {
            type: 'uniform',
            segmentGranularity: 'HOUR',
            queryGranularity: 'HOUR',
            intervals: ['2022-08-01/2022-08-02'],
            rollup: true,
          },
          transformSpec: {
            filter: {
              type: 'selector',
              dimension: 'col2',
              value: 'xxx',
            },
          },
        },
        ioConfig: {
          type: 'hadoop',
          inputSpec: {
            type: 'static',
            inputFormat: 'org.apache.druid.data.input.parquet.DruidParquetInputFormat',
            paths: 's3://path',
          },
        },
        tuningConfig: {
          type: 'hadoop',
          partitionsSpec: {
            type: 'hashed',
            targetPartitionSize: 3000000,
          },
          forceExtendableShardSpecs: true,
          jobProperties: {
            'mapreduce.job.classloader': 'true',
            'mapreduce.map.memory.mb': '8192',
            'mapreduce.reduce.memory.mb': '18288',
          },
        },
      },
    });

    expect(converted.queryString).toMatchSnapshot();

    expect(converted.queryContext).toEqual({
      arrayIngestMode: 'array',
      groupByEnableMultiValueUnnesting: false,
      finalizeAggregations: false,
    });
  });

  it('converts with issue when there is a __time transform', () => {
    const converted = convertSpecToSql({
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
          },
          inputFormat: {
            type: 'json',
          },
        },
        dataSchema: {
          granularitySpec: {
            segmentGranularity: 'hour',
            queryGranularity: 'none',
            rollup: false,
          },
          dataSource: 'wikipedia',
          transformSpec: {
            transforms: [{ name: '__time', expression: '_some_time_parse_expression_' }],
          },
          timestampSpec: {
            column: 'timestamp',
            format: 'auto',
          },
          dimensionsSpec: {
            dimensions: [
              'isRobot',
              'channel',
              'flags',
              'isUnpatrolled',
              'page',
              'diffUrl',
              {
                type: 'long',
                name: 'added',
              },
              'comment',
              {
                type: 'long',
                name: 'commentLength',
              },
              'isNew',
              'isMinor',
              {
                type: 'long',
                name: 'delta',
              },
              'isAnonymous',
              'user',
              {
                type: 'long',
                name: 'deltaBucket',
              },
              {
                type: 'long',
                name: 'deleted',
              },
              'namespace',
              'cityName',
              'countryName',
              'regionIsoCode',
              'metroCode',
              'countryIsoCode',
              'regionName',
            ],
          },
        },
        tuningConfig: {
          type: 'index_parallel',
          partitionsSpec: {
            type: 'single_dim',
            partitionDimension: 'isRobot',
            targetRowsPerSegment: 150000,
          },
          forceGuaranteedRollup: true,
          maxNumConcurrentSubTasks: 4,
          maxParseExceptions: 3,
        },
      },
    });

    expect(converted.queryString).toMatchSnapshot();
  });

  it('converts with when the __time column is used as the __time column', () => {
    const converted = convertSpecToSql({
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
          },
          inputFormat: {
            type: 'json',
          },
        },
        dataSchema: {
          granularitySpec: {
            segmentGranularity: 'hour',
            queryGranularity: 'none',
            rollup: false,
          },
          dataSource: 'wikipedia',
          timestampSpec: {
            column: '__time',
            format: 'millis',
          },
          dimensionsSpec: {
            dimensions: ['isRobot', 'channel', 'flags'],
          },
        },
        tuningConfig: {
          type: 'index_parallel',
          partitionsSpec: {
            type: 'single_dim',
            partitionDimension: 'isRobot',
            targetRowsPerSegment: 150000,
          },
          forceGuaranteedRollup: true,
          maxNumConcurrentSubTasks: 4,
          maxParseExceptions: 3,
        },
      },
    });

    expect(converted.queryString).toMatchSnapshot();
  });

  it('converts with issue when there is a dimension transform and strange filter', () => {
    const converted = convertSpecToSql({
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
          },
          inputFormat: {
            type: 'json',
          },
        },
        dataSchema: {
          granularitySpec: {
            segmentGranularity: 'hour',
            queryGranularity: 'none',
            rollup: false,
          },
          dataSource: 'wikipedia',
          transformSpec: {
            transforms: [{ name: 'comment', expression: '_some_expression_' }],
            filter: {
              type: 'strange',
            },
          },
          timestampSpec: {
            column: 'timestamp',
            format: 'auto',
          },
          dimensionsSpec: {
            dimensions: [
              'isRobot',
              'channel',
              'flags',
              'isUnpatrolled',
              'page',
              'diffUrl',
              {
                type: 'long',
                name: 'added',
              },
              'comment',
              {
                type: 'long',
                name: 'commentLength',
              },
              'isNew',
              'isMinor',
              {
                type: 'long',
                name: 'delta',
              },
              'isAnonymous',
              'user',
              {
                type: 'long',
                name: 'deltaBucket',
              },
              {
                type: 'long',
                name: 'deleted',
              },
              'namespace',
              'cityName',
              'countryName',
              'regionIsoCode',
              'metroCode',
              'countryIsoCode',
              'regionName',
            ],
          },
        },
        tuningConfig: {
          type: 'index_parallel',
          partitionsSpec: {
            type: 'single_dim',
            partitionDimension: 'isRobot',
            targetRowsPerSegment: 150000,
          },
          forceGuaranteedRollup: true,
          maxNumConcurrentSubTasks: 4,
          maxParseExceptions: 3,
        },
      },
    });

    expect(converted.queryString).toMatchSnapshot();
  });

  it('works with ARRAY mode', () => {
    const converted = convertSpecToSql({
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'inline',
            data: '{"s":"X", "l":10, "f":10.1, "array_s":["A", "B"], "array_l":[1,2], "array_f":[1.1,2.2], "mix1":[1, "lol"], "mix2":[1.1, 77]}\n{"s":"Y", "l":11, "f":11.1, "array_s":["C", "D"], "array_l":[3,4], "array_f":[3.3,4.4], "mix1":[2, "zoz"], "mix2":[1.2, 88]}',
          },
          inputFormat: {
            type: 'json',
          },
        },
        tuningConfig: {
          type: 'index_parallel',
          partitionsSpec: {
            type: 'dynamic',
          },
        },
        dataSchema: {
          dataSource: 'lol',
          timestampSpec: {
            column: '!!!_no_such_column_!!!',
            missingValue: '2010-01-01T00:00:00Z',
          },
          dimensionsSpec: {
            dimensions: [
              's',
              {
                type: 'long',
                name: 'l',
              },
              {
                type: 'double',
                name: 'f',
              },
              {
                type: 'auto',
                name: 'array_s',
                castToType: 'ARRAY<STRING>',
              },
              {
                type: 'auto',
                name: 'array_l',
                castToType: 'ARRAY<LONG>',
              },
              {
                type: 'auto',
                name: 'array_f',
                castToType: 'ARRAY<DOUBLE>',
              },
              {
                type: 'auto',
                name: 'mix1',
                castToType: 'ARRAY<STRING>',
              },
              {
                type: 'auto',
                name: 'mix2',
                castToType: 'ARRAY<DOUBLE>',
              },
            ],
          },
          granularitySpec: {
            queryGranularity: 'none',
            rollup: false,
            segmentGranularity: 'day',
          },
        },
      },
    });

    expect(converted.queryString).toMatchSnapshot();
  });
});
