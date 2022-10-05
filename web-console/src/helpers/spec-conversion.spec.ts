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

import { sane } from 'druid-query-toolkit';

import { convertSpecToSql } from './spec-conversion';

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

    expect(converted.queryString).toEqual(sane`
      -- This SQL query was auto generated from an ingestion spec
      REPLACE INTO wikipedia OVERWRITE ALL
      WITH source AS (SELECT * FROM TABLE(
        EXTERN(
          '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
          '{"type":"json"}',
          '[{"name":"timestamp","type":"string"},{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"string"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
        )
      ))
      SELECT
        CASE WHEN CAST("timestamp" AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST("timestamp" AS BIGINT)) ELSE TIME_PARSE("timestamp") END AS __time,
        "isRobot",
        "channel",
        "flags",
        "isUnpatrolled",
        "page",
        "diffUrl",
        "added",
        "comment",
        "commentLength",
        "isNew",
        "isMinor",
        "delta",
        "isAnonymous",
        "user",
        "deltaBucket",
        "deleted",
        "namespace",
        "cityName",
        "countryName",
        "regionIsoCode",
        "metroCode",
        "countryIsoCode",
        "regionName"
      FROM source
      WHERE NOT ("channel" = 'xxx')
      PARTITIONED BY HOUR
      CLUSTERED BY "isRobot"
    `);

    expect(converted.queryContext).toEqual({
      groupByEnableMultiValueUnnesting: false,
      maxParseExceptions: 3,
      finalizeAggregations: false,
      maxNumTasks: 5,
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

    expect(converted.queryString).toEqual(sane`
      -- This SQL query was auto generated from an ingestion spec
      REPLACE INTO wikipedia_rollup OVERWRITE ALL
      WITH source AS (SELECT * FROM TABLE(
        EXTERN(
          '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
          '{"type":"json"}',
          '[{"name":"timestamp","type":"string"},{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"comment","type":"string"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"string"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"},{"name":"added","type":"long"},{"name":"commentLength","type":"long"},{"name":"delta","type":"long"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"page","type":"string"}]'
        )
      ))
      SELECT
        TIME_FLOOR(TIME_PARSE("timestamp"), 'PT1H') AS __time,
        "isRobot",
        "channel",
        "flags",
        "isUnpatrolled",
        "comment",
        "isNew",
        "isMinor",
        "isAnonymous",
        "user",
        "namespace",
        "cityName",
        "countryName",
        "regionIsoCode",
        "metroCode",
        "countryIsoCode",
        "regionName",
        COUNT(*) AS "count",
        SUM("added") AS "sum_added",
        SUM("commentLength") AS "sum_commentLength",
        SUM("delta") AS "sum_delta",
        SUM("deltaBucket") AS "sum_deltaBucket",
        SUM("deleted") AS "sum_deleted",
        APPROX_COUNT_DISTINCT_DS_THETA("page") AS "page_theta"
      FROM source
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
      PARTITIONED BY HOUR
    `);

    expect(converted.queryContext).toEqual({
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

    expect(converted.queryString).toEqual(sane`
      -- This SQL query was auto generated from an ingestion spec
      REPLACE INTO newSource OVERWRITE ALL
      WITH source AS (SELECT * FROM TABLE(
        EXTERN(
          '{"type":"s3","uris":["s3://path"]}',
          '{"columns":["col1","col2","col3","col4","metric1","metric2","metric3","metric4","metric5","metric6","metric7"],"type":"timeAndDims"}',
          '[{"name":"event_ts","type":"string"},{"name":"col1","type":"string"},{"name":"col2","type":"string"},{"name":"col3","type":"string"},{"name":"col4","type":"string"},{"name":"field1","type":"double"},{"name":"field2","type":"double"},{"name":"field3","type":"double"},{"name":"field4","type":"string"},{"name":"field5","type":"string"},{"name":"field6","type":"long"},{"name":"field7","type":"double"}]'
        )
      ))
      SELECT
        TIME_FLOOR(CASE WHEN CAST(event_ts AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST(event_ts AS BIGINT)) ELSE TIME_PARSE(event_ts) END, 'PT1H') AS __time,
        "col1",
        "col2",
        "col3",
        "col4",
        SUM("field1") AS "metric1",
        MAX("field2") AS "metric2",
        MIN("field3") AS "metric3",
        APPROX_COUNT_DISTINCT_BUILTIN("field4") AS "metric4",
        APPROX_COUNT_DISTINCT_BUILTIN("field5") AS "metric5",
        SUM("field6") AS "metric6",
        SUM("field7") AS "metric7"
      FROM source
      WHERE "col2" = 'xxx'
      GROUP BY 1, 2, 3, 4, 5
      PARTITIONED BY HOUR
    `);

    expect(converted.queryContext).toEqual({
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

    expect(converted.queryString).toEqual(sane`
      -- This SQL query was auto generated from an ingestion spec
      REPLACE INTO wikipedia OVERWRITE ALL
      WITH source AS (SELECT * FROM TABLE(
        EXTERN(
          '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
          '{"type":"json"}',
          '[{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"string"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
        )
      ))
      SELECT
        --:ISSUE: The spec contained transforms that could not be automatically converted.
        REWRITE_[_some_time_parse_expression_]_TO_SQL AS __time, --:ISSUE: Transform for __time could not be converted
        "isRobot",
        "channel",
        "flags",
        "isUnpatrolled",
        "page",
        "diffUrl",
        "added",
        "comment",
        "commentLength",
        "isNew",
        "isMinor",
        "delta",
        "isAnonymous",
        "user",
        "deltaBucket",
        "deleted",
        "namespace",
        "cityName",
        "countryName",
        "regionIsoCode",
        "metroCode",
        "countryIsoCode",
        "regionName"
      FROM source
      PARTITIONED BY HOUR
      CLUSTERED BY "isRobot"
    `);
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

    expect(converted.queryString).toEqual(sane`
      -- This SQL query was auto generated from an ingestion spec
      REPLACE INTO wikipedia OVERWRITE ALL
      WITH source AS (SELECT * FROM TABLE(
        EXTERN(
          '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
          '{"type":"json"}',
          '[{"name":"timestamp","type":"string"},{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"string"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
        )
      ))
      SELECT
        --:ISSUE: The spec contained transforms that could not be automatically converted.
        CASE WHEN CAST("timestamp" AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST("timestamp" AS BIGINT)) ELSE TIME_PARSE("timestamp") END AS __time,
        "isRobot",
        "channel",
        "flags",
        "isUnpatrolled",
        "page",
        "diffUrl",
        "added",
        REWRITE_[_some_expression_]_TO_SQL AS "comment", --:ISSUE: Transform for dimension could not be converted
        "commentLength",
        "isNew",
        "isMinor",
        "delta",
        "isAnonymous",
        "user",
        "deltaBucket",
        "deleted",
        "namespace",
        "cityName",
        "countryName",
        "regionIsoCode",
        "metroCode",
        "countryIsoCode",
        "regionName"
      FROM source
      WHERE REWRITE_[{"type":"strange"}]_TO_SQL --:ISSUE: The spec contained a filter that could not be automatically converted, please convert it manually
      PARTITIONED BY HOUR
      CLUSTERED BY "isRobot"
    `);
  });
});
