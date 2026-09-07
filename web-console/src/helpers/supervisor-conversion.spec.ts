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

import type { IngestionSpec } from '../druid-models';

import { convertSupervisorToSql } from './supervisor-conversion';

expect.addSnapshotSerializer({
  test: val => typeof val === 'string',
  print: String,
});

function wikipediaSupervisor(): IngestionSpec {
  return {
    type: 'kafka',
    spec: {
      dataSchema: {
        dataSource: 'wikipedia',
        timestampSpec: {
          column: 'timestamp',
          format: 'iso',
        },
        dimensionsSpec: {
          dimensions: [
            'channel',
            'user',
            { name: 'page', type: 'string' },
            { name: 'namespace', type: 'string' },
          ],
        },
        metricsSpec: [
          { name: 'count', type: 'count' },
          { name: 'sum_added', type: 'longSum', fieldName: 'added' },
        ],
      },
      ioConfig: {
        topic: 'wikipedia',
        inputSource: {
          type: 's3',
          uris: ['s3://my-bucket/wikipedia/data/'],
        },
      },
    },
  } as IngestionSpec;
}

describe('supervisor conversion', () => {
  describe('convertSupervisorToSql', () => {
    it('converts a supervisor with dimensions and metrics (rollup -> GROUP BY)', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toMatchInlineSnapshot(`
        -- This SQL query was auto generated from an ingestion spec
        SET arrayIngestMode = 'array';
        SET finalizeAggregations = FALSE;
        SET groupByEnableMultiValueUnnesting = FALSE;
        REPLACE INTO "wikipedia" OVERWRITE ALL
        WITH "source" AS (SELECT * FROM TABLE(
          EXTERN(
            '{"type":"s3","uris":["s3://my-bucket/wikipedia/data/"],"objectGlob":"**.json"}',
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "channel" VARCHAR, "user" VARCHAR, "page" VARCHAR, "namespace" VARCHAR, "added" BIGINT))
        SELECT
          TIME_PARSE("timestamp") AS "__time",
          "channel",
          "user",
          "page",
          "namespace",
          COUNT(*) AS "count",
          SUM("added") AS "sum_added"
        FROM "source"
        GROUP BY 1, 2, 3, 4, 5
        PARTITIONED BY DAY
        CLUSTERED BY "channel", "user"
      `);
    });

    it('does not GROUP BY when rollup is disabled', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.granularitySpec = { rollup: false };

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).not.toContain('GROUP BY');
    });

    it('declares numeric metric and dimension columns with their native types', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.dimensionsSpec = {
        dimensions: ['channel', { name: 'delta', type: 'long' }, { name: 'ratio', type: 'double' }],
      };
      supervisor.spec.dataSchema.metricsSpec = [
        { name: 'sum_added', type: 'longSum', fieldName: 'added' },
        { name: 'unique_user', type: 'thetaSketch', fieldName: 'user' },
      ];

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      // The EXTERN signature carries native types, not all VARCHAR
      expect(converted.queryString).toContain('"delta" BIGINT');
      expect(converted.queryString).toContain('"ratio" DOUBLE');
      expect(converted.queryString).toContain('"added" BIGINT');
      // Sketch metrics read from a string input column
      expect(converted.queryString).toContain('"user" VARCHAR');
    });

    it('converts the full range of supported metric aggregations', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.metricsSpec = [
        { name: 'count', type: 'count' },
        { name: 'sum_added', type: 'longSum', fieldName: 'added' },
        { name: 'min_added', type: 'doubleMin', fieldName: 'added' },
        { name: 'max_added', type: 'floatMax', fieldName: 'added' },
        { name: 'first_added', type: 'longFirst', fieldName: 'added' },
        { name: 'last_added', type: 'doubleLast', fieldName: 'added' },
        { name: 'first_page', type: 'stringFirst', fieldName: 'page' },
        { name: 'last_page', type: 'stringLast', fieldName: 'page' },
        { name: 'theta_user', type: 'thetaSketch', fieldName: 'user' },
        { name: 'hll_user', type: 'HLLSketchBuild', fieldName: 'user' },
        { name: 'quantiles_added', type: 'quantilesDoublesSketch', fieldName: 'added' },
        { name: 'unique_user', type: 'hyperUnique', fieldName: 'user' },
      ];

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toMatchInlineSnapshot(`
        -- This SQL query was auto generated from an ingestion spec
        SET arrayIngestMode = 'array';
        SET finalizeAggregations = FALSE;
        SET groupByEnableMultiValueUnnesting = FALSE;
        REPLACE INTO "wikipedia" OVERWRITE ALL
        WITH "source" AS (SELECT * FROM TABLE(
          EXTERN(
            '{"type":"s3","uris":["s3://my-bucket/wikipedia/data/"],"objectGlob":"**.json"}',
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "channel" VARCHAR, "user" VARCHAR, "page" VARCHAR, "namespace" VARCHAR, "added" BIGINT))
        SELECT
          TIME_PARSE("timestamp") AS "__time",
          "channel",
          "user",
          "page",
          "namespace",
          COUNT(*) AS "count",
          SUM("added") AS "sum_added",
          MIN("added") AS "min_added",
          MAX("added") AS "max_added",
          EARLIEST("added") AS "first_added",
          LATEST("added") AS "last_added",
          EARLIEST("page", 128) AS "first_page",
          LATEST("page", 128) AS "last_page",
          APPROX_COUNT_DISTINCT_DS_THETA("user") AS "theta_user",
          APPROX_COUNT_DISTINCT_DS_HLL("user") AS "hll_user",
          DS_QUANTILES_SKETCH("added") AS "quantiles_added",
          APPROX_COUNT_DISTINCT_BUILTIN("user") AS "unique_user"
        FROM "source"
        GROUP BY 1, 2, 3, 4, 5
        PARTITIONED BY DAY
        CLUSTERED BY "channel", "user"
      `);
    });

    it('includes non-default sketch arguments', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.metricsSpec = [
        { name: 'theta_user', type: 'thetaSketch', fieldName: 'user', size: 32768 } as any,
        {
          name: 'hll_user',
          type: 'HLLSketchBuild',
          fieldName: 'user',
          lgK: 14,
          tgtHllType: 'HLL_8',
        } as any,
        {
          name: 'quantiles_added',
          type: 'quantilesDoublesSketch',
          fieldName: 'added',
          k: 256,
        } as any,
        { name: 'first_page', type: 'stringFirst', fieldName: 'page', maxStringBytes: 1024 } as any,
      ];

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('APPROX_COUNT_DISTINCT_DS_THETA("user", 32768)');
      expect(converted.queryString).toContain(`APPROX_COUNT_DISTINCT_DS_HLL("user", 14, 'HLL_8')`);
      expect(converted.queryString).toContain('DS_QUANTILES_SKETCH("added", 256)');
      expect(converted.queryString).toContain('EARLIEST("page", 1024)');
    });

    it('emits a comment for unsupported metrics and metrics missing a fieldName', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.metricsSpec = [
        { name: 'sum_added', type: 'longSum', fieldName: 'added' },
        { name: 'unsupported', type: 'tDigestSketch', fieldName: 'added' },
        { name: 'no_field', type: 'longSum' },
      ];

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('SUM("added") AS "sum_added"');
      expect(converted.queryString).toContain('could not convert metric');
    });

    it('adds objectGlob for an s3 directory location', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'parquet',
      });

      expect(converted.queryString).toContain('objectGlob');
      expect(converted.queryString).toContain('**.parquet');
    });

    it('does not add objectGlob for a single s3 file location', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 's3://my-bucket/wikipedia/data.json',
        fileType: 'json',
      });

      expect(converted.queryString).not.toContain('objectGlob');
    });

    it('builds a google input source for a gs:// location', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 'gs://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('"type":"google"');
    });

    it('builds an http input source for an http(s) location', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 'https://example.com/wikipedia/data.json',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('"type":"http"');
    });

    it('builds a local input source and strips the file:// prefix', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 'file:///var/data/wikipedia/',
        fileType: 'csv',
      });

      expect(converted.queryString).toContain('"type":"local"');
      expect(converted.queryString).toContain('/var/data/wikipedia/');
      expect(converted.queryString).not.toContain('file://');
      expect(converted.queryString).toContain('*.csv');
    });

    it('preserves the supervisor input format settings, overriding only the type', () => {
      const supervisor = wikipediaSupervisor();
      (supervisor.spec.ioConfig as any).inputFormat = {
        type: 'kafka',
        valueFormat: { type: 'json', flattenSpec: { fields: [{ name: 'x', expr: '$.x' }] } },
      };

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'csv',
      });

      expect(converted.queryString).toContain('flattenSpec');
      expect(converted.queryString).toContain('"type":"csv"');
      expect(converted.queryString).not.toContain('"type":"kafka"');
    });

    it('applies the supervisor segment and query granularity', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.granularitySpec = {
        segmentGranularity: 'HOUR',
        queryGranularity: 'hour',
      };

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('PARTITIONED BY HOUR');
      expect(converted.queryString).toContain(`TIME_FLOOR(TIME_PARSE("timestamp"), 'PT1H')`);
    });

    it('defaults to PARTITIONED BY DAY when no segment granularity is set', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('PARTITIONED BY DAY');
    });

    it('clusters by at most the first two dimensions', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.dimensionsSpec = {
        dimensions: ['a', 'b', 'c', 'd'],
      };

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('CLUSTERED BY "a", "b"');
      const clusteredBy = converted.queryString.slice(
        converted.queryString.indexOf('CLUSTERED BY'),
      );
      expect(clusteredBy).not.toContain('"c"');
      expect(clusteredBy).not.toContain('"d"');
    });

    it('omits CLUSTERED BY when there are no dimensions', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.dimensionsSpec = { dimensions: [] };

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).not.toContain('CLUSTERED BY');
    });

    it('throws when dataSchema is missing', () => {
      const supervisor = { type: 'kafka', spec: {} } as unknown as IngestionSpec;

      expect(() =>
        convertSupervisorToSql(supervisor, { fileLocation: 's3://x/', fileType: 'json' }),
      ).toThrow('Supervisor spec missing dataSchema');
    });
  });
});
