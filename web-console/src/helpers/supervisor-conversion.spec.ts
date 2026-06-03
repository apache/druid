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

import type { SupervisorSpec } from './supervisor-conversion';
import { convertSupervisorToSql } from './supervisor-conversion';

expect.addSnapshotSerializer({
  test: val => typeof val === 'string',
  print: String,
});

function wikipediaSupervisor(): SupervisorSpec {
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
  };
}

describe('supervisor conversion', () => {
  describe('convertSupervisorToSql', () => {
    it('converts a supervisor with dimensions and metrics (rollup -> GROUP BY)', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toMatchSnapshot();
      expect(converted.queryContext).toEqual({});
    });

    it('converts a supervisor without metrics (no GROUP BY)', () => {
      const supervisor = wikipediaSupervisor();
      delete supervisor.spec.dataSchema.metricsSpec;

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toMatchSnapshot();
      expect(converted.queryString).not.toContain('GROUP BY');
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

      expect(converted.queryString).toMatchSnapshot();
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

      expect(converted.queryString).toMatchSnapshot();
    });

    it('drops unsupported metrics and metrics missing a fieldName', () => {
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

      expect(converted.queryString).toContain('"sum_added"');
      expect(converted.queryString).not.toContain('unsupported');
      expect(converted.queryString).not.toContain('no_field');
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

      expect(converted.queryString).toContain('google');
    });

    it('builds an http input source for an http(s) location', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 'https://example.com/wikipedia/data.json',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('http');
    });

    it('builds a local input source and strips the file:// prefix', () => {
      const converted = convertSupervisorToSql(wikipediaSupervisor(), {
        fileLocation: 'file:///var/data/wikipedia/',
        fileType: 'csv',
      });

      expect(converted.queryString).toContain('local');
      expect(converted.queryString).toContain('/var/data/wikipedia/');
      expect(converted.queryString).not.toContain('file://');
      expect(converted.queryString).toContain('*.csv');
    });

    it('uses TIME_PARSE without a format when timestamp format is auto', () => {
      const supervisor = wikipediaSupervisor();
      supervisor.spec.dataSchema.timestampSpec = { column: 'timestamp', format: 'auto' };

      const converted = convertSupervisorToSql(supervisor, {
        fileLocation: 's3://my-bucket/wikipedia/data/',
        fileType: 'json',
      });

      expect(converted.queryString).toContain('TIME_PARSE("timestamp")');
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

      expect(converted.queryString).toContain('PARTITIONED BY DAY');
      expect(converted.queryString).toContain('CLUSTERED BY "a", "b"');
      // Only the first two dimensions are clustered; "c" and "d" are not in the CLUSTERED BY clause.
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

      expect(converted.queryString).toContain('PARTITIONED BY DAY');
      expect(converted.queryString).not.toContain('CLUSTERED BY');
    });

    it('throws when dataSchema is missing', () => {
      const supervisor = { type: 'kafka', spec: {} } as unknown as SupervisorSpec;

      expect(() =>
        convertSupervisorToSql(supervisor, { fileLocation: 's3://x/', fileType: 'json' }),
      ).toThrow('Supervisor spec missing dataSchema');
    });

    it('throws when dataSource is missing', () => {
      const supervisor = wikipediaSupervisor();
      (supervisor.spec.dataSchema as any).dataSource = undefined;

      expect(() =>
        convertSupervisorToSql(supervisor, { fileLocation: 's3://x/', fileType: 'json' }),
      ).toThrow('Supervisor spec missing dataSource');
    });
  });
});
