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

import { getDruidErrorMessage, parseHtmlError, parseQueryPlan } from './druid-query';
import {
  getColumnTypeFromHeaderAndRows,
  getDimensionSpecs,
  getMetricSecs,
  guessTypeFromSample,
  updateSchemaWithSample,
} from './druid-type';
import { IngestionSpec } from './ingestion-spec';
import {
  getSamplerType,
  headerFromSampleResponse,
  sampleForConnect,
  sampleForExampleManifests,
  sampleForFilter,
  sampleForParser,
  sampleForSchema,
  sampleForTimestamp,
  sampleForTransform,
} from './sampler';

describe('test-utils', () => {
  const ingestionSpec = {
    type: 'index_parallel',
    ioConfig: {
      type: 'index_parallel',
      firehose: {
        type: 'http',
        uris: ['https://static.imply.io/data/wikipedia.json.gz'],
      },
    },
    tuningConfig: {
      type: 'index_parallel',
    },
    dataSchema: {
      dataSource: 'wikipedia',
      granularitySpec: {
        type: 'uniform',
        segmentGranularity: 'DAY',
        queryGranularity: 'HOUR',
      },
      parser: {
        type: 'string',
        parseSpec: {
          format: 'json',
          timestampSpec: {
            column: 'timestamp',
            format: 'iso',
          },
          dimensionsSpec: {},
        },
      },
    },
  };
  it('spec-utils getSamplerType', () => {
    expect(getSamplerType(ingestionSpec as IngestionSpec)).toMatchInlineSnapshot(`"index"`);
  });
  it('spec-utils headerFromSampleResponse', () => {
    expect(headerFromSampleResponse({ cacheKey: 'abc123', data: [] })).toMatchInlineSnapshot(
      `Array []`,
    );
  });
  it('spec-utils sampleForParser', () => {
    expect(
      sampleForParser(ingestionSpec as IngestionSpec, 'start', 'abc123'),
    ).toMatchInlineSnapshot(`Promise {}`);
  });
  it('spec-utils SampleSpec', () => {
    expect(sampleForConnect(ingestionSpec as IngestionSpec, 'start')).toMatchInlineSnapshot(
      `Promise {}`,
    );
  });
  it('spec-utils sampleForTimestamp', () => {
    expect(
      sampleForTimestamp(ingestionSpec as IngestionSpec, 'start', 'abc123'),
    ).toMatchInlineSnapshot(`Promise {}`);
  });
  it('spec-utils sampleForTransform', () => {
    expect(
      sampleForTransform(ingestionSpec as IngestionSpec, 'start', 'abc123'),
    ).toMatchInlineSnapshot(`Promise {}`);
  });
  it('spec-utils sampleForFilter', () => {
    expect(
      sampleForFilter(ingestionSpec as IngestionSpec, 'start', 'abc123'),
    ).toMatchInlineSnapshot(`Promise {}`);
  });
  it('spec-utils sampleForSchema', () => {
    expect(
      sampleForSchema(ingestionSpec as IngestionSpec, 'start', 'abc123'),
    ).toMatchInlineSnapshot(`Promise {}`);
  });
  it('spec-utils sampleForExampleManifests', () => {
    expect(sampleForExampleManifests('abc123')).toMatchInlineSnapshot(`Promise {}`);
  });
});

describe('druid-type.ts', () => {
  const ingestionSpec = {
    type: 'index_parallel',
    ioConfig: {
      type: 'index_parallel',
      firehose: {
        type: 'http',
        uris: ['https://static.imply.io/data/wikipedia.json.gz'],
      },
    },
    tuningConfig: {
      type: 'index_parallel',
    },
    dataSchema: {
      dataSource: 'wikipedia',
      granularitySpec: {
        type: 'uniform',
        segmentGranularity: 'DAY',
        queryGranularity: 'HOUR',
      },
      parser: {
        type: 'string',
        parseSpec: {
          format: 'json',
          timestampSpec: {
            column: 'timestamp',
            format: 'iso',
          },
          dimensionsSpec: {},
        },
      },
    },
  };
  it('spec-utils getSamplerType', () => {
    expect(guessTypeFromSample([])).toMatchInlineSnapshot(`"string"`);
  });
  it('spec-utils getColumnTypeFromHeaderAndRows', () => {
    expect(
      getColumnTypeFromHeaderAndRows({ header: ['header'], rows: [] }, 'header'),
    ).toMatchInlineSnapshot(`"string"`);
  });
  it('spec-utils getDimensionSpecs', () => {
    expect(getDimensionSpecs({ header: ['header'], rows: [] }, true)).toMatchInlineSnapshot(`
      Array [
        "header",
      ]
    `);
  });
  it('spec-utils getMetricSecs', () => {
    expect(getMetricSecs({ header: ['header'], rows: [] })).toMatchInlineSnapshot(`
      Array [
        Object {
          "name": "count",
          "type": "count",
        },
      ]
    `);
  });
  it('spec-utils updateSchemaWithSample', () => {
    expect(
      updateSchemaWithSample(
        ingestionSpec as IngestionSpec,
        { header: ['header'], rows: [] },
        'specific',
        true,
      ),
    ).toMatchInlineSnapshot(`
      Object {
        "dataSchema": Object {
          "dataSource": "wikipedia",
          "granularitySpec": Object {
            "queryGranularity": "HOUR",
            "rollup": true,
            "segmentGranularity": "DAY",
            "type": "uniform",
          },
          "metricsSpec": Array [
            Object {
              "name": "count",
              "type": "count",
            },
          ],
          "parser": Object {
            "parseSpec": Object {
              "dimensionsSpec": Object {
                "dimensions": Array [
                  "header",
                ],
              },
              "format": "json",
              "timestampSpec": Object {
                "column": "timestamp",
                "format": "iso",
              },
            },
            "type": "string",
          },
        },
        "ioConfig": Object {
          "firehose": Object {
            "type": "http",
            "uris": Array [
              "https://static.imply.io/data/wikipedia.json.gz",
            ],
          },
          "type": "index_parallel",
        },
        "tuningConfig": Object {
          "type": "index_parallel",
        },
        "type": "index_parallel",
      }
    `);
  });
});
describe('druid-query.ts', () => {
  it('spec-utils parseHtmlError', () => {
    expect(parseHtmlError('<div></div>')).toMatchInlineSnapshot(`undefined`);
  });
  it('spec-utils parseHtmlError', () => {
    expect(getDruidErrorMessage({})).toMatchInlineSnapshot(`undefined`);
  });
  it('spec-utils parseQueryPlan', () => {
    expect(parseQueryPlan('start')).toMatchInlineSnapshot(`"start"`);
  });
});
