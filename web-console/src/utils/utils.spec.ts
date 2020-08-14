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
  getMetricSpecs,
  guessTypeFromSample,
  updateSchemaWithSample,
} from './druid-type';
import { IngestionSpec } from './ingestion-spec';
import { applyCache, headerFromSampleResponse } from './sampler';

describe('test-utils', () => {
  const ingestionSpec: IngestionSpec = {
    type: 'index_parallel',
    spec: {
      ioConfig: {
        type: 'index_parallel',
        inputSource: {
          type: 'http',
          uris: ['https://static.imply.io/data/wikipedia.json.gz'],
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
          type: 'uniform',
          segmentGranularity: 'DAY',
          queryGranularity: 'HOUR',
        },
        timestampSpec: {
          column: 'timestamp',
          format: 'iso',
        },
        dimensionsSpec: {},
      },
    },
  };

  // const cacheRows: CacheRows = [{ make: 'Honda', model: 'Civic' }, { make: 'BMW', model: 'M3' }];

  it('spec-utils headerFromSampleResponse', () => {
    expect(headerFromSampleResponse({ data: [{ input: { a: 1 }, parsed: { a: 1 } }] }))
      .toMatchInlineSnapshot(`
      Array [
        "a",
      ]
    `);
  });

  it('spec-utils applyCache', () => {
    expect(
      applyCache(
        Object.assign({}, ingestionSpec, {
          samplerConfig: {
            numRows: 500,
            timeoutMs: 15000,
          },
        }),
        [{ make: 'Honda', model: 'Accord' }, { make: 'Toyota', model: 'Prius' }],
      ),
    ).toMatchInlineSnapshot(`
      Object {
        "samplerConfig": Object {
          "numRows": 500,
          "timeoutMs": 15000,
        },
        "spec": Object {
          "dataSchema": Object {
            "dataSource": "wikipedia",
            "dimensionsSpec": Object {},
            "granularitySpec": Object {
              "queryGranularity": "HOUR",
              "segmentGranularity": "DAY",
              "type": "uniform",
            },
            "timestampSpec": Object {
              "column": "timestamp",
              "format": "iso",
            },
          },
          "ioConfig": Object {
            "inputFormat": Object {
              "keepNullColumns": true,
              "type": "json",
            },
            "inputSource": Object {
              "data": "{\\"make\\":\\"Honda\\",\\"model\\":\\"Accord\\"}
      {\\"make\\":\\"Toyota\\",\\"model\\":\\"Prius\\"}",
              "type": "inline",
            },
            "type": "index",
          },
          "tuningConfig": Object {
            "type": "index_parallel",
          },
          "type": "index",
        },
        "type": "index",
      }
    `);
  });

  // it('spec-utils sampleForParser', async () => {
  //   expect(await sampleForParser(ingestionSpec, 'start', 'abc123')).toMatchInlineSnapshot(
  //     `Promise {}`,
  //   );
  // });
  //
  // it('spec-utils SampleSpec', async () => {
  //   expect(await sampleForConnect(ingestionSpec, 'start')).toMatchInlineSnapshot(`Promise {}`);
  // });
  //
  // it('spec-utils sampleForTimestamp', async () => {
  //   expect(await sampleForTimestamp(ingestionSpec, 'start', cacheRows)).toMatchInlineSnapshot();
  // });
  //
  // it('spec-utils sampleForTransform', async () => {
  //   expect(await sampleForTransform(ingestionSpec, 'start', cacheRows)).toMatchInlineSnapshot();
  // });
  //
  // it('spec-utils sampleForFilter', async () => {
  //   expect(await sampleForFilter(ingestionSpec, 'start', cacheRows)).toMatchInlineSnapshot();
  // });
  //
  // it('spec-utils sampleForSchema', async () => {
  //   expect(await sampleForSchema(ingestionSpec, 'start', cacheRows)).toMatchInlineSnapshot();
  // });
  //
  // it('spec-utils sampleForExampleManifests', async () => {
  //   expect(await sampleForExampleManifests('some url')).toMatchInlineSnapshot();
  // });
});

describe('druid-type.ts', () => {
  const ingestionSpec: IngestionSpec = {
    type: 'index_parallel',
    spec: {
      ioConfig: {
        type: 'index_parallel',
        inputSource: {
          type: 'http',
          uris: ['https://static.imply.io/data/wikipedia.json.gz'],
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
          type: 'uniform',
          segmentGranularity: 'DAY',
          queryGranularity: 'HOUR',
        },
        timestampSpec: {
          column: 'timestamp',
          format: 'iso',
        },
        dimensionsSpec: {},
      },
    },
  };

  it('spec-utils guessTypeFromSample', () => {
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
    expect(getMetricSpecs({ header: ['header'], rows: [] })).toMatchInlineSnapshot(`
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
      updateSchemaWithSample(ingestionSpec, { header: ['header'], rows: [] }, 'specific', true),
    ).toMatchInlineSnapshot(`
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
                "https://static.imply.io/data/wikipedia.json.gz",
              ],
            },
            "type": "index_parallel",
          },
          "tuningConfig": Object {
            "type": "index_parallel",
          },
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
