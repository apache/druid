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

import { IngestionSpec } from '../druid-models';

import { applyCache, headerFromSampleResponse } from './sampler';

describe('utils', () => {
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

  // const cacheRows: CacheRows = [{ make: 'Honda', model: 'Civic' }, { make: 'BMW', model: 'M3' }];

  it('spec-utils headerFromSampleResponse', () => {
    expect(
      headerFromSampleResponse({
        sampleResponse: { data: [{ input: { a: 1 }, parsed: { a: 1 } }] },
      }),
    ).toMatchInlineSnapshot(`
      Array [
        "a",
      ]
    `);
  });

  it('spec-utils applyCache', () => {
    expect(
      applyCache(
        {
          ...ingestionSpec,
          samplerConfig: {
            numRows: 500,
            timeoutMs: 15000,
          },
        },
        [
          { make: 'Honda', model: 'Accord' },
          { make: 'Toyota', model: 'Prius' },
        ],
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
              "queryGranularity": "hour",
              "segmentGranularity": "day",
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
});
