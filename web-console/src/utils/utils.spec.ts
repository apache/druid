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

import { applyCache } from './sampler';

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
      {
        "samplerConfig": {
          "numRows": 500,
          "timeoutMs": 15000,
        },
        "spec": {
          "dataSchema": {
            "dataSource": "wikipedia",
            "dimensionsSpec": {},
            "granularitySpec": {
              "queryGranularity": "hour",
              "segmentGranularity": "day",
            },
            "timestampSpec": {
              "column": "timestamp",
              "format": "iso",
            },
          },
          "ioConfig": {
            "inputFormat": {
              "keepNullColumns": true,
              "type": "json",
            },
            "inputSource": {
              "data": "{"make":"Honda","model":"Accord"}
      {"make":"Toyota","model":"Prius"}",
              "type": "inline",
            },
            "type": "index",
          },
          "tuningConfig": {
            "type": "index_parallel",
          },
          "type": "index",
        },
        "type": "index",
      }
    `);
  });
});
