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

import { cleanSpec, downgradeSpec, upgradeSpec } from './ingestion-spec';

describe('ingestion-spec', () => {
  const oldSpec = {
    type: 'index_parallel',
    spec: {
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

  it('upgrades', () => {
    expect(upgradeSpec(oldSpec)).toMatchSnapshot();
  });

  it('round trips', () => {
    expect(downgradeSpec(upgradeSpec(oldSpec))).toMatchObject(oldSpec);
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
});
