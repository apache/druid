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

export interface ExampleSpec {
  name: string;
  description: string;
  spec: any;
}

export const EXAMPLE_SPECS: ExampleSpec[] = [
  {
    name: 'Wikipedia Edits',
    description: 'Edits on Wikipedia from one day',
    spec: {
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
          },
        },
        dataSchema: {
          granularitySpec: {
            segmentGranularity: 'day',
          },
        },
        tuningConfig: {
          type: 'index_parallel',
        },
      },
    },
  },
  {
    name: 'KoalasToTheMax one day',
    description: 'One day of flat events from KoalasToTheMax.com (JSON)',
    spec: {
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: ['https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz'],
          },
        },
        dataSchema: {
          dataSource: 'kttm1',
          granularitySpec: {
            segmentGranularity: 'day',
          },
        },
        tuningConfig: {
          type: 'index_parallel',
        },
      },
    },
  },
  {
    name: 'KoalasToTheMax one day (nested)',
    description: 'One day of nested events from KoalasToTheMax.com (JSON)',
    spec: {
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'http',
            uris: [
              'https://static.imply.io/example-data/kttm-nested-v2/kttm-nested-v2-2019-08-25.json.gz',
            ],
          },
        },
        dataSchema: {
          dataSource: 'kttm_nested1',
          granularitySpec: {
            segmentGranularity: 'day',
          },
        },
        tuningConfig: {
          type: 'index_parallel',
        },
      },
    },
  },
];
