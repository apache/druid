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

import { IngestionSpec } from './ingestion-spec';

export function getWikipediaSpec(dataSourceSuffix: string): IngestionSpec {
  return {
    'type': 'index',
    'dataSchema': {
      'dataSource': 'wikipedia-' + dataSourceSuffix,
      'parser': {
        'type': 'string',
        'parseSpec': {
          'format': 'json',
          'dimensionsSpec': {
            'dimensions': [
              'isRobot',
              'channel',
              'flags',
              'isUnpatrolled',
              'page',
              'diffUrl',
              {
                'name': 'added',
                'type': 'long'
              },
              'comment',
              {
                'name': 'commentLength',
                'type': 'long'
              },
              'isNew',
              'isMinor',
              {
                'name': 'delta',
                'type': 'long'
              },
              'isAnonymous',
              'user',
              {
                'name': 'deltaBucket',
                'type': 'long'
              },
              {
                'name': 'deleted',
                'type': 'long'
              },
              'namespace'
            ]
          },
          'timestampSpec': {
            'column': 'timestamp',
            'format': 'iso'
          }
        }
      },
      'granularitySpec': {
        'type': 'uniform',
        'segmentGranularity': 'DAY',
        'rollup': false,
        'queryGranularity': 'none'
      },
      'metricsSpec': []
    },
    'ioConfig': {
      'type': 'index',
      'firehose': {
        'fetchTimeout': 300000,
        'type': 'http',
        'uris': [
          'https://static.imply.io/data/wikipedia.json.gz'
        ]
      }
    },
    'tuningConfig': {
      'type': 'index',
      'forceExtendableShardSpecs': true,
      'maxParseExceptions': 100,
      'maxSavedParseExceptions': 10
    }
  };
}
