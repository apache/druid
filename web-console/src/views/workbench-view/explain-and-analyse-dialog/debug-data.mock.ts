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

export const debugData = [
  {
    debugInfo: {
      duration: 'PT86400S',
      sqlQueryId: '6e55065b-359f-4bb3-9a1e-fcf1896b6392',
      hasFilters: 'true',
      context: {
        applyLimitPushDown: true,
        debug: true,
        defaultTimeout: 300000,
        finalize: false,
        fudgeTimestamp: '-4611686018427387904',
        groupByOutermost: false,
        maxQueuedBytes: 5242880,
        maxScatterGatherBytes: 9223372036854775807,
        populateCache: false,
        queryFailTime: 1702965532577,
        queryId: '6e55065b-359f-4bb3-9a1e-fcf1896b6392',
        resultAsArray: true,
        sqlOuterLimit: 1001,
        sqlQueryId: '6e55065b-359f-4bb3-9a1e-fcf1896b6392',
        timeout: 299989,
        useCache: false,
        windowsAreForClosers: true,
      },
      subQueries: ['6e55065b-359f-4bb3-9a1e-fcf1896b6392'],
      interval: ['2016-06-27T00:00:00.000Z/2016-06-28T00:00:00.000Z'],
      id: '6e55065b-359f-4bb3-9a1e-fcf1896b6392',
      type: 'groupBy',
      dataSource: 'wikipedia',
    },
    metrics: {
      'query/cpu/time': 106680,
    },
    children: [
      {
        metrics: {
          'query/cpu/time': 106680,
        },
        debugInfo: {},
        children: [],
      },
      {
        metrics: {
          'query/cpu/time': 106680,
        },
        debugInfo: {},
        children: [],
      },
    ],
  },
];
