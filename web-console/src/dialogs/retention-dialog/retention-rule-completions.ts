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

import type { JsonCompletionRule } from '../../utils';

export const RETENTION_RULE_COMPLETIONS: JsonCompletionRule[] = [
  // Root array - rule type completions
  {
    path: '$.[]',
    isObject: true,
    completions: [{ value: 'type', documentation: 'The type of retention rule' }],
  },
  {
    path: '$.[].type',
    completions: [
      { value: 'loadForever', documentation: 'Load segments forever' },
      { value: 'loadByInterval', documentation: 'Load segments within a specific time interval' },
      { value: 'loadByPeriod', documentation: 'Load segments for a specific period of time' },
      { value: 'dropForever', documentation: 'Drop all segments' },
      { value: 'dropByInterval', documentation: 'Drop segments within a specific time interval' },
      { value: 'dropByPeriod', documentation: 'Drop segments older than a specific period' },
      { value: 'dropBeforeByPeriod', documentation: 'Drop segments before a specific period' },
      { value: 'broadcastForever', documentation: 'Broadcast segments to all nodes forever' },
      {
        value: 'broadcastByInterval',
        documentation: 'Broadcast segments within a specific time interval',
      },
      {
        value: 'broadcastByPeriod',
        documentation: 'Broadcast segments for a specific period of time',
      },
    ],
  },
  // tieredReplicants for load rules
  {
    path: '$.[]',
    isObject: true,
    condition: obj => obj.type && obj.type.startsWith('load'),
    completions: [
      { value: 'tieredReplicants', documentation: 'Replication configuration per tier' },
    ],
  },
  // period for period-based rules
  {
    path: '$.[]',
    isObject: true,
    condition: obj => obj.type && obj.type.endsWith('ByPeriod'),
    completions: [
      { value: 'period', documentation: 'ISO 8601 period string (e.g., P1M for 1 month)' },
    ],
  },
  // includeFuture for period-based rules (except dropBeforeByPeriod)
  {
    path: '$.[]',
    isObject: true,
    condition: obj =>
      obj.type && obj.type.endsWith('ByPeriod') && obj.type !== 'dropBeforeByPeriod',
    completions: [{ value: 'includeFuture', documentation: 'Whether to include future segments' }],
  },
  // interval for interval-based rules
  {
    path: '$.[]',
    isObject: true,
    condition: obj => obj.type && obj.type.endsWith('ByInterval'),
    completions: [
      { value: 'interval', documentation: 'ISO 8601 interval (e.g., 2020-01-01/2021-01-01)' },
    ],
  },
  // Common period values
  {
    path: '$.[].period',
    completions: [
      { value: 'P1D', documentation: '1 day' },
      { value: 'P7D', documentation: '7 days' },
      { value: 'P1M', documentation: '1 month' },
      { value: 'P3M', documentation: '3 months' },
      { value: 'P6M', documentation: '6 months' },
      { value: 'P1Y', documentation: '1 year' },
      { value: 'P2Y', documentation: '2 years' },
    ],
  },
  // Boolean values for includeFuture
  {
    path: '$.[].includeFuture',
    completions: [
      { value: 'true', documentation: 'Include future segments' },
      { value: 'false', documentation: 'Do not include future segments' },
    ],
  },
  // Tier names for tieredReplicants
  {
    path: '$.[].tieredReplicants',
    isObject: true,
    completions: [
      { value: '_default_tier', documentation: 'Default tier for historical nodes' },
      { value: 'hot', documentation: 'Hot tier for frequently accessed data' },
      { value: 'cold', documentation: 'Cold tier for infrequently accessed data' },
    ],
  },
  // Replication factors
  {
    path: /^\$\.\[]\.tieredReplicants\.[^.]+$/,
    completions: [
      { value: '1', documentation: '1 replica' },
      { value: '2', documentation: '2 replicas' },
      { value: '3', documentation: '3 replicas' },
    ],
  },
];
