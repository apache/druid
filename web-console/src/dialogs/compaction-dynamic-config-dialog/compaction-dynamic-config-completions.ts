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

export const COMPACTION_DYNAMIC_CONFIG_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      {
        value: 'compactionTaskSlotRatio',
        documentation:
          'Ratio of number of slots taken up by compaction tasks to total task slots across all workers (default: 0.1)',
      },
      {
        value: 'maxCompactionTaskSlots',
        documentation:
          'Maximum number of task slots that can be taken up by compaction tasks (default: 2147483647)',
      },
      { value: 'compactionPolicy', documentation: 'Policy to choose intervals for compaction' },
      {
        value: 'useSupervisors',
        documentation:
          'Whether compaction should be run on Overlord using supervisors instead of Coordinator duties (experimental)',
      },
      {
        value: 'engine',
        documentation: 'Engine used for running compaction tasks (native or msq)',
      },
    ],
  },
  // compactionTaskSlotRatio values
  {
    path: '$.compactionTaskSlotRatio',
    completions: [
      { value: '0.05', documentation: '5% of slots for compaction' },
      { value: '0.1', documentation: '10% of slots for compaction (default)' },
      { value: '0.2', documentation: '20% of slots for compaction' },
      { value: '0.3', documentation: '30% of slots for compaction' },
      { value: '0.5', documentation: '50% of slots for compaction' },
    ],
  },
  // maxCompactionTaskSlots values
  {
    path: '$.maxCompactionTaskSlots',
    completions: [
      { value: '1', documentation: 'Minimum for native engine' },
      { value: '2', documentation: 'Minimum for MSQ engine' },
      { value: '10', documentation: '10 task slots' },
      { value: '50', documentation: '50 task slots' },
      { value: '100', documentation: '100 task slots' },
      { value: '500', documentation: '500 task slots' },
      { value: '1000', documentation: '1000 task slots' },
      { value: '2147483647', documentation: 'Maximum value (default)' },
    ],
  },
  // compactionPolicy object properties
  {
    path: '$.compactionPolicy',
    isObject: true,
    completions: [
      {
        value: 'type',
        documentation: 'Policy type (currently only newestSegmentFirst is supported)',
      },
      { value: 'priorityDatasource', documentation: 'Datasource to prioritize for compaction' },
    ],
  },
  // compactionPolicy.type values
  {
    path: '$.compactionPolicy.type',
    completions: [
      {
        value: 'newestSegmentFirst',
        documentation: 'Prioritizes segments with more recent intervals for compaction',
      },
    ],
  },
  // useSupervisors values
  {
    path: '$.useSupervisors',
    completions: [
      { value: 'true', documentation: 'Use supervisors for compaction (experimental)' },
      { value: 'false', documentation: 'Use Coordinator duties for compaction (default)' },
    ],
  },
  // engine values (only when useSupervisors is true)
  {
    path: '$.engine',
    condition: obj => obj.useSupervisors === true,
    completions: [
      { value: 'native', documentation: 'Native indexing engine (default)' },
      { value: 'msq', documentation: 'Multi-Stage Query engine (requires useSupervisors: true)' },
    ],
  },
  // engine values (when useSupervisors is not specified or false)
  {
    path: '$.engine',
    condition: obj => !obj.useSupervisors,
    completions: [{ value: 'native', documentation: 'Native indexing engine (default)' }],
  },
];
