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

import { Code } from '@blueprintjs/core';

import type { Field } from '../../components';
import { deepGet, deepSet } from '../../utils';

export type CompactionPolicy =
  | { type: 'newestSegmentFirst'; priorityDatasource?: string | null }
  | {
      type: 'mostFragmentedFirst';
      priorityDatasource?: string | null;
      minUncompactedCount?: number;
      minUncompactedBytes?: number;
      maxAverageUncompactedBytesPerSegment?: number;
      minUncompactedBytesPercentForFullCompaction?: number;
      minUncompactedRowsPercentForFullCompaction?: number;
    };

export interface CompactionDynamicConfig {
  compactionTaskSlotRatio: number;
  maxCompactionTaskSlots: number;
  compactionPolicy: CompactionPolicy;
  useSupervisors: boolean;
  engine: 'native' | 'msq';
  storeCompactionStatePerSegment: boolean;
}

export const COMPACTION_DYNAMIC_CONFIG_DEFAULT_RATIO = 0.1;
export const COMPACTION_DYNAMIC_CONFIG_DEFAULT_MAX = 2147483647;
export const COMPACTION_DYNAMIC_CONFIG_FIELDS: Field<CompactionDynamicConfig>[] = [
  {
    name: 'useSupervisors',
    label: 'Use supervisors',
    experimental: true,
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        <p>
          Whether compaction should be run on Overlord using supervisors instead of Coordinator
          duties.
        </p>
        <p>Supervisor based compaction is an experimental feature.</p>
      </>
    ),
  },
  {
    name: 'engine',
    type: 'string',
    defined: config => Boolean(config.useSupervisors),
    defaultValue: 'native',
    suggestions: ['native', 'msq'],
    info: 'Engine to use for running compaction tasks, native or MSQ.',
  },
  {
    name: 'compactionTaskSlotRatio',
    type: 'ratio',
    defaultValue: COMPACTION_DYNAMIC_CONFIG_DEFAULT_RATIO,
    info: <>The ratio of the total task slots to the compaction task slots.</>,
  },
  {
    name: 'maxCompactionTaskSlots',
    type: 'number',
    defaultValue: COMPACTION_DYNAMIC_CONFIG_DEFAULT_MAX,
    info: <>The maximum number of task slots for compaction tasks</>,
    min: 0,
  },
  {
    name: 'compactionPolicy.type',
    label: 'Compaction search policy',
    type: 'string',
    suggestions: ['newestSegmentFirst', 'mostFragmentedFirst'],
    info: (
      <>
        <p>
          <Code>newestSegmentFirst</Code> prioritizes segments with more recent intervals for
          compaction.
        </p>
        <p>
          <Code>mostFragmentedFirst</Code> (experimental) prioritizes intervals with the largest
          number of small uncompacted segments, favoring cluster stability over query performance on
          newer intervals.
        </p>
      </>
    ),
    adjustment: config => {
      const policyType = deepGet(config, 'compactionPolicy.type');
      const priorityDatasource = deepGet(config, 'compactionPolicy.priorityDatasource');
      const newPolicy: Record<string, any> = { type: policyType };
      if (priorityDatasource) {
        newPolicy.priorityDatasource = priorityDatasource;
      }
      return deepSet(config, 'compactionPolicy', newPolicy);
    },
  },
  {
    name: 'compactionPolicy.priorityDatasource',
    type: 'string',
    defined: config =>
      deepGet(config, 'compactionPolicy.type') === 'newestSegmentFirst' ||
      deepGet(config, 'compactionPolicy.type') === 'mostFragmentedFirst',
    placeholder: '(none)',
    info: (
      <>
        Datasource to prioritize for compaction. The intervals of this datasource are chosen for
        compaction before the intervals of any other datasource. Within this datasource, the
        intervals are prioritized based on the chosen compaction policy.
      </>
    ),
  },
  {
    name: 'compactionPolicy.minUncompactedCount',
    label: 'Min uncompacted segment count',
    type: 'number',
    defaultValue: 100,
    min: 1,
    defined: config => deepGet(config, 'compactionPolicy.type') === 'mostFragmentedFirst',
    info: (
      <>
        Minimum number of uncompacted segments that must be present in an interval to make it
        eligible for compaction.
      </>
    ),
  },
  {
    name: 'compactionPolicy.minUncompactedBytes',
    label: 'Min uncompacted bytes',
    type: 'size-bytes',
    defaultValue: 10485760,
    defined: config => deepGet(config, 'compactionPolicy.type') === 'mostFragmentedFirst',
    info: (
      <>
        Minimum total bytes of uncompacted segments that must be present in an interval to make it
        eligible for compaction. Default: 10 MiB.
      </>
    ),
  },
  {
    name: 'compactionPolicy.maxAverageUncompactedBytesPerSegment',
    label: 'Max avg uncompacted segment size',
    type: 'size-bytes',
    defaultValue: 2147483648,
    defined: config => deepGet(config, 'compactionPolicy.type') === 'mostFragmentedFirst',
    info: (
      <>
        Maximum average size of uncompacted segments in an interval eligible for compaction.
        Intervals where the average uncompacted segment is larger than this are skipped. Default: 2
        GiB.
      </>
    ),
  },
  {
    name: 'compactionPolicy.minUncompactedBytesPercentForFullCompaction',
    label: 'Min uncompacted bytes % for full compaction',
    type: 'number',
    defaultValue: 0,
    min: 0,
    max: 99,
    defined: config => deepGet(config, 'compactionPolicy.type') === 'mostFragmentedFirst',
    info: (
      <>
        Threshold percentage of uncompacted bytes to total bytes. When the ratio of uncompacted
        bytes falls below this threshold, minor compaction is used instead of full compaction. Set
        to 0 (default) to always use full compaction.
      </>
    ),
  },
  {
    name: 'compactionPolicy.minUncompactedRowsPercentForFullCompaction',
    label: 'Min uncompacted rows % for full compaction',
    type: 'number',
    defaultValue: 0,
    min: 0,
    max: 99,
    defined: config => deepGet(config, 'compactionPolicy.type') === 'mostFragmentedFirst',
    info: (
      <>
        Threshold percentage of uncompacted rows to total rows. When the ratio of uncompacted rows
        falls below this threshold, minor compaction is used instead of full compaction. Set to 0
        (default) to always use full compaction.
      </>
    ),
  },
  {
    name: 'storeCompactionStatePerSegment',
    label: 'Legacy: Persist last compaction state in segments',
    type: 'boolean',
    defaultValue: true,
    info: (
      <>
        <p>
          Whether to persist the full compaction state in segment metadata. When <Code>true</Code>{' '}
          (default), compaction state is stored in both the segment metadata and the compaction
          states table.
        </p>
        <p>
          When <Code>false</Code>, only a fingerprint reference is stored in the segment metadata,
          reducing storage overhead in the segments table. The actual compaction state is stored in
          the compaction states table.
        </p>
        <p>
          <strong>Note:</strong> Eventually this configuration will be removed and all compaction
          will use the fingerprint method only. This configuration exists for operators to opt into
          this future pattern early.
        </p>
      </>
    ),
  },
];
