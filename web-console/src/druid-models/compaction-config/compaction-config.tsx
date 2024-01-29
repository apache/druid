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
import React from 'react';

import type { Field } from '../../components';
import { deepGet, deepSet, oneOfKnown } from '../../utils';

export interface CompactionConfig {
  dataSource: string;
  skipOffsetFromLatest?: string;
  tuningConfig?: any;
  [key: string]: any;

  // Deprecated:
  inputSegmentSizeBytes?: number;
}

export const NOOP_INPUT_SEGMENT_SIZE_BYTES = 100000000000000;

export function compactionConfigHasLegacyInputSegmentSizeBytesSet(
  config: CompactionConfig,
): boolean {
  return (
    typeof config.inputSegmentSizeBytes === 'number' &&
    config.inputSegmentSizeBytes < NOOP_INPUT_SEGMENT_SIZE_BYTES
  );
}

const KNOWN_PARTITION_TYPES = ['dynamic', 'hashed', 'single_dim', 'range'];
export const COMPACTION_CONFIG_FIELDS: Field<CompactionConfig>[] = [
  {
    name: 'skipOffsetFromLatest',
    type: 'string',
    defaultValue: 'P1D',
    suggestions: ['PT0H', 'PT1H', 'P1D', 'P3D'],
    info: (
      <p>
        The offset for searching segments to be compacted. Strongly recommended to set for realtime
        dataSources.
      </p>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.type',
    label: 'Partitioning type',
    type: 'string',
    suggestions: ['dynamic', 'hashed', 'range'],
    info: (
      <p>
        For perfect rollup, you should use either <Code>hashed</Code> (partitioning based on the
        hash of dimensions in each row) or <Code>range</Code> (based on several dimensions). For
        best-effort rollup, you should use <Code>dynamic</Code>.
      </p>
    ),
  },
  // partitionsSpec type: dynamic
  {
    name: 'tuningConfig.partitionsSpec.maxRowsPerSegment',
    type: 'number',
    defaultValue: 5000000,
    defined: t =>
      oneOfKnown(deepGet(t, 'tuningConfig.partitionsSpec.type'), KNOWN_PARTITION_TYPES, 'dynamic'),
    info: <>Determines how many rows are in each segment.</>,
  },
  {
    name: 'tuningConfig.partitionsSpec.maxTotalRows',
    type: 'number',
    defaultValue: 20000000,
    defined: t =>
      oneOfKnown(deepGet(t, 'tuningConfig.partitionsSpec.type'), KNOWN_PARTITION_TYPES, 'dynamic'),
    info: <>Total number of rows in segments waiting for being pushed.</>,
  },
  // partitionsSpec type: hashed
  {
    name: 'tuningConfig.partitionsSpec.targetRowsPerSegment',
    type: 'number',
    zeroMeansUndefined: true,
    placeholder: `(defaults to 500000)`,
    defined: t =>
      oneOfKnown(deepGet(t, 'tuningConfig.partitionsSpec.type'), KNOWN_PARTITION_TYPES, 'hashed') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.numShards') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment'),
    info: (
      <>
        <p>
          If the segments generated are a sub-optimal size for the requested partition dimensions,
          consider setting this field.
        </p>
        <p>
          A target row count for each partition. Each partition will have a row count close to the
          target assuming evenly distributed keys. Defaults to 5 million if numShards is null.
        </p>
        <p>
          If <Code>numShards</Code> is left unspecified, the Parallel task will determine{' '}
          <Code>numShards</Code> automatically by <Code>targetRowsPerSegment</Code>.
        </p>
        <p>
          Note that either <Code>targetRowsPerSegment</Code> or <Code>numShards</Code> will be used
          to evenly distribute rows and find the best partitioning. Leave blank to show all
          properties.
        </p>
      </>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.maxRowsPerSegment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: t =>
      oneOfKnown(deepGet(t, 'tuningConfig.partitionsSpec.type'), KNOWN_PARTITION_TYPES, 'hashed') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.numShards') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment'),
    info: (
      <>
        <p>
          Target number of rows to include in a partition, should be a number that targets segments
          of 500MB~1GB.
        </p>
        <p>
          <Code>maxRowsPerSegment</Code> renamed to <Code>targetRowsPerSegment</Code>
        </p>
        <p>
          If <Code>numShards</Code> is left unspecified, the Parallel task will determine{' '}
          <Code>numShards</Code> automatically by <Code>targetRowsPerSegment</Code>.
        </p>
        <p>
          Note that either <Code>targetRowsPerSegment</Code> or <Code>numShards</Code> will be used
          to evenly distribute rows and find the best partitioning. Leave blank to show all
          properties.
        </p>
      </>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.numShards',
    type: 'number',
    zeroMeansUndefined: true,
    defined: t =>
      oneOfKnown(deepGet(t, 'tuningConfig.partitionsSpec.type'), KNOWN_PARTITION_TYPES, 'hashed') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment'),
    info: (
      <>
        <p>
          If you know the optimal number of shards and want to speed up the time it takes for
          compaction to run, set this field.
        </p>
        <p>
          Directly specify the number of shards to create. If this is specified and
          &apos;intervals&apos; is specified in the granularitySpec, the index task can skip the
          determine intervals/partitions pass through the data.
        </p>
        <p>
          Note that either <Code>targetRowsPerSegment</Code> or <Code>numShards</Code> will be used
          to evenly distribute rows across partitions and find the best partitioning. Leave blank to
          show all properties.
        </p>
      </>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.partitionDimensions',
    type: 'string-array',
    placeholder: '(all dimensions)',
    defined: t =>
      oneOfKnown(deepGet(t, 'tuningConfig.partitionsSpec.type'), KNOWN_PARTITION_TYPES, 'hashed'),
    info: <p>The dimensions to partition on. Leave blank to select all dimensions.</p>,
  },
  // partitionsSpec type: single_dim, range
  {
    name: 'tuningConfig.partitionsSpec.partitionDimension',
    type: 'string',
    defined: t =>
      oneOfKnown(
        deepGet(t, 'tuningConfig.partitionsSpec.type'),
        KNOWN_PARTITION_TYPES,
        'single_dim',
      ),
    required: true,
    info: <p>The dimension to partition on.</p>,
  },
  {
    name: 'tuningConfig.partitionsSpec.partitionDimensions',
    type: 'string-array',
    defined: t =>
      oneOfKnown(deepGet(t, 'tuningConfig.partitionsSpec.type'), KNOWN_PARTITION_TYPES, 'range'),
    required: true,
    info: <p>The dimensions to partition on.</p>,
  },
  {
    name: 'tuningConfig.partitionsSpec.targetRowsPerSegment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: t =>
      oneOfKnown(
        deepGet(t, 'tuningConfig.partitionsSpec.type'),
        KNOWN_PARTITION_TYPES,
        'single_dim',
        'range',
      ) && !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment'),
    required: t =>
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment'),
    info: (
      <>
        <p>
          Target number of rows to include in a partition, should be a number that targets segments
          of 500MB~1GB.
        </p>
        <p>
          Note that either <Code>targetRowsPerSegment</Code> or <Code>maxRowsPerSegment</Code> will
          be used to find the best partitioning. Leave blank to show all properties.
        </p>
      </>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.maxRowsPerSegment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: t =>
      oneOfKnown(
        deepGet(t, 'tuningConfig.partitionsSpec.type'),
        KNOWN_PARTITION_TYPES,
        'single_dim',
        'range',
      ) && !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment'),
    required: t =>
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment'),
    info: (
      <>
        <p>Maximum number of rows to include in a partition.</p>
        <p>
          Note that either <Code>targetRowsPerSegment</Code> or <Code>maxRowsPerSegment</Code> will
          be used to find the best partitioning. Leave blank to show all properties.
        </p>
      </>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.assumeGrouped',
    type: 'boolean',
    defaultValue: false,
    defined: t =>
      oneOfKnown(
        deepGet(t, 'tuningConfig.partitionsSpec.type'),
        KNOWN_PARTITION_TYPES,
        'single_dim',
        'range',
      ),
    info: (
      <p>
        Assume that input data has already been grouped on time and dimensions. Ingestion will run
        faster, but may choose sub-optimal partitions if this assumption is violated.
      </p>
    ),
  },
  {
    name: 'tuningConfig.maxNumConcurrentSubTasks',
    type: 'number',
    defaultValue: 1,
    min: 1,
    info: (
      <>
        Maximum number of tasks which can be run at the same time. The supervisor task would spawn
        worker tasks up to maxNumConcurrentSubTasks regardless of the available task slots. If this
        value is set to 1, the supervisor task processes data ingestion on its own instead of
        spawning worker tasks. If this value is set to too large, too many worker tasks can be
        created which might block other ingestion.
      </>
    ),
  },
  {
    name: 'tuningConfig.maxColumnsToMerge',
    type: 'number',
    defaultValue: -1,
    min: -1,
    info: (
      <>
        <p>
          Limit of the number of segments to merge in a single phase when merging segments for
          publishing. This limit affects the total number of columns present in a set of segments to
          merge. If the limit is exceeded, segment merging occurs in multiple phases. Druid merges
          at least 2 segments per phase, regardless of this setting.
        </p>
        <p>Default: -1 (unlimited)</p>
      </>
    ),
  },
  {
    name: 'tuningConfig.totalNumMergeTasks',
    type: 'number',
    defaultValue: 10,
    min: 1,
    defined: t =>
      oneOfKnown(
        deepGet(t, 'tuningConfig.partitionsSpec.type'),
        KNOWN_PARTITION_TYPES,
        'hashed',
        'single_dim',
        'range',
      ),
    info: <>Maximum number of merge tasks which can be run at the same time.</>,
  },
  {
    name: 'tuningConfig.splitHintSpec.maxSplitSize',
    type: 'number',
    defaultValue: 1073741824,
    min: 1000000,
    hideInMore: true,
    adjustment: t => deepSet(t, 'tuningConfig.splitHintSpec.type', 'maxSize'),
    info: (
      <>
        Maximum number of bytes of input segments to process in a single task. If a single segment
        is larger than this number, it will be processed by itself in a single task (input segments
        are never split across tasks).
      </>
    ),
  },
  {
    name: 'tuningConfig.splitHintSpec.maxNumFiles',
    label: 'Max num files (segments)',
    type: 'number',
    defaultValue: 1000,
    min: 1,
    hideInMore: true,
    adjustment: t => deepSet(t, 'tuningConfig.splitHintSpec.type', 'maxSize'),
    info: (
      <>
        Maximum number of input segments to process in a single subtask. This limit is to avoid task
        failures when the ingestion spec is too long. There are two known limits on the max size of
        serialized ingestion spec, i.e., the max ZNode size in ZooKeeper (
        <Code>jute.maxbuffer</Code>) and the max packet size in MySQL (
        <Code>max_allowed_packet</Code>). These can make ingestion tasks fail if the serialized
        ingestion spec size hits one of them.
      </>
    ),
  },
];
