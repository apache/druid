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

import { Field } from '../components';
import { deepGet, deepSet, oneOf } from '../utils';

export type CompactionConfig = Record<string, any>;

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
    suggestions: ['dynamic', 'hashed', 'single_dim'],
    info: (
      <p>
        For perfect rollup, you should use either <Code>hashed</Code> (partitioning based on the
        hash of dimensions in each row) or <Code>single_dim</Code> (based on ranges of a single
        dimension). For best-effort rollup, you should use <Code>dynamic</Code>.
      </p>
    ),
  },
  // partitionsSpec type: dynamic
  {
    name: 'tuningConfig.partitionsSpec.maxRowsPerSegment',
    label: 'Max rows per segment',
    type: 'number',
    defaultValue: 5000000,
    defined: (t: CompactionConfig) => deepGet(t, 'tuningConfig.partitionsSpec.type') === 'dynamic',
    info: <>Determines how many rows are in each segment.</>,
  },
  {
    name: 'tuningConfig.partitionsSpec.maxTotalRows',
    label: 'Max total rows',
    type: 'number',
    defaultValue: 20000000,
    defined: (t: CompactionConfig) => deepGet(t, 'tuningConfig.partitionsSpec.type') === 'dynamic',
    info: <>Total number of rows in segments waiting for being pushed.</>,
  },
  // partitionsSpec type: hashed
  {
    name: 'tuningConfig.partitionsSpec.targetRowsPerSegment',
    label: 'Target rows per segment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: (t: CompactionConfig) =>
      deepGet(t, 'tuningConfig.partitionsSpec.type') === 'hashed' &&
      !deepGet(t, 'tuningConfig.partitionsSpec.numShards'),
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
      </>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.numShards',
    label: 'Num shards',
    type: 'number',
    zeroMeansUndefined: true,
    defined: (t: CompactionConfig) =>
      deepGet(t, 'tuningConfig.partitionsSpec.type') === 'hashed' &&
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment'),
    info: (
      <>
        <p>
          If you know the optimal number of shards and want to speed up the time it takes for
          compaction to run, set this field.
        </p>
        <p>
          Directly specify the number of shards to create. If this is specified and 'intervals' is
          specified in the granularitySpec, the index task can skip the determine
          intervals/partitions pass through the data.
        </p>
      </>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.partitionDimensions',
    label: 'Partition dimensions',
    type: 'string-array',
    placeholder: '(all dimensions)',
    defined: (t: CompactionConfig) => deepGet(t, 'tuningConfig.partitionsSpec.type') === 'hashed',
    info: <p>The dimensions to partition on. Leave blank to select all dimensions.</p>,
  },
  // partitionsSpec type: single_dim
  {
    name: 'tuningConfig.partitionsSpec.partitionDimension',
    label: 'Partition dimension',
    type: 'string',
    defined: (t: CompactionConfig) =>
      deepGet(t, 'tuningConfig.partitionsSpec.type') === 'single_dim',
    required: true,
    info: <p>The dimension to partition on.</p>,
  },
  {
    name: 'tuningConfig.partitionsSpec.targetRowsPerSegment',
    label: 'Target rows per segment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: (t: CompactionConfig) =>
      deepGet(t, 'tuningConfig.partitionsSpec.type') === 'single_dim' &&
      !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment'),
    required: (t: CompactionConfig) =>
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment'),
    info: (
      <p>
        Target number of rows to include in a partition, should be a number that targets segments of
        500MB~1GB.
      </p>
    ),
  },
  {
    name: 'tuningConfig.partitionsSpec.maxRowsPerSegment',
    label: 'Max rows per segment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: (t: CompactionConfig) =>
      deepGet(t, 'tuningConfig.partitionsSpec.type') === 'single_dim' &&
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment'),
    required: (t: CompactionConfig) =>
      !deepGet(t, 'tuningConfig.partitionsSpec.targetRowsPerSegment') &&
      !deepGet(t, 'tuningConfig.partitionsSpec.maxRowsPerSegment'),
    info: <p>Maximum number of rows to include in a partition.</p>,
  },
  {
    name: 'tuningConfig.partitionsSpec.assumeGrouped',
    label: 'Assume grouped',
    type: 'boolean',
    defaultValue: false,
    defined: (t: CompactionConfig) =>
      deepGet(t, 'tuningConfig.partitionsSpec.type') === 'single_dim',
    info: (
      <p>
        Assume that input data has already been grouped on time and dimensions. Ingestion will run
        faster, but may choose sub-optimal partitions if this assumption is violated.
      </p>
    ),
  },
  {
    name: 'tuningConfig.maxNumConcurrentSubTasks',
    label: 'Max num concurrent sub tasks',
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
    name: 'inputSegmentSizeBytes',
    type: 'number',
    defaultValue: 419430400,
    info: (
      <p>
        Maximum number of total segment bytes processed per compaction task. Since a time chunk must
        be processed in its entirety, if the segments for a particular time chunk have a total size
        in bytes greater than this parameter, compaction will not run for that time chunk. Because
        each compaction task runs with a single thread, setting this value too far above 1–2GB will
        result in compaction tasks taking an excessive amount of time.
      </p>
    ),
  },
  {
    name: 'tuningConfig.totalNumMergeTasks',
    label: 'Total num merge tasks',
    type: 'number',
    defaultValue: 10,
    min: 1,
    defined: (t: CompactionConfig) =>
      oneOf(deepGet(t, 'tuningConfig.partitionsSpec.type'), 'hashed', 'single_dim'),
    info: <>Maximum number of merge tasks which can be run at the same time.</>,
  },
  {
    name: 'tuningConfig.splitHintSpec.maxInputSegmentBytesPerTask',
    label: 'Max input segment bytes per task',
    type: 'number',
    defaultValue: 500000000,
    min: 1000000,
    adjustment: (t: CompactionConfig) => deepSet(t, 'tuningConfig.splitHintSpec.type', 'segments'),
    info: (
      <>
        Maximum number of bytes of input segments to process in a single task. If a single segment
        is larger than this number, it will be processed by itself in a single task (input segments
        are never split across tasks).
      </>
    ),
  },
];
