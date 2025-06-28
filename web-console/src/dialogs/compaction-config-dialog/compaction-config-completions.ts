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

export const COMPACTION_CONFIG_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'dataSource', documentation: 'Name of the datasource to compact' },
      {
        value: 'skipOffsetFromLatest',
        documentation: 'Time period to avoid compaction relative to latest segment',
      },
      { value: 'taskPriority', documentation: 'Priority of compaction tasks' },
      { value: 'tuningConfig', documentation: 'Tuning configuration for compaction tasks' },
      { value: 'ioConfig', documentation: 'IO configuration for compaction tasks' },
      { value: 'dimensionsSpec', documentation: 'Custom dimensions specification' },
      { value: 'transformSpec', documentation: 'Custom transform specification' },
      { value: 'metricsSpec', documentation: 'Custom metrics specification' },
      { value: 'granularitySpec', documentation: 'Granularity specification for compaction' },
      { value: 'taskContext', documentation: 'Task context for compaction tasks' },
    ],
  },
  // skipOffsetFromLatest common values
  {
    path: '$.skipOffsetFromLatest',
    completions: [
      { value: 'PT0H', documentation: 'No offset (compact all segments)' },
      { value: 'PT1H', documentation: '1 hour offset' },
      { value: 'PT6H', documentation: '6 hours offset' },
      { value: 'P1D', documentation: '1 day offset (recommended default)' },
      { value: 'P3D', documentation: '3 days offset' },
      { value: 'P1W', documentation: '1 week offset' },
    ],
  },
  // taskPriority values
  {
    path: '$.taskPriority',
    completions: [
      { value: '25', documentation: 'Default compaction priority' },
      { value: '50', documentation: 'Higher priority compaction' },
      { value: '75', documentation: 'High priority compaction' },
      { value: '100', documentation: 'Maximum priority compaction' },
    ],
  },
  // tuningConfig object properties
  {
    path: '$.tuningConfig',
    isObject: true,
    completions: [
      { value: 'partitionsSpec', documentation: 'Partitioning specification for segments' },
      { value: 'maxNumConcurrentSubTasks', documentation: 'Maximum number of concurrent subtasks' },
      { value: 'maxColumnsToMerge', documentation: 'Maximum columns to merge in a single phase' },
      { value: 'totalNumMergeTasks', documentation: 'Maximum number of merge tasks' },
      { value: 'splitHintSpec', documentation: 'Hint specification for splitting input segments' },
      { value: 'indexSpec', documentation: 'Index specification for segments' },
    ],
  },
  // partitionsSpec object properties
  {
    path: '$.tuningConfig.partitionsSpec',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of partitioning scheme' }],
  },
  // partitionsSpec.type values
  {
    path: '$.tuningConfig.partitionsSpec.type',
    completions: [
      { value: 'dynamic', documentation: 'Dynamic partitioning (best-effort rollup)' },
      { value: 'hashed', documentation: 'Hash-based partitioning (perfect rollup)' },
      { value: 'range', documentation: 'Range-based partitioning (perfect rollup)' },
      { value: 'single_dim', documentation: 'Single dimension partitioning (legacy)' },
    ],
  },
  // dynamic partitioning properties
  {
    path: '$.tuningConfig.partitionsSpec',
    isObject: true,
    condition: obj => obj.type === 'dynamic',
    completions: [
      {
        value: 'maxRowsPerSegment',
        documentation: 'Maximum rows per segment for dynamic partitioning',
      },
      {
        value: 'maxTotalRows',
        documentation: 'Maximum total rows in segments waiting to be pushed',
      },
    ],
  },
  // hashed partitioning properties
  {
    path: '$.tuningConfig.partitionsSpec',
    isObject: true,
    condition: obj => obj.type === 'hashed',
    completions: [
      { value: 'targetRowsPerSegment', documentation: 'Target rows per segment' },
      { value: 'maxRowsPerSegment', documentation: 'Maximum rows per segment (legacy)' },
      { value: 'numShards', documentation: 'Fixed number of shards' },
      { value: 'partitionDimensions', documentation: 'Dimensions to partition on' },
    ],
  },
  // range/single_dim partitioning properties
  {
    path: '$.tuningConfig.partitionsSpec',
    isObject: true,
    condition: obj => obj.type === 'range' || obj.type === 'single_dim',
    completions: [
      { value: 'targetRowsPerSegment', documentation: 'Target rows per segment' },
      { value: 'maxRowsPerSegment', documentation: 'Maximum rows per segment' },
      { value: 'assumeGrouped', documentation: 'Assume input data is already grouped' },
    ],
  },
  // range partitioning specific
  {
    path: '$.tuningConfig.partitionsSpec',
    isObject: true,
    condition: obj => obj.type === 'range',
    completions: [
      {
        value: 'partitionDimensions',
        documentation: 'Dimensions to partition on (required for range)',
      },
    ],
  },
  // single_dim partitioning specific
  {
    path: '$.tuningConfig.partitionsSpec',
    isObject: true,
    condition: obj => obj.type === 'single_dim',
    completions: [
      { value: 'partitionDimension', documentation: 'Single dimension to partition on (required)' },
    ],
  },
  // Common row count values
  {
    path: /^.*\.(?:maxRowsPerSegment|targetRowsPerSegment)$/,
    completions: [
      { value: '1000000', documentation: '1 million rows (small segments)' },
      { value: '5000000', documentation: '5 million rows (recommended default)' },
      { value: '10000000', documentation: '10 million rows (large segments)' },
      { value: '20000000', documentation: '20 million rows (very large segments)' },
    ],
  },
  // maxTotalRows values
  {
    path: '$.tuningConfig.partitionsSpec.maxTotalRows',
    completions: [
      { value: '20000000', documentation: '20 million rows (default)' },
      { value: '50000000', documentation: '50 million rows' },
      { value: '100000000', documentation: '100 million rows' },
    ],
  },
  // numShards values
  {
    path: '$.tuningConfig.partitionsSpec.numShards',
    completions: [
      { value: '2', documentation: '2 shards' },
      { value: '4', documentation: '4 shards' },
      { value: '8', documentation: '8 shards' },
      { value: '16', documentation: '16 shards' },
    ],
  },
  // assumeGrouped values
  {
    path: '$.tuningConfig.partitionsSpec.assumeGrouped',
    completions: [
      { value: 'false', documentation: 'Do not assume grouped (default, safer)' },
      { value: 'true', documentation: 'Assume data is grouped (faster but risky)' },
    ],
  },
  // maxNumConcurrentSubTasks values
  {
    path: '$.tuningConfig.maxNumConcurrentSubTasks',
    completions: [
      { value: '1', documentation: '1 task (sequential, default)' },
      { value: '2', documentation: '2 concurrent tasks' },
      { value: '4', documentation: '4 concurrent tasks' },
      { value: '8', documentation: '8 concurrent tasks' },
    ],
  },
  // maxColumnsToMerge values
  {
    path: '$.tuningConfig.maxColumnsToMerge',
    completions: [
      { value: '-1', documentation: 'Unlimited columns (default)' },
      { value: '100', documentation: '100 columns per merge phase' },
      { value: '500', documentation: '500 columns per merge phase' },
      { value: '1000', documentation: '1000 columns per merge phase' },
    ],
  },
  // totalNumMergeTasks values
  {
    path: '$.tuningConfig.totalNumMergeTasks',
    completions: [
      { value: '10', documentation: '10 merge tasks (default)' },
      { value: '5', documentation: '5 merge tasks' },
      { value: '20', documentation: '20 merge tasks' },
      { value: '50', documentation: '50 merge tasks' },
    ],
  },
  // splitHintSpec object properties
  {
    path: '$.tuningConfig.splitHintSpec',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of split hint specification' },
      { value: 'maxSplitSize', documentation: 'Maximum bytes of input segments per task' },
      { value: 'maxNumFiles', documentation: 'Maximum number of segments per task' },
    ],
  },
  // splitHintSpec.type values
  {
    path: '$.tuningConfig.splitHintSpec.type',
    completions: [
      { value: 'maxSize', documentation: 'Split based on maximum size' },
      { value: 'segments', documentation: 'Split based on segment count' },
    ],
  },
  // maxSplitSize values (in bytes)
  {
    path: '$.tuningConfig.splitHintSpec.maxSplitSize',
    completions: [
      { value: '1073741824', documentation: '1 GB (default)' },
      { value: '536870912', documentation: '512 MB' },
      { value: '2147483648', documentation: '2 GB' },
      { value: '5368709120', documentation: '5 GB' },
    ],
  },
  // maxNumFiles values
  {
    path: '$.tuningConfig.splitHintSpec.maxNumFiles',
    completions: [
      { value: '1000', documentation: '1000 segments (default)' },
      { value: '500', documentation: '500 segments' },
      { value: '2000', documentation: '2000 segments' },
      { value: '5000', documentation: '5000 segments' },
    ],
  },
  // granularitySpec object properties
  {
    path: '$.granularitySpec',
    isObject: true,
    completions: [
      { value: 'segmentGranularity', documentation: 'Granularity of segments' },
      { value: 'queryGranularity', documentation: 'Granularity of queries' },
      { value: 'rollup', documentation: 'Whether to enable rollup' },
      { value: 'intervals', documentation: 'Specific intervals to compact' },
    ],
  },
  // segmentGranularity values
  {
    path: '$.granularitySpec.segmentGranularity',
    completions: [
      { value: 'HOUR', documentation: 'Hourly segments' },
      { value: 'DAY', documentation: 'Daily segments (common choice)' },
      { value: 'WEEK', documentation: 'Weekly segments' },
      { value: 'MONTH', documentation: 'Monthly segments' },
      { value: 'YEAR', documentation: 'Yearly segments' },
    ],
  },
  // queryGranularity values
  {
    path: '$.granularitySpec.queryGranularity',
    completions: [
      { value: 'NONE', documentation: 'No query granularity aggregation' },
      { value: 'SECOND', documentation: 'Second-level aggregation' },
      { value: 'MINUTE', documentation: 'Minute-level aggregation' },
      { value: 'HOUR', documentation: 'Hour-level aggregation' },
      { value: 'DAY', documentation: 'Day-level aggregation' },
    ],
  },
  // rollup values
  {
    path: '$.granularitySpec.rollup',
    completions: [
      { value: 'true', documentation: 'Enable rollup (aggregation)' },
      { value: 'false', documentation: 'Disable rollup (raw data)' },
    ],
  },
  // taskContext object properties
  {
    path: '$.taskContext',
    isObject: true,
    completions: [
      { value: 'useConcurrentLocks', documentation: 'Enable concurrent append and replace' },
      { value: 'maxNumTasks', documentation: 'Maximum number of tasks for MSQ engine' },
      { value: 'useAutoColumnTypes', documentation: 'Enable automatic column type detection' },
      { value: 'storeCompactionState', documentation: 'Store compaction state in segments' },
    ],
  },
  // useConcurrentLocks values
  {
    path: '$.taskContext.useConcurrentLocks',
    completions: [
      { value: 'true', documentation: 'Enable concurrent locks for append during compaction' },
      { value: 'false', documentation: 'Disable concurrent locks (default)' },
    ],
  },
  // maxNumTasks values (for MSQ)
  {
    path: '$.taskContext.maxNumTasks',
    completions: [
      { value: '2', documentation: '2 tasks (minimum for MSQ)' },
      { value: '4', documentation: '4 tasks' },
      { value: '8', documentation: '8 tasks' },
      { value: '16', documentation: '16 tasks' },
    ],
  },
  // useAutoColumnTypes values
  {
    path: '$.taskContext.useAutoColumnTypes',
    completions: [
      { value: 'true', documentation: 'Auto-detect column types' },
      { value: 'false', documentation: 'Use existing column types (default)' },
    ],
  },
  // storeCompactionState values
  {
    path: '$.taskContext.storeCompactionState',
    completions: [
      { value: 'true', documentation: 'Store compaction state in segment metadata (default)' },
      { value: 'false', documentation: 'Do not store compaction state' },
    ],
  },
  // ioConfig object properties
  {
    path: '$.ioConfig',
    isObject: true,
    completions: [
      { value: 'allowBroadcastSegments', documentation: 'Allow broadcasting segments' },
      { value: 'dropExisting', documentation: 'Drop existing segments in intervals' },
    ],
  },
  // allowBroadcastSegments values
  {
    path: '$.ioConfig.allowBroadcastSegments',
    completions: [
      { value: 'true', documentation: 'Allow broadcasting segments' },
      { value: 'false', documentation: 'Do not broadcast segments (default)' },
    ],
  },
  // dropExisting values
  {
    path: '$.ioConfig.dropExisting',
    completions: [
      { value: 'true', documentation: 'Drop existing segments in compacted intervals' },
      { value: 'false', documentation: 'Keep existing segments (default)' },
    ],
  },
  // metricsSpec array properties
  {
    path: '$.metricsSpec',
    completions: [
      { value: 'longSum', documentation: 'Sum aggregator for long values' },
      { value: 'doubleSum', documentation: 'Sum aggregator for double values' },
      { value: 'count', documentation: 'Count aggregator' },
    ],
  },
  // metricsSpec object properties
  {
    path: '$.metricsSpec.[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of aggregator' },
      { value: 'name', documentation: 'Name of the metric' },
      { value: 'fieldName', documentation: 'Field to aggregate' },
    ],
  },
  // Common aggregator types
  {
    path: '$.metricsSpec.[].type',
    completions: [
      { value: 'longSum', documentation: 'Sum of long values' },
      { value: 'doubleSum', documentation: 'Sum of double values' },
      { value: 'longMin', documentation: 'Minimum of long values' },
      { value: 'longMax', documentation: 'Maximum of long values' },
      { value: 'doubleMin', documentation: 'Minimum of double values' },
      { value: 'doubleMax', documentation: 'Maximum of double values' },
      { value: 'count', documentation: 'Count of rows' },
    ],
  },
];
