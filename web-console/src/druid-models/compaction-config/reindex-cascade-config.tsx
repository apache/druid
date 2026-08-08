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
import { v4 as uuidv4 } from 'uuid';

import type { Field } from '../../components';
import { deepGet, oneOfKnown, pluralIfNeeded } from '../../utils';

// --- Virtual column types ---

export interface ExpressionVirtualColumn {
  type: 'expression';
  name: string;
  expression: string;
  outputType?: string;
}

export const EXPRESSION_VIRTUAL_COLUMN_FIELDS: Field<ExpressionVirtualColumn>[] = [
  {
    name: 'name',
    type: 'string',
    required: true,
    info: <p>Output name of the virtual column.</p>,
  },
  {
    name: 'expression',
    type: 'string',
    required: true,
    info: <p>Druid expression that computes the virtual column value.</p>,
  },
  {
    name: 'outputType',
    type: 'string',
    suggestions: ['STRING', 'LONG', 'FLOAT', 'DOUBLE', 'COMPLEX'],
    info: <p>Output type of the expression result.</p>,
  },
];

export function newExpressionVirtualColumn(): ExpressionVirtualColumn {
  return {
    type: 'expression',
    name: '',
    expression: '',
  };
}

export function summarizeVirtualColumns(
  virtualColumns: ExpressionVirtualColumn[] | undefined,
): string {
  if (!virtualColumns?.length) return '(none)';
  return virtualColumns.map(vc => vc.name || '(unnamed)').join(', ');
}

// --- Reindexing rule types ---

export interface ReindexingRule {
  id: string;
  olderThan: string;
  description?: string;
}

export interface PartitioningRule extends ReindexingRule {
  segmentGranularity: string;
  partitionsSpec: any;
  virtualColumns?: any;
}

export interface DeletionRule extends ReindexingRule {
  deleteWhere: any;
  virtualColumns?: any;
}

export interface IndexSpecRule extends ReindexingRule {
  indexSpec: any;
}

export interface DataSchemaRule extends ReindexingRule {
  dimensionsSpec?: any;
  metricsSpec?: any[];
  queryGranularity?: string;
  rollup?: boolean;
  projections?: any[];
}

// --- Rule provider types ---

export interface InlineRuleProvider {
  type: 'inline';
  partitioningRules?: PartitioningRule[];
  deletionRules?: DeletionRule[];
  indexSpecRules?: IndexSpecRule[];
  dataSchemaRules?: DataSchemaRule[];
}

export type RuleProvider = InlineRuleProvider;

// --- Top-level reindex cascade config ---

export interface ReindexCascadeConfig {
  type: 'reindexCascade';
  dataSource: string;
  defaultSegmentGranularity: string;
  defaultPartitionsSpec: any;
  defaultPartitioningVirtualColumns?: any;
  ruleProvider: RuleProvider;
  taskPriority?: number;
  inputSegmentSizeBytes?: number;
  taskContext?: Record<string, any>;
  skipOffsetFromLatest?: string;
  skipOffsetFromNow?: string;
  tuningConfig?: any;
}

// --- Factory functions for new rules ---

export function newPartitioningRule(): PartitioningRule {
  return {
    id: uuidv4(),
    olderThan: 'P30D',
    segmentGranularity: 'DAY',
    partitionsSpec: { type: 'dynamic', maxRowsPerSegment: 5000000 },
  };
}

export function newDeletionRule(): DeletionRule {
  return {
    id: uuidv4(),
    olderThan: 'P90D',
    deleteWhere: { type: 'equals', column: '', matchValueType: 'STRING', matchValue: '' },
  };
}

export function newIndexSpecRule(): IndexSpecRule {
  return {
    id: uuidv4(),
    olderThan: 'P90D',
    indexSpec: {},
  };
}

export function newDataSchemaRule(): DataSchemaRule {
  return {
    id: uuidv4(),
    olderThan: 'P30D',
  };
}

// --- Summary helpers ---

export function summarizeRuleProvider(ruleProvider: RuleProvider | undefined): string {
  if (!ruleProvider) return '(none)';
  const parts: string[] = [];
  if (ruleProvider.partitioningRules?.length) {
    parts.push(pluralIfNeeded(ruleProvider.partitioningRules.length, 'partitioning rule'));
  }
  if (ruleProvider.deletionRules?.length) {
    parts.push(pluralIfNeeded(ruleProvider.deletionRules.length, 'deletion rule'));
  }
  if (ruleProvider.indexSpecRules?.length) {
    parts.push(pluralIfNeeded(ruleProvider.indexSpecRules.length, 'index spec rule'));
  }
  if (ruleProvider.dataSchemaRules?.length) {
    parts.push(pluralIfNeeded(ruleProvider.dataSchemaRules.length, 'data schema rule'));
  }
  return parts.length ? parts.join(', ') : '(no rules)';
}

export function summarizeRule(rule: ReindexingRule): string {
  return rule.description || rule.id;
}

// --- Shared field constants ---

const SEGMENT_GRANULARITY_SUGGESTIONS = [
  'MINUTE',
  'FIFTEEN_MINUTE',
  'HOUR',
  'DAY',
  'MONTH',
  'QUARTER',
  'YEAR',
];

const KNOWN_DEFAULT_PARTITION_TYPES = ['dynamic', 'range'];

const PERIOD_SUGGESTIONS = ['P1D', 'P7D', 'P30D', 'P90D', 'P180D', 'P365D'];

// --- Top-level fields for ReindexCascadeConfig ---

export const REINDEX_CASCADE_CONFIG_FIELDS: Field<ReindexCascadeConfig>[] = [
  {
    name: 'defaultSegmentGranularity',
    label: 'Default segment granularity',
    type: 'string',
    required: true,
    suggestions: SEGMENT_GRANULARITY_SUGGESTIONS,
    info: (
      <p>
        Segment granularity used for intervals where no partitioning rule matches. This is the
        default time bucketing for segments.
      </p>
    ),
  },
  {
    name: 'defaultPartitionsSpec.type',
    label: 'Default partitioning type',
    type: 'string',
    suggestions: ['dynamic', 'range'],
    info: (
      <p>
        Partitioning strategy used for intervals where no partitioning rule matches. Use{' '}
        <Code>dynamic</Code> for best-effort rollup or <Code>range</Code> for range-based
        partitioning.
      </p>
    ),
  },
  // defaultPartitionsSpec: dynamic
  {
    name: 'defaultPartitionsSpec.maxRowsPerSegment',
    label: 'Default max rows per segment',
    type: 'number',
    defaultValue: 5000000,
    defined: c =>
      oneOfKnown(
        deepGet(c, 'defaultPartitionsSpec.type'),
        KNOWN_DEFAULT_PARTITION_TYPES,
        'dynamic',
      ),
    info: <>Determines how many rows are in each segment.</>,
  },
  {
    name: 'defaultPartitionsSpec.maxTotalRows',
    label: 'Default max total rows',
    type: 'number',
    defaultValue: 20000000,
    defined: c =>
      oneOfKnown(
        deepGet(c, 'defaultPartitionsSpec.type'),
        KNOWN_DEFAULT_PARTITION_TYPES,
        'dynamic',
      ),
    info: <>Total number of rows in segments waiting for being pushed.</>,
  },
  // defaultPartitionsSpec: range
  {
    name: 'defaultPartitionsSpec.partitionDimensions',
    label: 'Default partition dimensions',
    type: 'string-array',
    defined: c =>
      oneOfKnown(deepGet(c, 'defaultPartitionsSpec.type'), KNOWN_DEFAULT_PARTITION_TYPES, 'range'),
    required: true,
    info: <p>The dimensions to partition on.</p>,
  },
  {
    name: 'defaultPartitionsSpec.targetRowsPerSegment',
    label: 'Default target rows per segment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: c =>
      oneOfKnown(deepGet(c, 'defaultPartitionsSpec.type'), KNOWN_DEFAULT_PARTITION_TYPES, 'range'),
    required: c =>
      !deepGet(c, 'defaultPartitionsSpec.targetRowsPerSegment') &&
      !deepGet(c, 'defaultPartitionsSpec.maxRowsPerSegment'),
    info: (
      <p>
        Target number of rows per segment. Either <Code>targetRowsPerSegment</Code> or{' '}
        <Code>maxRowsPerSegment</Code> must be set.
      </p>
    ),
  },
  {
    name: 'defaultPartitioningVirtualColumns',
    label: 'Default partitioning virtual columns',
    type: 'custom',
    defined: c =>
      oneOfKnown(deepGet(c, 'defaultPartitionsSpec.type'), KNOWN_DEFAULT_PARTITION_TYPES, 'range'),
    customSummary: summarizeVirtualColumns,
    info: (
      <p>
        Optional virtual columns used if your default partitions spec range partitioning definition
        references virtual columns.
      </p>
    ),
    // customDialog is set by the CompactionConfigDialog component
  },
  // ruleProvider (custom dialog)
  {
    name: 'ruleProvider',
    label: 'Rule provider',
    type: 'custom',
    required: true,
    customSummary: summarizeRuleProvider,
    info: (
      <p>
        Configure the reindexing rules that control how data is compacted as it ages. Rules define
        partitioning, deletion, index spec, and data schema changes.
      </p>
    ),
    // customDialog is set by the CompactionConfigDialog component
  },
  // Skip offset - virtual selector with 3 options: disabled, fromLatest, fromNow
  {
    name: 'skipOffsetType',
    label: 'Skip offset',
    type: 'string',
    suggestions: ['disabled', 'fromLatest', 'fromNow'],
    defaultValue: (c: ReindexCascadeConfig) => {
      if (c.skipOffsetFromNow) return 'fromNow';
      if (c.skipOffsetFromLatest) return 'fromLatest';
      return 'disabled';
    },
    adjustment: c => {
      const skipType = (c as any).skipOffsetType;
      const adjusted = { ...c };
      if (skipType === 'fromNow') {
        if (!adjusted.skipOffsetFromNow) {
          adjusted.skipOffsetFromNow = adjusted.skipOffsetFromLatest || 'P1D';
        }
        delete adjusted.skipOffsetFromLatest;
      } else if (skipType === 'fromLatest') {
        if (!adjusted.skipOffsetFromLatest) {
          adjusted.skipOffsetFromLatest = adjusted.skipOffsetFromNow || 'P1D';
        }
        delete adjusted.skipOffsetFromNow;
      } else {
        // disabled
        delete adjusted.skipOffsetFromLatest;
        delete adjusted.skipOffsetFromNow;
      }
      delete (adjusted as any).skipOffsetType;
      return adjusted;
    },
    info: (
      <p>
        Choose whether to skip recent data and how the offset is calculated. <Code>disabled</Code>{' '}
        means no skip offset. <Code>fromLatest</Code> skips relative to the end of the latest
        segment. <Code>fromNow</Code> skips relative to the current time.
      </p>
    ),
  },
  {
    name: 'skipOffsetFromLatest',
    label: 'Skip offset value',
    type: 'string',
    suggestions: ['PT0H', 'PT1H', 'P1D', 'P3D'],
    defined: c => Boolean(c.skipOffsetFromLatest),
    info: (
      <p>ISO 8601 period. Skips data newer than this offset from the end of the latest segment.</p>
    ),
  },
  {
    name: 'skipOffsetFromNow',
    label: 'Skip offset value',
    type: 'string',
    suggestions: ['PT0H', 'PT1H', 'P1D', 'P3D'],
    defined: c => Boolean(c.skipOffsetFromNow),
    info: <p>ISO 8601 period. Skips data newer than this offset from the current time.</p>,
  },
  {
    name: 'taskPriority',
    type: 'number',
    defaultValue: 25,
    min: 0,
    hideInMore: true,
    info: <p>Priority of compaction tasks.</p>,
  },
  {
    name: 'inputSegmentSizeBytes',
    type: 'size-bytes',
    hideInMore: true,
    info: <p>Maximum total input segment size in bytes per compaction task.</p>,
  },
  // Promoted task context fields
  {
    name: 'taskContext.useConcurrentLocks',
    label: 'Task context: concurrent locks',
    type: 'boolean',
    defaultValue: false,
    info: (
      <p>
        Enable concurrent append and replace for the datasource. Recommended if you are appending
        data to a datasource while compaction is running.
      </p>
    ),
  },
  {
    name: 'taskContext.maxNumTasks',
    label: 'Task context: max num tasks',
    type: 'number',
    min: 2,
    placeholder: '(cluster default)',
    zeroMeansUndefined: true,
    info: (
      <p>
        Maximum number of tasks (including the controller) for MSQ compaction. Must be at least 2
        (one controller, one worker).
      </p>
    ),
  },
  {
    name: 'taskContext.maxRowsInMemory',
    label: 'Task context: max rows in memory',
    type: 'number',
    zeroMeansUndefined: true,
    placeholder: '(default)',
    hideInMore: true,
    info: (
      <p>
        Maximum number of rows to hold in memory before persisting. Lower values reduce memory usage
        but may increase disk I/O.
      </p>
    ),
  },
  {
    name: 'taskContext.maxFrameSize',
    label: 'Task context: max frame size',
    type: 'number',
    zeroMeansUndefined: true,
    placeholder: '(default)',
    hideInMore: true,
    info: (
      <p>
        Maximum frame size in bytes for MSQ tasks. Increase if tasks fail due to frame size limits.
      </p>
    ),
  },
  {
    name: 'taskContext',
    label: 'Task context: additional settings',
    type: 'json',
    hideInMore: true,
    info: (
      <p>
        Full task context map. Common settings are available as dedicated fields above. Use this to
        set additional MSQ context parameters.
      </p>
    ),
  },
  {
    name: 'tuningConfig',
    type: 'json',
    hideInMore: true,
    info: (
      <p>
        Tuning config for compaction tasks. Note: you cannot set <Code>partitionsSpec</Code> inside{' '}
        <Code>tuningConfig</Code> for cascading reindexing — partitioning is controlled by rules and
        defaults.
      </p>
    ),
  },
];

// --- Field arrays for individual rule types ---

export const PARTITIONING_RULE_FIELDS: Field<PartitioningRule>[] = [
  {
    name: 'id',
    type: 'string',
    required: true,
    info: <p>Unique identifier for this rule.</p>,
  },
  {
    name: 'olderThan',
    type: 'string',
    required: true,
    suggestions: PERIOD_SUGGESTIONS,
    info: (
      <p>
        ISO 8601 period defining the age threshold. The rule applies to data older than the current
        time minus this period.
      </p>
    ),
  },
  {
    name: 'description',
    type: 'string',
    info: <p>Human-readable description of this rule.</p>,
  },
  {
    name: 'segmentGranularity',
    type: 'string',
    required: true,
    suggestions: SEGMENT_GRANULARITY_SUGGESTIONS,
    info: <p>Time granularity for segment buckets.</p>,
  },
  {
    name: 'partitionsSpec.type',
    label: 'Partitioning type',
    type: 'string',
    suggestions: ['dynamic', 'range'],
    info: (
      <p>
        Use <Code>dynamic</Code> for best-effort rollup or <Code>range</Code> for range-based
        partitioning.
      </p>
    ),
  },
  {
    name: 'partitionsSpec.maxRowsPerSegment',
    label: 'Max rows per segment',
    type: 'number',
    defaultValue: 5000000,
    defined: r => oneOfKnown(deepGet(r, 'partitionsSpec.type'), ['dynamic', 'range'], 'dynamic'),
    info: <>Determines how many rows are in each segment.</>,
  },
  {
    name: 'partitionsSpec.partitionDimensions',
    label: 'Partition dimensions',
    type: 'string-array',
    defined: r => deepGet(r, 'partitionsSpec.type') === 'range',
    required: true,
    info: <p>The dimensions to partition on.</p>,
  },
  {
    name: 'partitionsSpec.targetRowsPerSegment',
    label: 'Target rows per segment',
    type: 'number',
    zeroMeansUndefined: true,
    defined: r => deepGet(r, 'partitionsSpec.type') === 'range',
    info: <p>Target number of rows per segment for range partitioning.</p>,
  },
  {
    name: 'virtualColumns',
    type: 'custom',
    defined: r => deepGet(r, 'partitionsSpec.type') === 'range',
    customSummary: summarizeVirtualColumns,
    info: <p>Virtual columns for partitioning by nested or derived fields.</p>,
    // customDialog is set by the RuleProviderEditor component
  },
];

export const DELETION_RULE_FIELDS: Field<DeletionRule>[] = [
  {
    name: 'id',
    type: 'string',
    required: true,
    info: <p>Unique identifier for this rule.</p>,
  },
  {
    name: 'olderThan',
    type: 'string',
    required: true,
    suggestions: PERIOD_SUGGESTIONS,
    info: (
      <p>
        ISO 8601 period defining the age threshold. The rule applies to data older than the current
        time minus this period.
      </p>
    ),
  },
  {
    name: 'description',
    type: 'string',
    info: <p>Human-readable description of this rule.</p>,
  },
  {
    name: 'deleteWhere',
    type: 'json',
    required: true,
    info: (
      <p>
        A Druid filter matching rows to <strong>delete</strong>. The compacted data retains rows
        that do not match this filter. Multiple deletion rules combine as{' '}
        <Code>NOT(A OR B OR C)</Code>.
      </p>
    ),
  },
  {
    name: 'virtualColumns',
    type: 'json',
    hideInMore: true,
    info: <p>Virtual columns for filtering on nested or derived fields.</p>,
  },
];

export const INDEX_SPEC_RULE_FIELDS: Field<IndexSpecRule>[] = [
  {
    name: 'id',
    type: 'string',
    required: true,
    info: <p>Unique identifier for this rule.</p>,
  },
  {
    name: 'olderThan',
    type: 'string',
    required: true,
    suggestions: PERIOD_SUGGESTIONS,
    info: (
      <p>
        ISO 8601 period defining the age threshold. The rule applies to data older than the current
        time minus this period.
      </p>
    ),
  },
  {
    name: 'description',
    type: 'string',
    info: <p>Human-readable description of this rule.</p>,
  },
  {
    name: 'indexSpec',
    type: 'json',
    required: true,
    info: (
      <p>
        An IndexSpec object defining bitmap type, metric compression, and other encoding settings
        for compacted segments.
      </p>
    ),
  },
];

export const DATA_SCHEMA_RULE_FIELDS: Field<DataSchemaRule>[] = [
  {
    name: 'id',
    type: 'string',
    required: true,
    info: <p>Unique identifier for this rule.</p>,
  },
  {
    name: 'olderThan',
    type: 'string',
    required: true,
    suggestions: PERIOD_SUGGESTIONS,
    info: (
      <p>
        ISO 8601 period defining the age threshold. The rule applies to data older than the current
        time minus this period.
      </p>
    ),
  },
  {
    name: 'description',
    type: 'string',
    info: <p>Human-readable description of this rule.</p>,
  },
  {
    name: 'queryGranularity',
    type: 'string',
    placeholder: '(unset)',
    suggestions: SEGMENT_GRANULARITY_SUGGESTIONS,
    info: (
      <p>
        Query granularity for the compacted segments. Leave unset to preserve existing granularity.
      </p>
    ),
  },
  {
    name: 'rollup',
    type: 'boolean',
    info: (
      <p>
        Whether to enable rollup. Set to <Code>true</Code> only when <Code>metricsSpec</Code> is
        defined.
      </p>
    ),
  },
  {
    name: 'metricsSpec',
    type: 'json',
    info: (
      <>
        <p>Array of aggregator factories for rollup metrics. Example:</p>
        <pre>
          {`[
  {
    "type": "longSum",
    "name": "added",
    "fieldName": "added"
  },
  {
    "type": "longSum",
    "name": "deleted",
    "fieldName": "deleted"
  }
]`}
        </pre>
      </>
    ),
  },
  {
    name: 'dimensionsSpec',
    type: 'json',
    info: (
      <>
        <p>Dimensions config for the compacted segments. Example:</p>
        <pre>{`{
  "dimensions": [
    "page",
    { "type": "string", "name": "channel" }
  ]
}`}</pre>
      </>
    ),
  },
  {
    name: 'projections',
    type: 'json',
    hideInMore: true,
    info: <p>List of aggregate projections.</p>,
  },
];

// --- Default config ---

export function newReindexCascadeConfig(dataSource: string): ReindexCascadeConfig {
  return {
    type: 'reindexCascade',
    dataSource,
    defaultSegmentGranularity: 'DAY',
    defaultPartitionsSpec: { type: 'dynamic', maxRowsPerSegment: 5000000 },
    ruleProvider: { type: 'inline' },
  };
}
