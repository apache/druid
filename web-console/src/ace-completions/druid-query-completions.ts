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

import type { CompletionRule } from './json-completion-utils';

export const DRUID_QUERY_COMPLETIONS: CompletionRule[] = [
  // Root level - when starting a new query
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'queryType', documentation: 'The type of query to execute' },
      { value: 'dataSource', documentation: 'The data source to query' },
    ],
  },

  // Query type values
  {
    path: '$.queryType',
    completions: [
      {
        value: 'timeseries',

        documentation: 'Timeseries query for time-based aggregations',
      },
      {
        value: 'topN',

        documentation: 'TopN query to get the top N dimension values',
      },
      { value: 'groupBy', documentation: 'GroupBy query for grouped aggregations' },
      { value: 'scan', documentation: 'Scan query to return raw Druid rows' },
      { value: 'search', documentation: 'Search query to find dimension values' },
      {
        value: 'timeBoundary',

        documentation: 'Time boundary query to find data time range',
      },
      { value: 'segmentMetadata', documentation: 'Segment metadata query' },
      { value: 'dataSourceMetadata', documentation: 'Data source metadata query' },
    ],
  },

  // Common properties for most query types
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType !== 'dataSourceMetadata',
    completions: [
      { value: 'intervals', documentation: 'Time intervals to query' },
      { value: 'filter', documentation: 'Filter to apply to the query' },
      { value: 'context', documentation: 'Query context parameters' },
    ],
  },

  // Timeseries query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'timeseries',
    completions: [
      { value: 'granularity', documentation: 'Time granularity for bucketing' },
      { value: 'aggregations', documentation: 'Aggregations to compute' },
      {
        value: 'postAggregations',

        documentation: 'Post-aggregations to compute',
      },
      {
        value: 'descending',

        documentation: 'Whether to sort results in descending order',
      },
    ],
  },

  // TopN query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'topN',
    completions: [
      {
        value: 'dimension',

        documentation: 'The dimension to get top values for',
      },
      { value: 'threshold', documentation: 'The number of top values to return' },
      { value: 'metric', documentation: 'The metric to sort by' },
      { value: 'granularity', documentation: 'Time granularity for bucketing' },
      { value: 'aggregations', documentation: 'Aggregations to compute' },
      {
        value: 'postAggregations',

        documentation: 'Post-aggregations to compute',
      },
    ],
  },

  // GroupBy query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'groupBy',
    completions: [
      { value: 'dimensions', documentation: 'Dimensions to group by' },
      { value: 'granularity', documentation: 'Time granularity for bucketing' },
      { value: 'aggregations', documentation: 'Aggregations to compute' },
      {
        value: 'postAggregations',

        documentation: 'Post-aggregations to compute',
      },
      { value: 'having', documentation: 'Having clause to filter groups' },
      { value: 'limitSpec', documentation: 'Limit and ordering specification' },
    ],
  },

  // Scan query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'scan',
    completions: [
      { value: 'columns', documentation: 'Columns to return' },
      { value: 'limit', documentation: 'Maximum number of rows to return' },
      { value: 'offset', documentation: 'Number of rows to skip' },
      { value: 'resultFormat', documentation: 'Format of the result' },
      { value: 'batchSize', documentation: 'Batch size for streaming' },
      { value: 'legacy', documentation: 'Use legacy scan query mode' },
    ],
  },

  // Granularity simple values
  {
    path: '$.granularity',
    completions: [
      { value: 'all', documentation: 'All data in a single bucket' },
      { value: 'none', documentation: 'No time bucketing' },
      { value: 'second', documentation: 'Bucket by second' },
      { value: 'minute', documentation: 'Bucket by minute' },
      { value: 'fifteen_minute', documentation: 'Bucket by 15 minutes' },
      { value: 'thirty_minute', documentation: 'Bucket by 30 minutes' },
      { value: 'hour', documentation: 'Bucket by hour' },
      { value: 'day', documentation: 'Bucket by day' },
      { value: 'week', documentation: 'Bucket by week' },
      { value: 'month', documentation: 'Bucket by month' },
      { value: 'quarter', documentation: 'Bucket by quarter' },
      { value: 'year', documentation: 'Bucket by year' },
    ],
  },

  // Granularity object properties (when granularity is an object)
  {
    path: '$.granularity',
    isObject: true,
    condition: obj => typeof obj === 'object' && obj !== null,
    completions: [
      { value: 'type', documentation: 'Type of granularity' },
      { value: 'period', documentation: 'ISO 8601 period' },
      { value: 'timeZone', documentation: 'Timezone for bucketing' },
      { value: 'origin', documentation: 'Origin timestamp' },
    ],
  },

  // Aggregation properties
  {
    path: '$.aggregations[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of aggregation' },
      { value: 'name', documentation: 'Output name for this aggregation' },
    ],
  },

  // Aggregation types
  {
    path: '$.aggregations[].type',
    completions: [
      { value: 'count', documentation: 'Count aggregator' },
      { value: 'longSum', documentation: 'Sum aggregator for long values' },
      { value: 'doubleSum', documentation: 'Sum aggregator for double values' },
      { value: 'floatSum', documentation: 'Sum aggregator for float values' },
      { value: 'longMin', documentation: 'Min aggregator for long values' },
      { value: 'doubleMin', documentation: 'Min aggregator for double values' },
      { value: 'floatMin', documentation: 'Min aggregator for float values' },
      { value: 'longMax', documentation: 'Max aggregator for long values' },
      { value: 'doubleMax', documentation: 'Max aggregator for double values' },
      { value: 'floatMax', documentation: 'Max aggregator for float values' },
      { value: 'doubleFirst', documentation: 'First value aggregator for doubles' },
      { value: 'doubleLast', documentation: 'Last value aggregator for doubles' },
      { value: 'floatFirst', documentation: 'First value aggregator for floats' },
      { value: 'floatLast', documentation: 'Last value aggregator for floats' },
      { value: 'longFirst', documentation: 'First value aggregator for longs' },
      { value: 'longLast', documentation: 'Last value aggregator for longs' },
      { value: 'stringFirst', documentation: 'First value aggregator for strings' },
      { value: 'stringLast', documentation: 'Last value aggregator for strings' },
      {
        value: 'thetaSketch',

        documentation: 'Theta sketch for approximate distinct count',
      },
      {
        value: 'HLLSketchBuild',

        documentation: 'HLL sketch for approximate distinct count',
      },
      {
        value: 'quantilesDoublesSketch',

        documentation: 'Quantiles sketch for doubles',
      },
      {
        value: 'hyperUnique',

        documentation: 'HyperLogLog for approximate distinct count',
      },
      { value: 'cardinality', documentation: 'Cardinality aggregator' },
      { value: 'histogram', documentation: 'Approximate histogram aggregator' },
      {
        value: 'fixedBucketsHistogram',

        documentation: 'Fixed buckets histogram aggregator',
      },
    ],
  },

  // Aggregation properties for field-based aggregators
  {
    path: '$.aggregations[]',
    isObject: true,
    condition: obj =>
      obj.type &&
      [
        'longSum',
        'doubleSum',
        'floatSum',
        'longMin',
        'doubleMin',
        'floatMin',
        'longMax',
        'doubleMax',
        'floatMax',
      ].includes(obj.type),
    completions: [{ value: 'fieldName', documentation: 'The field to aggregate' }],
  },

  // Filter types
  {
    path: '$.filter',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of filter' }],
  },

  {
    path: '$.filter.type',
    completions: [
      { value: 'selector', documentation: 'Matches a specific dimension value' },
      { value: 'in', documentation: 'Matches any of the given dimension values' },
      { value: 'bound', documentation: 'Matches dimension values within bounds' },
      { value: 'interval', documentation: 'Matches time intervals' },
      {
        value: 'like',

        documentation: 'Matches dimension values using LIKE pattern',
      },
      { value: 'regex', documentation: 'Matches dimension values using regex' },
      { value: 'search', documentation: 'Matches dimension values using search' },
      { value: 'and', documentation: 'AND filter combinator' },
      { value: 'or', documentation: 'OR filter combinator' },
      { value: 'not', documentation: 'NOT filter combinator' },
      { value: 'true', documentation: 'Always matches' },
      { value: 'false', documentation: 'Never matches' },
      { value: 'null', documentation: 'Matches null or missing values' },
    ],
  },

  // Selector filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'selector',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'value', documentation: 'The value to match' },
    ],
  },

  // In filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'in',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'values', documentation: 'The values to match' },
    ],
  },

  // AND/OR filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'and' || obj.type === 'or',
    completions: [{ value: 'fields', documentation: 'Array of filters to combine' }],
  },

  // NOT filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'not',
    completions: [{ value: 'field', documentation: 'Filter to negate' }],
  },

  // Dimension spec properties (when dimension is an object)
  {
    path: '$.dimensions[]',
    isObject: true,
    condition: obj => typeof obj === 'object' && obj !== null,
    completions: [
      { value: 'type', documentation: 'Type of dimension spec' },
      { value: 'dimension', documentation: 'The dimension field' },
      { value: 'outputName', documentation: 'Output name for this dimension' },
    ],
  },

  // Dimension spec types
  {
    path: '$.dimensions[].type',
    completions: [
      { value: 'default', documentation: 'Default dimension spec' },
      {
        value: 'extraction',

        documentation: 'Dimension spec with extraction function',
      },
      {
        value: 'listFiltered',

        documentation: 'Dimension spec with value filtering',
      },
      { value: 'lookup', documentation: 'Dimension spec with lookup' },
      {
        value: 'prefixFiltered',

        documentation: 'Dimension spec with prefix filtering',
      },
      {
        value: 'regexFiltered',

        documentation: 'Dimension spec with regex filtering',
      },
    ],
  },

  // Query context properties
  {
    path: '$.context',
    isObject: true,
    completions: [
      { value: 'timeout', documentation: 'Query timeout in milliseconds' },
      {
        value: 'priority',

        documentation: 'Query priority (higher = more important)',
      },
      { value: 'queryId', documentation: 'Unique identifier for this query' },
      { value: 'useCache', documentation: 'Whether to use cached results' },
      { value: 'populateCache', documentation: 'Whether to populate the cache' },
      {
        value: 'useResultLevelCache',

        documentation: 'Whether to use result level cache',
      },
      {
        value: 'populateResultLevelCache',

        documentation: 'Whether to populate result level cache',
      },
      { value: 'bySegment', documentation: 'Return results by segment' },
      { value: 'finalize', documentation: 'Whether to finalize aggregations' },
      {
        value: 'maxScatterGatherBytes',

        documentation: 'Maximum bytes for scatter-gather',
      },
      {
        value: 'maxQueuedBytes',

        documentation: 'Maximum bytes queued per query',
      },
      {
        value: 'serializeDateTimeAsLong',

        documentation: 'Serialize DateTime as long',
      },
      {
        value: 'serializeDateTimeAsLongInner',

        documentation: 'Inner DateTime serialization',
      },
    ],
  },

  // Result format for scan queries
  {
    path: '$.resultFormat',
    completions: [
      { value: 'list', documentation: 'Results as a list of rows' },
      { value: 'compactedList', documentation: 'Results as a compacted list' },
      { value: 'valueVector', documentation: 'Results as value vectors' },
    ],
  },
];
