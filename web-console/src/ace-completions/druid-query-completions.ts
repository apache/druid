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
      { value: 'virtualColumns', documentation: 'Virtual columns for computed values' },
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
      { value: 'limit', documentation: 'Maximum number of results to return' },
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
      { value: 'order', documentation: 'Result ordering (none, ascending, descending)' },
    ],
  },

  // Search query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'search',
    completions: [
      { value: 'granularity', documentation: 'Time granularity for bucketing' },
      { value: 'searchDimensions', documentation: 'Dimensions to search' },
      { value: 'query', documentation: 'Search query specification' },
      { value: 'sort', documentation: 'Sort specification for results' },
      { value: 'limit', documentation: 'Maximum number of results to return' },
    ],
  },

  // TimeBoundary query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'timeBoundary',
    completions: [
      {
        value: 'bound',
        documentation: 'Which boundary to return (minTime, maxTime, or null for both)',
      },
    ],
  },

  // SegmentMetadata query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'segmentMetadata',
    completions: [
      { value: 'toInclude', documentation: 'Columns to include in the result (defaults to "all")' },
      {
        value: 'merge',
        documentation: 'Merge all individual segment metadata results into a single result',
      },
      {
        value: 'analysisTypes',
        documentation: 'Column properties to calculate and return (e.g. cardinality, size)',
      },
      {
        value: 'aggregatorMergeStrategy',
        documentation: 'Strategy for merging aggregators: strict, lenient, earliest, or latest',
      },
      {
        value: 'lenientAggregatorMerge',
        documentation: 'Deprecated. Use aggregatorMergeStrategy instead',
      },
    ],
  },

  // SegmentMetadata toInclude properties
  {
    path: '$.toInclude',
    isObject: true,
    condition: obj => obj.queryType === 'segmentMetadata',
    completions: [
      { value: 'type', documentation: 'Type of columns to include: all, none, or list' },
    ],
  },

  // SegmentMetadata toInclude types
  {
    path: '$.toInclude.type',
    condition: obj => obj.queryType === 'segmentMetadata',
    completions: [
      { value: 'all', documentation: 'Include all columns in the result' },
      { value: 'none', documentation: 'Include no columns in the result' },
      { value: 'list', documentation: 'Include specific columns listed in the columns array' },
    ],
  },

  // SegmentMetadata toInclude list properties
  {
    path: '$.toInclude',
    isObject: true,
    condition: obj => obj.queryType === 'segmentMetadata' && obj.toInclude?.type === 'list',
    completions: [{ value: 'columns', documentation: 'Array of column names to include' }],
  },

  // SegmentMetadata analysisTypes values
  {
    path: '$.analysisTypes[]',
    condition: obj => obj.queryType === 'segmentMetadata',
    completions: [
      { value: 'cardinality', documentation: 'Number of unique values in string columns' },
      { value: 'interval', documentation: 'Time intervals associated with queried segments' },
      { value: 'minmax', documentation: 'Estimated min/max values for string columns' },
      { value: 'size', documentation: 'Estimated byte size as if stored in text format' },
      { value: 'timestampSpec', documentation: 'Timestamp specification of data in segments' },
      { value: 'queryGranularity', documentation: 'Query granularity of data in segments' },
      { value: 'aggregators', documentation: 'List of aggregators usable for metric columns' },
      { value: 'rollup', documentation: 'Whether the segments are rolled up' },
    ],
  },

  // SegmentMetadata aggregatorMergeStrategy values
  {
    path: '$.aggregatorMergeStrategy',
    condition: obj => obj.queryType === 'segmentMetadata',
    completions: [
      { value: 'strict', documentation: 'Fail if any conflicts or unknown aggregators exist' },
      { value: 'lenient', documentation: 'Ignore unknown aggregators, set conflicts to null' },
      { value: 'earliest', documentation: 'Use aggregator from earliest segment in conflicts' },
      { value: 'latest', documentation: 'Use aggregator from most recent segment in conflicts' },
    ],
  },

  // DataSourceMetadata query specific properties
  {
    path: '$',
    isObject: true,
    condition: obj => obj.queryType === 'dataSourceMetadata',
    completions: [
      { value: 'dataSource', documentation: 'The data source to get metadata for' },
      { value: 'context', documentation: 'Query context parameters' },
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

  // Granularity types
  {
    path: '$.granularity.type',
    completions: [
      { value: 'duration', documentation: 'Duration-based granularity' },
      { value: 'period', documentation: 'Period-based granularity' },
      { value: 'uniform', documentation: 'Uniform granularity' },
    ],
  },

  // Duration granularity properties
  {
    path: '$.granularity',
    isObject: true,
    condition: obj => obj.type === 'duration',
    completions: [
      { value: 'duration', documentation: 'Duration in milliseconds' },
      { value: 'origin', documentation: 'Origin timestamp' },
    ],
  },

  // Period granularity properties
  {
    path: '$.granularity',
    isObject: true,
    condition: obj => obj.type === 'period',
    completions: [
      { value: 'period', documentation: 'ISO 8601 period string' },
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
      { value: 'doubleMean', documentation: 'Mean aggregator for double values (query time only)' },
      { value: 'doubleFirst', documentation: 'First value aggregator for doubles' },
      { value: 'doubleLast', documentation: 'Last value aggregator for doubles' },
      { value: 'floatFirst', documentation: 'First value aggregator for floats' },
      { value: 'floatLast', documentation: 'Last value aggregator for floats' },
      { value: 'longFirst', documentation: 'First value aggregator for longs' },
      { value: 'longLast', documentation: 'Last value aggregator for longs' },
      { value: 'stringFirst', documentation: 'First value aggregator for strings' },
      { value: 'stringLast', documentation: 'Last value aggregator for strings' },
      { value: 'doubleAny', documentation: 'Any value aggregator for doubles' },
      { value: 'floatAny', documentation: 'Any value aggregator for floats' },
      { value: 'longAny', documentation: 'Any value aggregator for longs' },
      { value: 'stringAny', documentation: 'Any value aggregator for strings' },
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
      { value: 'filtered', documentation: 'Filtered aggregator' },
      { value: 'grouping', documentation: 'Grouping aggregator for subtotals' },
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
        'doubleMean',
      ].includes(obj.type),
    completions: [
      { value: 'fieldName', documentation: 'The field to aggregate' },
      { value: 'expression', documentation: 'Expression to evaluate instead of fieldName' },
    ],
  },

  // Aggregation properties for first/last aggregators
  {
    path: '$.aggregations[]',
    isObject: true,
    condition: obj =>
      obj.type &&
      [
        'doubleFirst',
        'doubleLast',
        'floatFirst',
        'floatLast',
        'longFirst',
        'longLast',
        'stringFirst',
        'stringLast',
        'doubleAny',
        'floatAny',
        'longAny',
        'stringAny',
      ].includes(obj.type),
    completions: [
      { value: 'fieldName', documentation: 'The field to aggregate' },
      {
        value: 'timeColumn',
        documentation: 'Time column to use for ordering (defaults to __time)',
      },
    ],
  },

  // Filtered aggregator properties
  {
    path: '$.aggregations[]',
    isObject: true,
    condition: obj => obj.type === 'filtered',
    completions: [
      { value: 'filter', documentation: 'Filter to apply before aggregation' },
      { value: 'aggregator', documentation: 'The aggregator to apply to filtered data' },
    ],
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
      { value: 'equals', documentation: 'Equality filter with type checking' },
      { value: 'null', documentation: 'Matches null or missing values' },
      { value: 'in', documentation: 'Matches any of the given dimension values' },
      { value: 'bound', documentation: 'Matches dimension values within bounds' },
      { value: 'interval', documentation: 'Matches time intervals' },
      {
        value: 'like',
        documentation: 'Matches dimension values using LIKE pattern',
      },
      { value: 'regex', documentation: 'Matches dimension values using regex' },
      { value: 'search', documentation: 'Matches dimension values using search' },
      { value: 'columnComparison', documentation: 'Compares two columns' },
      { value: 'expression', documentation: 'Expression-based filter' },
      { value: 'javascript', documentation: 'JavaScript function filter' },
      { value: 'spatial', documentation: 'Spatial rectangle filter' },
      { value: 'and', documentation: 'AND filter combinator' },
      { value: 'or', documentation: 'OR filter combinator' },
      { value: 'not', documentation: 'NOT filter combinator' },
      { value: 'true', documentation: 'Always matches' },
      { value: 'false', documentation: 'Never matches' },
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
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
    ],
  },

  // Equals filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'equals',
    completions: [
      { value: 'column', documentation: 'The column to filter on' },
      { value: 'matchValue', documentation: 'The value to match' },
      { value: 'matchValueType', documentation: 'Type of the match value' },
    ],
  },

  // Null filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'null',
    completions: [{ value: 'column', documentation: 'The column to check for null values' }],
  },

  // In filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'in',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'values', documentation: 'The values to match' },
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
    ],
  },

  // Bound filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'bound',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'lower', documentation: 'Lower bound value' },
      { value: 'upper', documentation: 'Upper bound value' },
      { value: 'lowerStrict', documentation: 'Whether lower bound is strict' },
      { value: 'upperStrict', documentation: 'Whether upper bound is strict' },
      { value: 'ordering', documentation: 'Ordering to use for comparison' },
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
    ],
  },

  // Like filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'like',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'pattern', documentation: 'LIKE pattern to match' },
      { value: 'escape', documentation: 'Escape character for pattern' },
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
    ],
  },

  // Regex filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'regex',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'pattern', documentation: 'Regular expression pattern' },
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
    ],
  },

  // Search filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'search',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'query', documentation: 'Search query specification' },
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
    ],
  },

  // Column comparison filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'columnComparison',
    completions: [{ value: 'dimensions', documentation: 'Array of dimension specs to compare' }],
  },

  // Expression filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'expression',
    completions: [{ value: 'expression', documentation: 'Druid expression that returns boolean' }],
  },

  // JavaScript filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'javascript',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on' },
      { value: 'function', documentation: 'JavaScript function for filtering' },
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
    ],
  },

  // Interval filter properties
  {
    path: '$.filter',
    isObject: true,
    condition: obj => obj.type === 'interval',
    completions: [
      { value: 'dimension', documentation: 'The dimension to filter on (usually __time)' },
      { value: 'intervals', documentation: 'Array of ISO-8601 intervals' },
      { value: 'extractionFn', documentation: 'Extraction function to apply' },
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

  // Nested filter fields for logical operators (AND/OR)
  {
    path: '$.filter.fields[]',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of this filter' }],
  },

  // Nested filter field for NOT operator
  {
    path: '$.filter.field',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of this filter' }],
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
      { value: 'skipEmptyBuckets', documentation: 'Skip empty time buckets in results' },
      { value: 'grandTotal', documentation: 'Include grand total row in timeseries results' },
      {
        value: 'sqlQueryId',
        documentation: 'SQL query ID for native query translation',
      },
      {
        value: 'sqlStringifyArrays',
        documentation: 'Stringify arrays in SQL results',
      },
      {
        value: 'vectorize',
        documentation: 'Enable vectorized query execution',
      },
      {
        value: 'vectorSize',
        documentation: 'Vector size for vectorized execution',
      },
      {
        value: 'maxSubqueryRows',
        documentation: 'Maximum rows in subquery results',
      },
      {
        value: 'enableParallelMerges',
        documentation: 'Enable parallel merge processing',
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

  // Post-aggregation properties
  {
    path: '$.postAggregations[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of post-aggregation' },
      { value: 'name', documentation: 'Output name for this post-aggregation' },
    ],
  },

  // Post-aggregation types
  {
    path: '$.postAggregations[].type',
    completions: [
      { value: 'arithmetic', documentation: 'Arithmetic operations on fields' },
      { value: 'fieldAccess', documentation: 'Access raw aggregation result' },
      { value: 'finalizingFieldAccess', documentation: 'Access finalized aggregation result' },
      { value: 'constant', documentation: 'Return constant value' },
      { value: 'expression', documentation: 'Druid expression post-aggregation' },
      { value: 'doubleGreatest', documentation: 'Maximum of multiple double fields' },
      { value: 'doubleLeast', documentation: 'Minimum of multiple double fields' },
      { value: 'longGreatest', documentation: 'Maximum of multiple long fields' },
      { value: 'longLeast', documentation: 'Minimum of multiple long fields' },
      { value: 'hyperUniqueCardinality', documentation: 'Cardinality from hyperUnique' },
    ],
  },

  // Arithmetic post-aggregation properties
  {
    path: '$.postAggregations[]',
    isObject: true,
    condition: obj => obj.type === 'arithmetic',
    completions: [
      { value: 'fn', documentation: 'Arithmetic function (+, -, *, /, pow, quotient)' },
      { value: 'fields', documentation: 'Array of post-aggregator inputs' },
      { value: 'ordering', documentation: 'Ordering for result values' },
    ],
  },

  // Field access post-aggregation properties
  {
    path: '$.postAggregations[]',
    isObject: true,
    condition: obj => obj.type === 'fieldAccess' || obj.type === 'finalizingFieldAccess',
    completions: [{ value: 'fieldName', documentation: 'Name of aggregator to reference' }],
  },

  // Constant post-aggregation properties
  {
    path: '$.postAggregations[]',
    isObject: true,
    condition: obj => obj.type === 'constant',
    completions: [{ value: 'value', documentation: 'Constant value to return' }],
  },

  // Expression post-aggregation properties
  {
    path: '$.postAggregations[]',
    isObject: true,
    condition: obj => obj.type === 'expression',
    completions: [
      { value: 'expression', documentation: 'Druid expression to evaluate' },
      { value: 'ordering', documentation: 'Ordering for result values' },
      { value: 'outputType', documentation: 'Expected output type' },
    ],
  },

  // Greatest/Least post-aggregation properties
  {
    path: '$.postAggregations[]',
    isObject: true,
    condition: obj =>
      ['doubleGreatest', 'doubleLeast', 'longGreatest', 'longLeast'].includes(obj.type),
    completions: [{ value: 'fields', documentation: 'Array of post-aggregator inputs' }],
  },

  // Having clause properties
  {
    path: '$.having',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of having clause' }],
  },

  // Having clause types
  {
    path: '$.having.type',
    completions: [
      { value: 'greaterThan', documentation: 'Greater than comparison' },
      { value: 'lessThan', documentation: 'Less than comparison' },
      { value: 'equalTo', documentation: 'Equal to comparison' },
      { value: 'and', documentation: 'AND having combinator' },
      { value: 'or', documentation: 'OR having combinator' },
      { value: 'not', documentation: 'NOT having combinator' },
      { value: 'dimSelector', documentation: 'Dimension value having' },
      { value: 'filter', documentation: 'Filter-based having' },
    ],
  },

  // Comparison having properties
  {
    path: '$.having',
    isObject: true,
    condition: obj => ['greaterThan', 'lessThan', 'equalTo'].includes(obj.type),
    completions: [
      { value: 'aggregation', documentation: 'Name of aggregation to compare' },
      { value: 'value', documentation: 'Value to compare against' },
    ],
  },

  // Logical having properties
  {
    path: '$.having',
    isObject: true,
    condition: obj => obj.type === 'and' || obj.type === 'or',
    completions: [{ value: 'havingSpecs', documentation: 'Array of having clauses to combine' }],
  },

  // NOT having properties
  {
    path: '$.having',
    isObject: true,
    condition: obj => obj.type === 'not',
    completions: [{ value: 'havingSpec', documentation: 'Having clause to negate' }],
  },

  // LimitSpec properties
  {
    path: '$.limitSpec',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of limit specification' },
      { value: 'limit', documentation: 'Maximum number of results' },
      { value: 'offset', documentation: 'Number of results to skip' },
      { value: 'columns', documentation: 'Column ordering specifications' },
    ],
  },

  // LimitSpec types
  {
    path: '$.limitSpec.type',
    completions: [{ value: 'default', documentation: 'Default limit specification' }],
  },

  // Virtual columns properties
  {
    path: '$.virtualColumns[]',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of virtual column' },
      { value: 'name', documentation: 'Name of the virtual column' },
    ],
  },

  // Virtual column types
  {
    path: '$.virtualColumns[].type',
    completions: [
      {
        value: 'expression',
        documentation: 'Expression-based virtual column using Druid expressions',
      },
      { value: 'nested-field', documentation: 'Optimized access to nested JSON fields' },
      { value: 'mv-filtered', documentation: 'Filtered multi-value string column' },
    ],
  },

  // Expression virtual column properties
  {
    path: '$.virtualColumns[]',
    isObject: true,
    condition: obj => obj.type === 'expression',
    completions: [
      { value: 'expression', documentation: 'Druid expression that takes a row as input' },
      {
        value: 'outputType',
        documentation: 'Output type: LONG, FLOAT, DOUBLE, STRING, ARRAY, or COMPLEX',
      },
    ],
  },

  // Expression virtual column output types
  {
    path: '$.virtualColumns[].outputType',
    condition: obj => obj.type === 'expression',
    completions: [
      { value: 'LONG', documentation: 'Long integer output type' },
      { value: 'FLOAT', documentation: 'Float output type (default)' },
      { value: 'DOUBLE', documentation: 'Double precision output type' },
      { value: 'STRING', documentation: 'String output type' },
      { value: 'ARRAY<STRING>', documentation: 'Array of strings output type' },
      { value: 'ARRAY<LONG>', documentation: 'Array of longs output type' },
      { value: 'ARRAY<DOUBLE>', documentation: 'Array of doubles output type' },
      { value: 'COMPLEX<json>', documentation: 'Complex JSON output type' },
    ],
  },

  // Nested field virtual column properties
  {
    path: '$.virtualColumns[]',
    isObject: true,
    condition: obj => obj.type === 'nested-field',
    completions: [
      { value: 'columnName', documentation: 'Name of the COMPLEX<json> input column' },
      { value: 'outputName', documentation: 'Name of the virtual column output' },
      { value: 'expectedType', documentation: 'Expected output type for coercion' },
      { value: 'path', documentation: 'JSONPath or jq syntax path to nested value' },
      { value: 'pathParts', documentation: 'Parsed path parts array for nested access' },
      {
        value: 'processFromRaw',
        documentation: 'Process raw JSON data instead of optimized selector',
      },
      { value: 'useJqSyntax', documentation: 'Use jq syntax instead of JSONPath for path parsing' },
    ],
  },

  // Nested field virtual column expected types
  {
    path: '$.virtualColumns[].expectedType',
    condition: obj => obj.type === 'nested-field',
    completions: [
      { value: 'STRING', documentation: 'String output type (default)' },
      { value: 'LONG', documentation: 'Long integer output type' },
      { value: 'FLOAT', documentation: 'Float output type' },
      { value: 'DOUBLE', documentation: 'Double precision output type' },
      { value: 'COMPLEX<json>', documentation: 'Complex JSON output type' },
    ],
  },

  // Nested field path parts properties
  {
    path: '$.virtualColumns[].pathParts[]',
    isObject: true,
    condition: obj => obj.type === 'nested-field',
    completions: [{ value: 'type', documentation: 'Type of path part: field or arrayElement' }],
  },

  // Nested field path part types
  {
    path: '$.virtualColumns[].pathParts[].type',
    completions: [
      { value: 'field', documentation: 'Access a specific field in nested structure' },
      { value: 'arrayElement', documentation: 'Access specific array element by index' },
    ],
  },

  // Field path part properties
  {
    path: '$.virtualColumns[].pathParts[]',
    isObject: true,
    condition: obj => obj.type === 'field',
    completions: [{ value: 'field', documentation: 'Name of the field to access' }],
  },

  // Array element path part properties
  {
    path: '$.virtualColumns[].pathParts[]',
    isObject: true,
    condition: obj => obj.type === 'arrayElement',
    completions: [{ value: 'index', documentation: 'Zero-based array index to access' }],
  },

  // Multi-value filtered virtual column properties
  {
    path: '$.virtualColumns[]',
    isObject: true,
    condition: obj => obj.type === 'mv-filtered',
    completions: [
      { value: 'delegate', documentation: 'Name of the multi-value STRING input column' },
      { value: 'values', documentation: 'Array of STRING values to allow or deny' },
      {
        value: 'isAllowList',
        documentation: 'If true, allow only specified values; if false, deny them',
      },
    ],
  },

  // DataSource specifications
  {
    path: '$.dataSource',
    isObject: true,
    condition: obj => typeof obj === 'object' && obj !== null,
    completions: [{ value: 'type', documentation: 'Type of data source' }],
  },

  // DataSource types
  {
    path: '$.dataSource.type',
    completions: [
      { value: 'table', documentation: 'Simple table data source' },
      { value: 'lookup', documentation: 'Lookup data source' },
      { value: 'union', documentation: 'Union of multiple data sources' },
      { value: 'inline', documentation: 'Inline data source' },
      { value: 'query', documentation: 'Query-based data source' },
      { value: 'join', documentation: 'Join data source' },
    ],
  },

  // Intervals object properties (when intervals is an object)
  {
    path: '$.intervals',
    isObject: true,
    condition: obj => typeof obj === 'object' && obj !== null && !Array.isArray(obj),
    completions: [
      { value: 'type', documentation: 'Type of interval specification' },
      { value: 'intervals', documentation: 'Array of ISO-8601 interval strings' },
    ],
  },

  // Intervals object types
  {
    path: '$.intervals.type',
    completions: [
      { value: 'intervals', documentation: 'Standard intervals specification' },
      { value: 'segments', documentation: 'Segment-based intervals' },
    ],
  },

  // Segments intervals properties
  {
    path: '$.intervals',
    isObject: true,
    condition: obj => obj.type === 'segments',
    completions: [{ value: 'segments', documentation: 'Array of segment specifications' }],
  },
];
