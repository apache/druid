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

export const QUERY_CONTEXT_COMPLETIONS: JsonCompletionRule[] = [
  // Root level query context properties
  {
    path: '$',
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
        value: 'realtimeSegmentsOnly',
        documentation: 'Whether to query only realtime segments',
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
      // Multi-stage query specific
      { value: 'maxNumTasks', documentation: 'Maximum number of tasks for MSQ' },
      { value: 'finalizeAggregations', documentation: 'Whether to finalize aggregations in MSQ' },
      { value: 'selectDestination', documentation: 'Where to write SELECT query results' },
      { value: 'durableShuffleStorage', documentation: 'Use durable storage for shuffle' },
      { value: 'maxParseExceptions', documentation: 'Maximum parse exceptions allowed' },
      {
        value: 'groupByEnableMultiValueUnnesting',
        documentation: 'Enable multi-value unnesting in GROUP BY',
      },
      { value: 'arrayIngestMode', documentation: 'How to handle arrays during ingestion' },
      { value: 'taskAssignment', documentation: 'Task assignment strategy' },
      { value: 'sqlJoinAlgorithm', documentation: 'Algorithm to use for SQL joins' },
      { value: 'failOnEmptyInsert', documentation: 'Fail if INSERT would create no segments' },
      {
        value: 'waitUntilSegmentsLoad',
        documentation: 'Wait for segments to load before completing',
      },
      { value: 'useConcurrentLocks', documentation: 'Use concurrent locks for better performance' },
      { value: 'forceSegmentSortByTime', documentation: 'Force segments to be sorted by time' },
      { value: 'includeAllCounters', documentation: 'Include all counters in task reports' },
      // SQL specific
      { value: 'sqlTimeZone', documentation: 'Time zone for SQL queries' },
      { value: 'useApproximateCountDistinct', documentation: 'Use approximate COUNT DISTINCT' },
      { value: 'useApproximateTopN', documentation: 'Use approximate TOP N queries' },
    ],
  },
  // Specific value completions
  {
    path: '$.selectDestination',
    completions: [
      { value: 'taskReport', documentation: 'Write results to task report' },
      { value: 'durableStorage', documentation: 'Write results to durable storage' },
    ],
  },
  {
    path: '$.taskAssignment',
    completions: [
      { value: 'auto', documentation: 'Automatically assign tasks' },
      { value: 'max', documentation: 'Use maximum available tasks' },
    ],
  },
  {
    path: '$.sqlJoinAlgorithm',
    completions: [
      { value: 'broadcast', documentation: 'Broadcast join algorithm' },
      { value: 'sortMerge', documentation: 'Sort-merge join algorithm' },
    ],
  },
  {
    path: '$.arrayIngestMode',
    completions: [
      { value: 'array', documentation: 'Ingest as native arrays' },
      { value: 'mvd', documentation: 'Ingest as multi-value dimensions' },
    ],
  },
  {
    path: '$.vectorize',
    completions: [
      { value: 'false', documentation: 'Disable vectorized execution' },
      { value: 'force', documentation: 'Force vectorized execution' },
    ],
  },
];
