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

export const BROKER_DYNAMIC_CONFIG_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      {
        value: 'queryBlocklist',
        documentation:
          'List of rules to block queries on brokers. Each rule can match by datasource, query type, and/or context parameters.',
      },
    ],
  },
  // Query blocklist rule properties
  {
    path: '$.queryBlocklist.[*]',
    isObject: true,
    completions: [
      {
        value: 'ruleName',
        documentation: 'Unique name for this blocklist rule (required)',
      },
      {
        value: 'dataSources',
        documentation: 'List of datasources to block (optional)',
      },
      {
        value: 'queryTypes',
        documentation: 'List of query types to block (optional)',
      },
      {
        value: 'contextMatches',
        documentation: 'Map of query context parameters to match (optional)',
      },
    ],
  },
  // Query type suggestions
  {
    path: '$.queryBlocklist.[*].queryTypes.[]',
    completions: [
      { value: 'scan', documentation: 'Scan queries' },
      { value: 'timeseries', documentation: 'Timeseries queries' },
      { value: 'groupBy', documentation: 'GroupBy queries' },
      { value: 'topN', documentation: 'TopN queries' },
      { value: 'search', documentation: 'Search queries' },
      { value: 'timeBoundary', documentation: 'TimeBoundary queries' },
      { value: 'segmentMetadata', documentation: 'SegmentMetadata queries' },
      { value: 'dataSourceMetadata', documentation: 'DataSourceMetadata queries' },
    ],
  },
  // Datasource array
  {
    path: '$.queryBlocklist.[*].dataSources.[]',
    completions: [{ value: 'example_datasource', documentation: 'Datasource name to block' }],
  },
  // Context matches example
  {
    path: '$.queryBlocklist.[*].contextMatches',
    isObject: true,
    completions: [
      { value: 'debug', documentation: 'Example: match debug context parameter' },
      { value: 'priority', documentation: 'Example: match priority context parameter' },
      { value: 'queryId', documentation: 'Example: match specific query ID' },
    ],
  },
];
