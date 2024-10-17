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

export type SelectDestination = 'taskReport' | 'durableStorage';
export type ArrayIngestMode = 'array' | 'mvd';
export type TaskAssignment = 'auto' | 'max';
export type SqlJoinAlgorithm = 'broadcast' | 'sortMerge';

export interface QueryContext {
  useCache?: boolean;
  populateCache?: boolean;
  useApproximateCountDistinct?: boolean;
  useApproximateTopN?: boolean;
  sqlTimeZone?: string;

  // Multi-stage query
  maxNumTasks?: number;
  finalizeAggregations?: boolean;
  selectDestination?: SelectDestination;
  durableShuffleStorage?: boolean;
  maxParseExceptions?: number;
  groupByEnableMultiValueUnnesting?: boolean;
  arrayIngestMode?: ArrayIngestMode;
  taskAssignment?: TaskAssignment;
  sqlJoinAlgorithm?: SqlJoinAlgorithm;
  failOnEmptyInsert?: boolean;
  waitUntilSegmentsLoad?: boolean;
  useConcurrentLocks?: boolean;
  forceSegmentSortByTime?: boolean;
  includeAllCounters?: boolean;

  [key: string]: any;
}

export const DEFAULT_SERVER_QUERY_CONTEXT: QueryContext = {
  useCache: true,
  populateCache: true,
  useApproximateCountDistinct: true,
  useApproximateTopN: true,
  sqlTimeZone: 'Etc/UTC',

  // Multi-stage query
  finalizeAggregations: true,
  selectDestination: 'taskReport',
  durableShuffleStorage: false,
  maxParseExceptions: 0,
  groupByEnableMultiValueUnnesting: true,
  taskAssignment: 'max',
  sqlJoinAlgorithm: 'broadcast',
  failOnEmptyInsert: false,
  waitUntilSegmentsLoad: false,
  useConcurrentLocks: false,
  forceSegmentSortByTime: true,
  includeAllCounters: false,
};

export interface QueryWithContext {
  queryString: string;
  queryContext?: QueryContext;
  wrapQueryLimit?: number;
}

export function isEmptyContext(context: QueryContext | undefined): boolean {
  return !context || Object.keys(context).length === 0;
}

export function getQueryContextKey(
  key: keyof QueryContext,
  context: QueryContext,
  defaultContext: QueryContext,
): any {
  return typeof context[key] !== 'undefined' ? context[key] : defaultContext[key];
}
