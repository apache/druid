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

import { deepDelete, deepSet } from '../../utils';

export interface QueryContext {
  useCache?: boolean;
  populateCache?: boolean;
  useApproximateCountDistinct?: boolean;
  useApproximateTopN?: boolean;
  sqlTimeZone?: string;

  // Multi-stage query
  maxNumTasks?: number;
  finalizeAggregations?: boolean;
  selectDestination?: string;
  durableShuffleStorage?: boolean;
  maxParseExceptions?: number;
  groupByEnableMultiValueUnnesting?: boolean;
  arrayIngestMode?: 'array' | 'mvd';

  [key: string]: any;
}

export interface QueryWithContext {
  queryString: string;
  queryContext?: QueryContext;
  wrapQueryLimit?: number;
}

export function isEmptyContext(context: QueryContext | undefined): boolean {
  return !context || Object.keys(context).length === 0;
}

// -----------------------------

export function getUseCache(context: QueryContext): boolean {
  const { useCache } = context;
  return typeof useCache === 'boolean' ? useCache : true;
}

export function changeUseCache(context: QueryContext, useCache: boolean): QueryContext {
  let newContext = context;
  if (useCache) {
    newContext = deepDelete(newContext, 'useCache');
    newContext = deepDelete(newContext, 'populateCache');
  } else {
    newContext = deepSet(newContext, 'useCache', false);
    newContext = deepSet(newContext, 'populateCache', false);
  }
  return newContext;
}

// -----------------------------

export function getUseApproximateCountDistinct(context: QueryContext): boolean {
  const { useApproximateCountDistinct } = context;
  return typeof useApproximateCountDistinct === 'boolean' ? useApproximateCountDistinct : true;
}

export function changeUseApproximateCountDistinct(
  context: QueryContext,
  useApproximateCountDistinct: boolean,
): QueryContext {
  if (useApproximateCountDistinct) {
    return deepDelete(context, 'useApproximateCountDistinct');
  } else {
    return deepSet(context, 'useApproximateCountDistinct', false);
  }
}

// -----------------------------

export function getUseApproximateTopN(context: QueryContext): boolean {
  const { useApproximateTopN } = context;
  return typeof useApproximateTopN === 'boolean' ? useApproximateTopN : true;
}

export function changeUseApproximateTopN(
  context: QueryContext,
  useApproximateTopN: boolean,
): QueryContext {
  if (useApproximateTopN) {
    return deepDelete(context, 'useApproximateTopN');
  } else {
    return deepSet(context, 'useApproximateTopN', false);
  }
}

// sqlTimeZone

export function getTimezone(context: QueryContext): string | undefined {
  return context.sqlTimeZone;
}

export function changeTimezone(context: QueryContext, timezone: string | undefined): QueryContext {
  if (timezone) {
    return deepSet(context, 'sqlTimeZone', timezone);
  } else {
    return deepDelete(context, 'sqlTimeZone');
  }
}

// maxNumTasks

export function getMaxNumTasks(context: QueryContext): number | undefined {
  return context.maxNumTasks;
}

export function changeMaxNumTasks(
  context: QueryContext,
  maxNumTasks: number | undefined,
): QueryContext {
  return typeof maxNumTasks === 'number'
    ? deepSet(context, 'maxNumTasks', maxNumTasks)
    : deepDelete(context, 'maxNumTasks');
}

// taskAssignment

export function getTaskAssigment(context: QueryContext): string {
  const { taskAssignment } = context;
  return taskAssignment ?? 'max';
}

export function changeTaskAssigment(
  context: QueryContext,
  taskAssignment: string | undefined,
): QueryContext {
  return typeof taskAssignment === 'string'
    ? deepSet(context, 'taskAssignment', taskAssignment)
    : deepDelete(context, 'taskAssignment');
}

// failOnEmptyInsert

export function getFailOnEmptyInsert(context: QueryContext): boolean | undefined {
  const { failOnEmptyInsert } = context;
  return typeof failOnEmptyInsert === 'boolean' ? failOnEmptyInsert : undefined;
}

export function changeFailOnEmptyInsert(
  context: QueryContext,
  failOnEmptyInsert: boolean | undefined,
): QueryContext {
  return typeof failOnEmptyInsert === 'boolean'
    ? deepSet(context, 'failOnEmptyInsert', failOnEmptyInsert)
    : deepDelete(context, 'failOnEmptyInsert');
}

// finalizeAggregations

export function getFinalizeAggregations(context: QueryContext): boolean | undefined {
  const { finalizeAggregations } = context;
  return typeof finalizeAggregations === 'boolean' ? finalizeAggregations : undefined;
}

export function changeFinalizeAggregations(
  context: QueryContext,
  finalizeAggregations: boolean | undefined,
): QueryContext {
  return typeof finalizeAggregations === 'boolean'
    ? deepSet(context, 'finalizeAggregations', finalizeAggregations)
    : deepDelete(context, 'finalizeAggregations');
}

// waitUntilSegmentsLoad

export function getWaitUntilSegmentsLoad(context: QueryContext): boolean | undefined {
  const { waitUntilSegmentsLoad } = context;
  return typeof waitUntilSegmentsLoad === 'boolean' ? waitUntilSegmentsLoad : undefined;
}

export function changeWaitUntilSegmentsLoad(
  context: QueryContext,
  waitUntilSegmentsLoad: boolean | undefined,
): QueryContext {
  return typeof waitUntilSegmentsLoad === 'boolean'
    ? deepSet(context, 'waitUntilSegmentsLoad', waitUntilSegmentsLoad)
    : deepDelete(context, 'waitUntilSegmentsLoad');
}

// groupByEnableMultiValueUnnesting

export function getGroupByEnableMultiValueUnnesting(context: QueryContext): boolean | undefined {
  const { groupByEnableMultiValueUnnesting } = context;
  return typeof groupByEnableMultiValueUnnesting === 'boolean'
    ? groupByEnableMultiValueUnnesting
    : undefined;
}

export function changeGroupByEnableMultiValueUnnesting(
  context: QueryContext,
  groupByEnableMultiValueUnnesting: boolean | undefined,
): QueryContext {
  return typeof groupByEnableMultiValueUnnesting === 'boolean'
    ? deepSet(context, 'groupByEnableMultiValueUnnesting', groupByEnableMultiValueUnnesting)
    : deepDelete(context, 'groupByEnableMultiValueUnnesting');
}

// durableShuffleStorage

export function getDurableShuffleStorage(context: QueryContext): boolean {
  const { durableShuffleStorage } = context;
  return Boolean(durableShuffleStorage);
}

export function changeDurableShuffleStorage(
  context: QueryContext,
  durableShuffleStorage: boolean,
): QueryContext {
  if (durableShuffleStorage) {
    return deepSet(context, 'durableShuffleStorage', true);
  } else {
    return deepDelete(context, 'durableShuffleStorage');
  }
}

// maxParseExceptions

export function getMaxParseExceptions(context: QueryContext): number {
  const { maxParseExceptions } = context;
  return Number(maxParseExceptions) || 0;
}

export function changeMaxParseExceptions(
  context: QueryContext,
  maxParseExceptions: number,
): QueryContext {
  if (maxParseExceptions !== 0) {
    return deepSet(context, 'maxParseExceptions', maxParseExceptions);
  } else {
    return deepDelete(context, 'maxParseExceptions');
  }
}
