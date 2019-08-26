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

import { deepDelete, deepSet } from './object-change';

export interface QueryContext {
  useCache?: boolean | undefined;
  populateCache?: boolean | undefined;
  useApproximateCountDistinct?: boolean | undefined;
  useApproximateTopN?: boolean | undefined;
  [key: string]: any;
}

export function isEmptyContext(context: QueryContext): boolean {
  return Object.keys(context).length === 0;
}

// -----------------------------

export function getUseCache(context: QueryContext): boolean {
  const { useCache } = context;
  return typeof useCache === 'boolean' ? useCache : true;
}

export function setUseCache(context: QueryContext, useCache: boolean): QueryContext {
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

export function setUseApproximateCountDistinct(
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

export function setUseApproximateTopN(
  context: QueryContext,
  useApproximateTopN: boolean,
): QueryContext {
  if (useApproximateTopN) {
    return deepDelete(context, 'useApproximateTopN');
  } else {
    return deepSet(context, 'useApproximateTopN', false);
  }
}
