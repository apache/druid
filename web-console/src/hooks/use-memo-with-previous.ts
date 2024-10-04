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

import { useRef } from 'react';

import { arraysEqualByElement } from '../utils';

/**
 * Custom hook similar to `useMemo`, but it provides the previous value as an argument.
 * @param computeFn - Function to compute the new value. It receives the previous value as an argument.
 * @param deps - Dependency array to determine when to recompute the value.
 * @returns The memoized value.
 */
export function useMemoWithPrevious<T>(computeFn: (prev: T | undefined) => T, deps: any[]): T {
  const value = useRef(computeFn(undefined));
  const prevDependencies = useRef(deps);
  if (!arraysEqualByElement(deps, prevDependencies.current)) {
    value.current = computeFn(value.current);
    prevDependencies.current = deps;
  }
  return value.current;
}
