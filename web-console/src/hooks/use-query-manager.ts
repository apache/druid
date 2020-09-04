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

import { CancelToken } from 'axios';
import { useEffect, useState } from 'react';

import { QueryManager, QueryState } from '../utils';

export interface UseQueryManagerOptions<Q, R> {
  processQuery: (
    query: Q,
    cancelToken: CancelToken,
    setIntermediateQuery: (intermediateQuery: any) => void,
  ) => Promise<R>;
  debounceIdle?: number;
  debounceLoading?: number;
  query?: Q;
  initQuery?: Q;
}

export function useQueryManager<Q, R>(
  options: UseQueryManagerOptions<Q, R>,
): [QueryState<R>, QueryManager<Q, R>] {
  const { processQuery, debounceIdle, debounceLoading, query, initQuery } = options;

  const [resultState, setResultState] = useState(QueryState.INIT);

  const [queryManager] = useState(() => {
    return new QueryManager({
      processQuery,
      debounceIdle,
      debounceLoading,
      onStateChange: setResultState,
    });
  });

  useEffect(() => {
    if (typeof initQuery !== 'undefined') {
      queryManager.runQuery(initQuery);
    }
    return () => {
      queryManager.terminate();
    };
  }, []);

  if (query) {
    useEffect(() => {
      queryManager.runQuery(query);
    }, [query]);
  }

  return [resultState, queryManager];
}
