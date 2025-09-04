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

import type { AxiosResponse } from 'axios';
import { QueryResult } from 'druid-query-toolkit';

import type { AsyncStatusResponse, MsqTaskPayloadResponse, QueryContext } from '../../druid-models';
import { Execution } from '../../druid-models';
import { Api } from '../../singletons';
import { deepGet, DruidError, IntermediateQueryState, QueryManager } from '../../utils';

// some executionMode has to be set on the /druid/v2/sql/statements API
function ensureExecutionModeIsSet(context: QueryContext | undefined): QueryContext {
  if (typeof context?.executionMode === 'string') return context;
  return {
    ...context,
    executionMode: 'async',
  };
}

export interface SubmitTaskQueryOptions {
  query: string | Record<string, any>;
  context?: QueryContext;
  baseQueryContext?: QueryContext;
  prefixLines?: number;
  signal?: AbortSignal;
  preserveOnTermination?: boolean;
  onSubmitted?: (id: string) => void;
}

export async function submitTaskQuery(
  options: SubmitTaskQueryOptions,
): Promise<Execution | IntermediateQueryState<Execution>> {
  const {
    query,
    context,
    baseQueryContext,
    prefixLines,
    signal,
    preserveOnTermination,
    onSubmitted,
  } = options;

  let sqlQuery: string;
  let jsonQuery: Record<string, any>;
  if (typeof query === 'string') {
    sqlQuery = query;
    jsonQuery = {
      query: sqlQuery,
      context: ensureExecutionModeIsSet({ ...baseQueryContext, ...context }),
      resultFormat: 'array',
      header: true,
      typesHeader: true,
      sqlTypesHeader: true,
    };
  } else {
    sqlQuery = query.query;

    jsonQuery = {
      ...query,
      context: ensureExecutionModeIsSet({
        ...baseQueryContext,
        ...query.context,
        ...context,
      }),
    };
  }

  let sqlAsyncResp: AxiosResponse<AsyncStatusResponse>;
  try {
    sqlAsyncResp = await Api.instance.post<AsyncStatusResponse>(
      `/druid/v2/sql/statements`,
      jsonQuery,
      {
        signal,
      },
    );
  } catch (e) {
    const druidError = deepGet(e, 'response.data');
    if (!druidError) throw e;
    throw new DruidError(druidError, prefixLines);
  }

  const sqlAsyncStatus = sqlAsyncResp.data;

  if (!sqlAsyncStatus.queryId) {
    if (!Array.isArray(sqlAsyncStatus)) throw new Error('unexpected task payload');
    return Execution.fromResult(
      'sql-msq-task',
      QueryResult.fromRawResult(sqlAsyncStatus, false, true, true, true),
    );
  }

  const execution = Execution.fromAsyncStatus(sqlAsyncStatus, sqlQuery, jsonQuery.context);

  if (onSubmitted) {
    onSubmitted(execution.id);
  }

  if (!execution.isWaitingForQuery()) return execution;

  if (signal) {
    cancelTaskExecutionOnCancel(execution.id, signal, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export interface ReattachTaskQueryOptions {
  id: string;
  signal?: AbortSignal;
  preserveOnTermination?: boolean;
}

export async function reattachTaskExecution(
  option: ReattachTaskQueryOptions,
): Promise<Execution | IntermediateQueryState<Execution>> {
  const { id, signal, preserveOnTermination } = option;
  let execution: Execution;

  try {
    execution = await getTaskExecution(id, undefined, signal);
  } catch (e) {
    throw new Error(`Reattaching to query failed due to: ${e.message}`);
  }

  if (!execution.isWaitingForQuery()) return execution;

  if (signal) {
    cancelTaskExecutionOnCancel(execution.id, signal, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export async function updateExecutionWithTaskIfNeeded(
  execution: Execution,
  signal?: AbortSignal,
): Promise<Execution> {
  if (!execution.isWaitingForQuery()) return execution;

  // Inherit old payload so as not to re-query it
  return await getTaskExecution(execution.id, execution._payload, signal);
}

export async function getTaskExecution(
  id: string,
  taskPayloadOverride?: MsqTaskPayloadResponse,
  signal?: AbortSignal,
): Promise<Execution> {
  const encodedId = Api.encodePath(id);

  let execution: Execution | undefined;

  if (Execution.USE_TASK_REPORTS) {
    let taskReport: any;
    try {
      taskReport = (
        await Api.instance.get(`/druid/indexer/v1/task/${encodedId}/reports`, {
          signal,
        })
      ).data;
    } catch (e) {
      if (Api.isNetworkError(e)) throw e;
    }
    if (taskReport) {
      try {
        execution = Execution.fromTaskReport(taskReport);
      } catch {
        // We got a bad payload, wait a bit and try to get the payload again (also log it)
        // This whole catch block is a hack, and we should make the detail route more robust
        console.error(
          `Got unusable response from the reports endpoint (/druid/indexer/v1/task/${encodedId}/reports) going to retry`,
        );
        console.log('Report response:', taskReport);
      }
    }
  }

  if (!execution) {
    const statusResp = await Api.instance.get<AsyncStatusResponse>(
      `/druid/v2/sql/statements/${encodedId}?detail=true`,
      {
        signal,
      },
    );

    execution = Execution.fromAsyncStatus(statusResp.data);
  }

  let taskPayload = taskPayloadOverride;
  if (Execution.USE_TASK_PAYLOAD && !taskPayload) {
    try {
      taskPayload = (
        await Api.instance.get(`/druid/indexer/v1/task/${encodedId}`, {
          signal,
        })
      ).data;
    } catch (e) {
      if (Api.isNetworkError(e)) throw e;
    }
  }
  if (taskPayload) {
    execution = execution.updateWithTaskPayload(taskPayload);
  }

  // Still have to pull the destination page info from the async status, do this in a best effort way since the statements API may have permission errors
  if (execution.status === 'SUCCESS' && !execution.destinationPages) {
    try {
      const statusResp = await Api.instance.get<AsyncStatusResponse>(
        `/druid/v2/sql/statements/${encodedId}`,
        {
          signal,
        },
      );

      execution = execution.updateWithAsyncStatus(statusResp.data);
    } catch (e) {
      if (Api.isNetworkError(e)) throw e;
    }
  }

  if (Execution.getClusterCapacity && execution.hasPotentiallyStuckStage()) {
    const capacityInfo = await Execution.getClusterCapacity();
    if (capacityInfo) {
      execution = execution.changeCapacityInfo(capacityInfo);
    }
  }

  return execution;
}

function cancelTaskExecutionOnCancel(
  id: string,
  signal: AbortSignal,
  preserveOnTermination = false,
): void {
  signal.addEventListener('abort', () => {
    if (preserveOnTermination && signal.reason === QueryManager.TERMINATION_MESSAGE) return;
    cancelTaskExecution(id).catch(() => {});
  });
}

export function cancelTaskExecution(id: string): Promise<void> {
  return Api.instance.post(`/druid/indexer/v1/task/${Api.encodePath(id)}/shutdown`, {});
}
