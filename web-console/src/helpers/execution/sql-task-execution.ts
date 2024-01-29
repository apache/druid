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

import { QueryResult } from '@druid-toolkit/query';
import type { AxiosResponse, CancelToken } from 'axios';

import type { AsyncStatusResponse, MsqTaskPayloadResponse, QueryContext } from '../../druid-models';
import { Execution } from '../../druid-models';
import { Api } from '../../singletons';
import { deepGet, DruidError, IntermediateQueryState, QueryManager } from '../../utils';
import { maybeGetClusterCapacity } from '../capacity';

const USE_TASK_PAYLOAD = true;
const USE_TASK_REPORTS = true;

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
  prefixLines?: number;
  cancelToken?: CancelToken;
  preserveOnTermination?: boolean;
  onSubmitted?: (id: string) => void;
}

export async function submitTaskQuery(
  options: SubmitTaskQueryOptions,
): Promise<Execution | IntermediateQueryState<Execution>> {
  const { query, context, prefixLines, cancelToken, preserveOnTermination, onSubmitted } = options;

  let sqlQuery: string;
  let jsonQuery: Record<string, any>;
  if (typeof query === 'string') {
    sqlQuery = query;
    jsonQuery = {
      query: sqlQuery,
      context: ensureExecutionModeIsSet(context),
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
        cancelToken,
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

  const execution = Execution.fromAsyncStatus(sqlAsyncStatus, sqlQuery, context);

  if (onSubmitted) {
    onSubmitted(execution.id);
  }

  if (!execution.isWaitingForQuery()) return execution;

  if (cancelToken) {
    cancelTaskExecutionOnCancel(execution.id, cancelToken, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export interface ReattachTaskQueryOptions {
  id: string;
  cancelToken?: CancelToken;
  preserveOnTermination?: boolean;
}

export async function reattachTaskExecution(
  option: ReattachTaskQueryOptions,
): Promise<Execution | IntermediateQueryState<Execution>> {
  const { id, cancelToken, preserveOnTermination } = option;
  let execution: Execution;

  try {
    execution = await getTaskExecution(id, undefined, cancelToken);
  } catch (e) {
    throw new Error(`Reattaching to query failed due to: ${e.message}`);
  }

  if (!execution.isWaitingForQuery()) return execution;

  if (cancelToken) {
    cancelTaskExecutionOnCancel(execution.id, cancelToken, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export async function updateExecutionWithTaskIfNeeded(
  execution: Execution,
  cancelToken?: CancelToken,
): Promise<Execution> {
  if (!execution.isWaitingForQuery()) return execution;

  // Inherit old payload so as not to re-query it
  return await getTaskExecution(execution.id, execution._payload, cancelToken);
}

export async function getTaskExecution(
  id: string,
  taskPayloadOverride?: MsqTaskPayloadResponse,
  cancelToken?: CancelToken,
): Promise<Execution> {
  const encodedId = Api.encodePath(id);

  let execution: Execution | undefined;

  if (USE_TASK_REPORTS) {
    let taskReport: any;
    try {
      taskReport = (
        await Api.instance.get(`/druid/indexer/v1/task/${encodedId}/reports`, {
          cancelToken,
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
      `/druid/v2/sql/statements/${encodedId}`,
      {
        cancelToken,
      },
    );

    execution = Execution.fromAsyncStatus(statusResp.data);
  }

  let taskPayload = taskPayloadOverride;
  if (USE_TASK_PAYLOAD && !taskPayload) {
    try {
      taskPayload = (
        await Api.instance.get(`/druid/indexer/v1/task/${encodedId}`, {
          cancelToken,
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
          cancelToken,
        },
      );

      execution = execution.updateWithAsyncStatus(statusResp.data);
    } catch (e) {
      if (Api.isNetworkError(e)) throw e;
    }
  }

  if (execution.hasPotentiallyStuckStage()) {
    const capacityInfo = await maybeGetClusterCapacity();
    if (capacityInfo) {
      execution = execution.changeCapacityInfo(capacityInfo);
    }
  }

  return execution;
}

function cancelTaskExecutionOnCancel(
  id: string,
  cancelToken: CancelToken,
  preserveOnTermination = false,
): void {
  void cancelToken.promise
    .then(cancel => {
      if (preserveOnTermination && cancel.message === QueryManager.TERMINATION_MESSAGE) return;
      return cancelTaskExecution(id);
    })
    .catch(() => {});
}

export function cancelTaskExecution(id: string): Promise<void> {
  return Api.instance.post(`/druid/indexer/v1/task/${Api.encodePath(id)}/shutdown`, {});
}
