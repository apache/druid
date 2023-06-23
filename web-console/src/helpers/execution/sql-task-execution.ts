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

import type { AxiosResponse, CancelToken } from 'axios';
import { L, QueryResult } from 'druid-query-toolkit';

import type { QueryContext } from '../../druid-models';
import { Execution } from '../../druid-models';
import { Api } from '../../singletons';
import {
  deepGet,
  DruidError,
  IntermediateQueryState,
  queryDruidSql,
  QueryManager,
} from '../../utils';
import { maybeGetClusterCapacity } from '../capacity';

const WAIT_FOR_SEGMENT_METADATA_TIMEOUT = 180000; // 3 minutes to wait until segments appear in the metadata
const WAIT_FOR_SEGMENT_LOAD_TIMEOUT = 540000; // 9 minutes to wait for segments to load at all

export interface SubmitTaskQueryOptions {
  query: string | Record<string, any>;
  context?: QueryContext;
  skipResults?: boolean;
  prefixLines?: number;
  cancelToken?: CancelToken;
  preserveOnTermination?: boolean;
  onSubmitted?: (id: string) => void;
}

export async function submitTaskQuery(
  options: SubmitTaskQueryOptions,
): Promise<Execution | IntermediateQueryState<Execution>> {
  const {
    query,
    context,
    skipResults,
    prefixLines,
    cancelToken,
    preserveOnTermination,
    onSubmitted,
  } = options;

  let sqlQuery: string;
  let jsonQuery: Record<string, any>;
  if (typeof query === 'string') {
    sqlQuery = query;
    jsonQuery = {
      query: sqlQuery,
      resultFormat: 'array',
      header: true,
      typesHeader: true,
      sqlTypesHeader: true,
      context: context,
    };
  } else {
    sqlQuery = query.query;

    if (context) {
      jsonQuery = {
        ...query,
        context: {
          ...(query.context || {}),
          ...context,
        },
      };
    } else {
      jsonQuery = query;
    }
  }

  let sqlTaskResp: AxiosResponse;

  try {
    sqlTaskResp = await Api.instance.post(`/druid/v2/sql/task`, jsonQuery, { cancelToken });
  } catch (e) {
    const druidError = deepGet(e, 'response.data.error');
    if (!druidError) throw e;
    throw new DruidError(druidError, prefixLines);
  }

  const sqlTaskPayload = sqlTaskResp.data;

  if (!sqlTaskPayload.taskId) {
    if (!Array.isArray(sqlTaskPayload)) throw new Error('unexpected task payload');
    return Execution.fromResult(
      'sql-msq-task',
      QueryResult.fromRawResult(sqlTaskPayload, false, true, true, true),
    );
  }

  let execution = Execution.fromTaskSubmit(sqlTaskPayload, sqlQuery, context);

  if (onSubmitted) {
    onSubmitted(execution.id);
  }

  if (skipResults) {
    execution = execution.changeDestination({ type: 'download' });
  }

  execution = await updateExecutionWithDatasourceLoadedIfNeeded(execution, cancelToken);

  if (execution.isFullyComplete()) return execution;

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
    execution = await updateExecutionWithDatasourceLoadedIfNeeded(execution, cancelToken);
  } catch (e) {
    throw new Error(`Reattaching to query failed due to: ${e.message}`);
  }

  if (execution.isFullyComplete()) return execution;

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
  return execution.updateWith(
    await getTaskExecution(execution.id, execution._payload, cancelToken),
  );
}

export async function getTaskExecution(
  id: string,
  taskPayloadOverride?: { payload: any; task: string },
  cancelToken?: CancelToken,
): Promise<Execution> {
  const encodedId = Api.encodePath(id);

  let taskPayloadResp: AxiosResponse | undefined;
  if (!taskPayloadOverride) {
    try {
      taskPayloadResp = await Api.instance.get(`/druid/indexer/v1/task/${encodedId}`, {
        cancelToken,
      });
    } catch (e) {
      if (Api.isNetworkError(e)) throw e;
    }
  }

  let taskReportResp: AxiosResponse | undefined;
  try {
    taskReportResp = await Api.instance.get(`/druid/indexer/v1/task/${encodedId}/reports`, {
      cancelToken,
    });
  } catch (e) {
    if (Api.isNetworkError(e)) throw e;
  }

  if ((taskPayloadResp || taskPayloadOverride) && taskReportResp) {
    let execution: Execution | undefined;
    try {
      execution = Execution.fromTaskPayloadAndReport(
        taskPayloadResp ? taskPayloadResp.data : taskPayloadOverride,
        taskReportResp.data,
      );
    } catch {
      // We got a bad payload, wait a bit and try to get the payload again (also log it)
      // This whole catch block is a hack, and we should make the detail route more robust
      console.error(
        `Got unusable response from the reports endpoint (/druid/indexer/v1/task/${encodedId}/reports) going to retry`,
      );
      console.log('Report response:', taskReportResp.data);
    }

    if (execution) {
      if (execution?.hasPotentiallyStuckStage()) {
        const capacityInfo = await maybeGetClusterCapacity();
        if (capacityInfo) {
          execution = execution.changeCapacityInfo(capacityInfo);
        }
      }

      return execution;
    }
  }

  const statusResp = await Api.instance.get(`/druid/indexer/v1/task/${encodedId}/status`, {
    cancelToken,
  });

  return Execution.fromTaskStatus(statusResp.data);
}

export async function updateExecutionWithDatasourceLoadedIfNeeded(
  execution: Execution,
  _cancelToken?: CancelToken,
): Promise<Execution> {
  if (
    !(execution.destination?.type === 'dataSource' && !execution.destination.loaded) ||
    execution.status !== 'SUCCESS'
  ) {
    return execution;
  }

  const endTime = execution.getEndTime();
  if (
    !endTime || // If endTime is not set (this is not expected to happen) then just bow out
    execution.stages?.getLastStage()?.partitionCount === 0 || // No data was meant to be written anyway, nothing to do
    endTime.valueOf() + WAIT_FOR_SEGMENT_LOAD_TIMEOUT < Date.now() // Enough time has passed since the query ran... don't bother waiting for segments to load.
  ) {
    return execution.markDestinationDatasourceLoaded();
  }

  // Ideally we would have a more accurate query here, instead of
  //   COUNT(*) FILTER (WHERE is_published = 1 AND is_available = 0)
  // we want to filter on something like
  //   COUNT(*) FILTER (WHERE is_should_be_available = 1 AND is_available = 0)
  // `is_published` does not quite capture what we want but this is the best we have for now.
  const segmentCheck = await queryDruidSql({
    query: `SELECT
  COUNT(*) AS num_segments,
  COUNT(*) FILTER (WHERE is_published = 1 AND is_available = 0) AS loading_segments
FROM sys.segments
WHERE datasource = ${L(execution.destination.dataSource)} AND is_overshadowed = 0`,
  });

  const numSegments: number = deepGet(segmentCheck, '0.num_segments') || 0;
  const loadingSegments: number = deepGet(segmentCheck, '0.loading_segments') || 0;

  // There appear to be no segments, since we checked above that something was written out we know that they have not shown up in the metadata yet
  if (numSegments === 0) {
    if (endTime.valueOf() + WAIT_FOR_SEGMENT_METADATA_TIMEOUT < Date.now()) {
      // Enough time has passed since the query ran... give up waiting for segments to show up in metadata.
      return execution.markDestinationDatasourceLoaded();
    }

    return execution;
  }

  // There are segments, and we are still waiting for some of them to load
  if (loadingSegments > 0) return execution;

  return execution.markDestinationDatasourceLoaded();
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
