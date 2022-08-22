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

import { AxiosResponse, CancelToken } from 'axios';
import { SqlLiteral } from 'druid-query-toolkit';

import { Execution, QueryContext } from '../../druid-models';
import { Api } from '../../singletons';
import {
  deepGet,
  DruidError,
  IntermediateQueryState,
  queryDruidSql,
  QueryManager,
} from '../../utils';

const WAIT_FOR_SEGMENTS_TIMEOUT = 180000; // 3 minutes to wait until segments appear

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

  let execution = Execution.fromTaskSubmit(sqlTaskResp.data, sqlQuery, context);

  if (onSubmitted) {
    onSubmitted(execution.id);
  }

  if (skipResults) {
    execution = execution.changeDestination({ type: 'download' });
  }

  execution = await updateExecutionWithDatasourceExistsIfNeeded(execution, cancelToken);

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
  let execution = await getTaskExecution(id, undefined, cancelToken);

  execution = await updateExecutionWithDatasourceExistsIfNeeded(execution, cancelToken);

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
    try {
      return Execution.fromTaskPayloadAndReport(
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
  }

  const statusResp = await Api.instance.get(`/druid/indexer/v1/task/${encodedId}/status`, {
    cancelToken,
  });

  return Execution.fromTaskStatus(statusResp.data);
}

export async function updateExecutionWithDatasourceExistsIfNeeded(
  execution: Execution,
  _cancelToken?: CancelToken,
): Promise<Execution> {
  if (
    !(execution.destination?.type === 'dataSource' && !execution.destination.exists) ||
    execution.status !== 'SUCCESS'
  ) {
    return execution;
  }

  const segmentCheck = await queryDruidSql({
    query: `SELECT
  COUNT(*) AS num_segments,
  COUNT(*) FILTER (WHERE is_published = 1 AND is_available = 0) AS loading_segments
FROM sys.segments
WHERE datasource = ${SqlLiteral.create(execution.destination.dataSource)} AND is_overshadowed = 0`,
  });

  const numSegments: number = deepGet(segmentCheck, '0.num_segments') || 0;
  const loadingSegments: number = deepGet(segmentCheck, '0.loading_segments') || 0;

  // There appear to be no segments either nothing was written out or they have not shown up in the metadata yet
  if (numSegments === 0) {
    const { stages } = execution;
    if (stages) {
      const lastStage = stages.getStage(stages.stageCount() - 1);
      if (lastStage.partitionCount === 0) {
        // No data was meant to be written anyway
        return execution.markDestinationDatasourceExists();
      }
    }

    const endTime = execution.getEndTime();
    if (!endTime || endTime.valueOf() + WAIT_FOR_SEGMENTS_TIMEOUT < Date.now()) {
      // Enough time has passed since the query ran... give up waiting (or there is no time info).
      return execution.markDestinationDatasourceExists();
    }

    return execution;
  }

  // There are segments, and we are still waiting for some of them to load
  if (loadingSegments > 0) return execution;

  return execution.markDestinationDatasourceExists();
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
