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

import type { QueryResult } from '@druid-toolkit/query';
import type { CancelToken } from 'axios';

import type { Execution } from '../../druid-models';
import { IntermediateQueryState } from '../../utils';

import { updateExecutionWithTaskIfNeeded } from './sql-task-execution';

export function extractResult(
  execution: Execution | IntermediateQueryState<Execution>,
): QueryResult | IntermediateQueryState<Execution> {
  if (execution instanceof IntermediateQueryState) return execution;

  if (execution.result) {
    return execution.result;
  } else {
    const error = new Error(execution.getErrorMessage() || 'unexpected destination');
    if (execution.error) (error as any).executionError = execution.error;
    throw error;
  }
}

export async function executionBackgroundStatusCheck(
  execution: Execution,
  _query: any,
  cancelToken: CancelToken,
): Promise<Execution | IntermediateQueryState<Execution>> {
  switch (execution.engine) {
    case 'sql-msq-task':
      execution = await updateExecutionWithTaskIfNeeded(execution, cancelToken);
      break;

    default:
      throw new Error(`can not background check execution for engine ${execution.engine}`);
  }

  if (execution.isWaitingForQuery()) return new IntermediateQueryState(execution);

  return execution;
}

export async function executionBackgroundResultStatusCheck(
  execution: Execution,
  query: any,
  cancelToken: CancelToken,
): Promise<QueryResult | IntermediateQueryState<Execution>> {
  return extractResult(await executionBackgroundStatusCheck(execution, query, cancelToken));
}
