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

import { Column, QueryResult, SqlExpression, SqlQuery, SqlWithQuery } from '@druid-toolkit/query';

import {
  deepGet,
  deleteKeys,
  formatDuration,
  formatInteger,
  nonEmptyArray,
  oneOf,
  pluralIfNeeded,
} from '../../utils';
import type { AsyncState, AsyncStatusResponse } from '../async-query/async-query';
import type { DruidEngine } from '../druid-engine/druid-engine';
import { validDruidEngine } from '../druid-engine/druid-engine';
import type { QueryContext } from '../query-context/query-context';
import { Stages } from '../stages/stages';
import type {
  MsqTaskPayloadResponse,
  MsqTaskReportResponse,
  SegmentLoadWaiterStatus,
  TaskStatus,
} from '../task/task';

const IGNORE_CONTEXT_KEYS = [
  '__asyncIdentity__',
  '__timeColumn',
  'queryId',
  'sqlQueryId',
  'sqlInsertSegmentGranularity',
  'signature',
  'scanSignature',
  'sqlReplaceTimeChunks',
];

// Hack around the concept that we might get back a SqlWithQuery and will need to unpack it
function parseSqlQuery(queryString: string): SqlQuery | undefined {
  const q = SqlExpression.maybeParse(queryString);
  if (!q) return;
  if (q instanceof SqlWithQuery) return q.flattenWith();
  if (q instanceof SqlQuery) return q;
  return;
}

export interface ExecutionError {
  error: {
    errorCode: string;
    errorMessage?: string;
    [key: string]: any;
  };
  host?: string;
  taskId?: string;
  stageNumber?: number;
  exceptionStackTrace?: string;
}

export type ExecutionDestination =
  | {
      type: 'taskReport';
      numTotalRows?: number;
    }
  | { type: 'durableStorage'; numTotalRows?: number }
  | { type: 'dataSource'; dataSource: string; numTotalRows?: number };

export interface ExecutionDestinationPage {
  id: number;
  numRows: number;
  sizeInBytes: number;
}

export type ExecutionStatus = 'RUNNING' | 'FAILED' | 'SUCCESS';

export interface LastExecution {
  engine: DruidEngine;
  id: string;
}

export function validateLastExecution(possibleLastExecution: any): LastExecution | undefined {
  if (
    !possibleLastExecution ||
    !validDruidEngine(possibleLastExecution.engine) ||
    typeof possibleLastExecution.id !== 'string'
  ) {
    return;
  }

  return {
    engine: possibleLastExecution.engine,
    id: possibleLastExecution.id,
  };
}

export interface UsageInfo {
  pendingTasks: number;
  runningTasks: number;
}

function getUsageInfoFromStatusPayload(status: any): UsageInfo | undefined {
  const { pendingTasks, runningTasks } = status;
  if (typeof pendingTasks !== 'number' || typeof runningTasks !== 'number') return;
  return {
    pendingTasks,
    runningTasks,
  };
}

export interface CapacityInfo {
  availableTaskSlots: number;
  usedTaskSlots: number;
  totalTaskSlots: number;
}

function formatPendingMessage(
  usageInfo: UsageInfo,
  capacityInfo: CapacityInfo | undefined,
): string | undefined {
  const { pendingTasks, runningTasks } = usageInfo;
  if (!pendingTasks) return;

  const totalNeeded = runningTasks + pendingTasks;

  let baseMessage = `Launched ${formatInteger(runningTasks)}/${formatInteger(totalNeeded)} tasks.`;

  if (!capacityInfo) {
    return baseMessage;
  }

  const { availableTaskSlots, usedTaskSlots, totalTaskSlots } = capacityInfo;

  // If there are enough slots free: "Launched 2/4 tasks." (It will resolve very soon, no need to make it complicated.)
  if (pendingTasks <= availableTaskSlots) {
    return baseMessage;
  }

  baseMessage += ` Cluster is currently using ${formatInteger(usedTaskSlots)}/${formatInteger(
    totalTaskSlots,
  )} task slots.`;

  // If there are not enough slots free then there are two cases:
  if (totalNeeded <= totalTaskSlots) {
    // (1) not enough free, but enough total: "Launched 2/4 tasks. Cluster is currently using 5/6 task slots. Waiting for 1 task slot to become available."
    const tasksThatNeedToFinish = pendingTasks - availableTaskSlots;
    return (
      baseMessage +
      ` Waiting for ${pluralIfNeeded(tasksThatNeedToFinish, 'task slot')} to become available.`
    );
  } else {
    // (2) not enough total: "Launched 2/4 tasks. Cluster is currently using 2/2 task slots. Add more capacity or reduce maxNumTasks to 2 or lower."
    return (
      baseMessage +
      ` Add more capacity or reduce maxNumTasks to ${formatInteger(totalTaskSlots)} or lower.`
    );
  }
}

export interface ExecutionValue {
  engine: DruidEngine;
  id: string;
  sqlQuery?: string;
  nativeQuery?: any;
  queryContext?: QueryContext;
  status?: ExecutionStatus;
  startTime?: Date;
  duration?: number;
  usageInfo?: UsageInfo;
  stages?: Stages;
  destination?: ExecutionDestination;
  destinationPages?: ExecutionDestinationPage[];
  result?: QueryResult;
  error?: ExecutionError;
  warnings?: ExecutionError[];
  capacityInfo?: CapacityInfo;
  _payload?: MsqTaskPayloadResponse;
  segmentStatus?: SegmentLoadWaiterStatus;
}

export class Execution {
  static INLINE_DATASOURCE_MARKER = '__query_select';

  static validAsyncState(status: string | undefined): status is AsyncState {
    return oneOf(status, 'ACCEPTED', 'RUNNING', 'FINISHED', 'FAILED');
  }

  static validTaskStatus(status: string | undefined): status is TaskStatus {
    return oneOf(status, 'WAITING', 'PENDING', 'RUNNING', 'FAILED', 'SUCCESS');
  }

  static normalizeAsyncState(state: AsyncState): ExecutionStatus {
    switch (state) {
      case 'ACCEPTED':
        return 'RUNNING';

      default:
        return state;
    }
  }

  // Treat WAITING as PENDING since they are all the same as far as the UI is concerned
  static normalizeTaskStatus(status: TaskStatus): ExecutionStatus {
    switch (status) {
      case 'SUCCESS':
      case 'FAILED':
        return status;

      default:
        return 'RUNNING';
    }
  }

  static fromAsyncStatus(
    asyncSubmitResult: AsyncStatusResponse,
    sqlQuery?: string,
    queryContext?: QueryContext,
  ): Execution {
    const { queryId, schema, result, errorDetails } = asyncSubmitResult;

    let queryResult: QueryResult | undefined;
    if (schema && result?.sampleRecords) {
      queryResult = new QueryResult({
        header: schema.map(
          s => new Column({ name: s.name, sqlType: s.type, nativeType: s.nativeType }),
        ),
        rows: result.sampleRecords,
      }).inflateDatesFromSqlTypes();
    }

    let executionError: ExecutionError | undefined;
    if (errorDetails) {
      executionError = {
        taskId: queryId,
        error: errorDetails as any,
      };
    }

    return new Execution({
      engine: 'sql-msq-task',
      id: queryId,
      startTime: new Date(asyncSubmitResult.createdAt),
      duration: asyncSubmitResult.durationMs,
      status: Execution.normalizeAsyncState(asyncSubmitResult.state),
      sqlQuery,
      queryContext,
      error: executionError,
      destination:
        typeof result?.dataSource === 'string'
          ? result.dataSource !== Execution.INLINE_DATASOURCE_MARKER
            ? {
                type: 'dataSource',
                dataSource: result.dataSource,
                numTotalRows: result.numTotalRows,
              }
            : {
                type: 'taskReport',
                numTotalRows: result.numTotalRows,
              }
          : undefined,
      destinationPages: result?.pages,
      result: queryResult,
    });
  }

  static fromTaskReport(taskReport: MsqTaskReportResponse): Execution {
    // Must have status set for a valid report
    const id = deepGet(taskReport, 'multiStageQuery.taskId');
    const status = deepGet(taskReport, 'multiStageQuery.payload.status.status');
    const warnings = deepGet(taskReport, 'multiStageQuery.payload.status.warnings');

    if (typeof id !== 'string' || !Execution.validTaskStatus(status)) {
      throw new Error('Invalid payload');
    }

    let error: ExecutionError | undefined;
    if (status === 'FAILED') {
      error =
        deepGet(taskReport, 'multiStageQuery.payload.status.errorReport') ||
        (typeof taskReport.error === 'string'
          ? { error: { errorCode: 'UnknownError', errorMessage: taskReport.error } }
          : undefined);
    }

    const stages = deepGet(taskReport, 'multiStageQuery.payload.stages');
    const startTime = new Date(deepGet(taskReport, 'multiStageQuery.payload.status.startTime'));
    const durationMs = deepGet(taskReport, 'multiStageQuery.payload.status.durationMs');

    const segmentLoaderStatus: SegmentLoadWaiterStatus = deepGet(
      taskReport,
      'multiStageQuery.payload.status.segmentLoadWaiterStatus',
    );

    let result: QueryResult | undefined;
    const resultsPayload: {
      signature: { name: string; type: string }[];
      sqlTypeNames: string[];
      results: any[];
    } = deepGet(taskReport, 'multiStageQuery.payload.results');
    if (resultsPayload) {
      const { signature, sqlTypeNames, results } = resultsPayload;
      result = new QueryResult({
        header: signature.map(
          (sig, i: number) =>
            new Column({ name: sig.name, nativeType: sig.type, sqlType: sqlTypeNames?.[i] }),
        ),
        rows: results,
      }).inflateDatesFromSqlTypes();
    }

    return new Execution({
      engine: 'sql-msq-task',
      id,
      status: Execution.normalizeTaskStatus(status),
      segmentStatus: segmentLoaderStatus,
      startTime: isNaN(startTime.getTime()) ? undefined : startTime,
      duration: typeof durationMs === 'number' ? durationMs : undefined,
      usageInfo: getUsageInfoFromStatusPayload(
        deepGet(taskReport, 'multiStageQuery.payload.status'),
      ),
      stages: Array.isArray(stages)
        ? new Stages(stages, deepGet(taskReport, 'multiStageQuery.payload.counters'))
        : undefined,
      error,
      warnings: Array.isArray(warnings) ? warnings : undefined,
      result,
    });
  }

  static fromResult(engine: DruidEngine, result: QueryResult): Execution {
    return new Execution({
      engine,
      id: result.sqlQueryId || result.queryId || 'direct_result',
      status: 'SUCCESS',
      result,
      duration: result.queryDuration,
    });
  }

  static getProgressDescription(execution: Execution | undefined): string {
    if (!execution?.stages) return 'Loading...';
    if (!execution.isWaitingForQuery())
      return 'Query complete, waiting for segments to be loaded...';

    let ret = execution.stages.getStage(0)?.phase ? 'Running query...' : 'Starting query...';
    if (execution.usageInfo) {
      const pendingMessage = formatPendingMessage(execution.usageInfo, execution.capacityInfo);
      if (pendingMessage) {
        ret += ` ${pendingMessage}`;
      }
    }

    return ret;
  }

  public readonly engine: DruidEngine;
  public readonly id: string;
  public readonly sqlQuery?: string;
  public readonly nativeQuery?: any;
  public readonly queryContext?: QueryContext;
  public readonly status?: ExecutionStatus;
  public readonly startTime?: Date;
  public readonly duration?: number;
  public readonly usageInfo?: UsageInfo;
  public readonly stages?: Stages;
  public readonly destination?: ExecutionDestination;
  public readonly destinationPages?: ExecutionDestinationPage[];
  public readonly result?: QueryResult;
  public readonly error?: ExecutionError;
  public readonly warnings?: ExecutionError[];
  public readonly capacityInfo?: CapacityInfo;
  public readonly segmentStatus?: SegmentLoadWaiterStatus;

  public readonly _payload?: { payload: any; task: string };

  constructor(value: ExecutionValue) {
    this.engine = value.engine;
    this.id = value.id;
    if (!this.id) throw new Error('must have an id');
    this.sqlQuery = value.sqlQuery;
    this.nativeQuery = value.nativeQuery;
    this.queryContext = value.queryContext;
    this.status = value.status;
    this.startTime = value.startTime;
    this.duration = value.duration;
    this.usageInfo = value.usageInfo;
    this.stages = value.stages;
    this.destination = value.destination;
    this.destinationPages = value.destinationPages;
    this.result = value.result;
    this.error = value.error;
    this.warnings = nonEmptyArray(value.warnings) ? value.warnings : undefined;
    this.capacityInfo = value.capacityInfo;
    this.segmentStatus = value.segmentStatus;

    this._payload = value._payload;
  }

  valueOf(): ExecutionValue {
    return {
      engine: this.engine,
      id: this.id,
      sqlQuery: this.sqlQuery,
      nativeQuery: this.nativeQuery,
      queryContext: this.queryContext,
      status: this.status,
      startTime: this.startTime,
      duration: this.duration,
      usageInfo: this.usageInfo,
      stages: this.stages,
      destination: this.destination,
      destinationPages: this.destinationPages,
      result: this.result,
      error: this.error,
      warnings: this.warnings,
      capacityInfo: this.capacityInfo,
      segmentStatus: this.segmentStatus,

      _payload: this._payload,
    };
  }

  public changeSqlQuery(sqlQuery: string, queryContext?: QueryContext): Execution {
    const value = this.valueOf();

    value.sqlQuery = sqlQuery;
    value.queryContext = queryContext;
    const parsedQuery = parseSqlQuery(sqlQuery);
    if (value.result && (parsedQuery || queryContext)) {
      value.result = value.result.attachQuery({ context: queryContext }, parsedQuery);
    }

    return new Execution(value);
  }

  public changeDestination(destination: ExecutionDestination): Execution {
    return new Execution({
      ...this.valueOf(),
      destination,
    });
  }

  public changeDestinationPages(destinationPages: ExecutionDestinationPage[]): Execution {
    return new Execution({
      ...this.valueOf(),
      destinationPages,
    });
  }

  public changeResult(result: QueryResult): Execution {
    return new Execution({
      ...this.valueOf(),
      result: result.attachQuery({}, this.sqlQuery ? parseSqlQuery(this.sqlQuery) : undefined),
    });
  }

  public changeCapacityInfo(capacityInfo: CapacityInfo | undefined): Execution {
    return new Execution({
      ...this.valueOf(),
      capacityInfo,
    });
  }

  public updateWithTaskPayload(taskPayload: MsqTaskPayloadResponse): Execution {
    const value = this.valueOf();

    value._payload = taskPayload;
    value.destination = {
      ...value.destination,
      ...deepGet(taskPayload, 'payload.spec.destination'),
    };
    value.nativeQuery = deepGet(taskPayload, 'payload.spec.query');

    let ret = new Execution(value);

    if (deepGet(taskPayload, 'payload.sqlQuery')) {
      ret = ret.changeSqlQuery(
        deepGet(taskPayload, 'payload.sqlQuery'),
        deleteKeys(deepGet(taskPayload, 'payload.sqlQueryContext'), IGNORE_CONTEXT_KEYS),
      );
    }

    return ret;
  }

  public updateWithAsyncStatus(statusPayload: AsyncStatusResponse): Execution {
    const value = this.valueOf();

    const { pages, numTotalRows } = statusPayload.result || {};

    if (!value.destinationPages && pages) {
      value.destinationPages = pages;
    }

    if (typeof value.destination?.numTotalRows !== 'number' && typeof numTotalRows === 'number') {
      value.destination = {
        ...(value.destination || { type: 'taskReport' }),
        numTotalRows,
      };
    }

    return new Execution(value);
  }

  public isProcessingData(): boolean {
    const { status, stages } = this;
    return Boolean(
      status === 'RUNNING' &&
        stages &&
        stages.getTotalInputForStage(stages.getStage(0), 'rows') > 0,
    );
  }

  public isWaitingForQuery(): boolean {
    const { status } = this;
    return status !== 'SUCCESS' && status !== 'FAILED';
  }

  public getSegmentStatusDescription() {
    const { segmentStatus } = this;

    let label = '';

    switch (segmentStatus?.state) {
      case 'INIT':
        label = 'Waiting for segment loading to start...';
        break;

      case 'WAITING':
        label = 'Waiting for segment loading to complete...';
        break;

      case 'SUCCESS':
        label = `Segments loaded successfully in ${formatDuration(segmentStatus.duration)}`;
        break;

      default:
        break;
    }

    return {
      label,
      ...segmentStatus,
    };
  }

  public getIngestDatasource(): string | undefined {
    const { destination } = this;
    if (destination?.type !== 'dataSource') return;
    return destination.dataSource;
  }

  public getOutputNumTotalRows(): number | undefined {
    return this.destination?.numTotalRows;
  }

  public isSuccessfulIngest(): boolean {
    return Boolean(this.status === 'SUCCESS' && this.getIngestDatasource());
  }

  public getErrorMessage(): string | undefined {
    const { error } = this;
    if (!error) return;
    return (
      (error.error.errorCode ? `${error.error.errorCode}: ` : '') +
      (error.error.errorMessage || (error.exceptionStackTrace || '').split('\n')[0])
    );
  }

  public getEndTime(): Date | undefined {
    const { startTime, duration } = this;
    if (!startTime || !duration) return;
    return new Date(startTime.valueOf() + duration);
  }

  public hasPotentiallyStuckStage(): boolean {
    return Boolean(
      this.status === 'RUNNING' &&
        this.stages &&
        this.stages.getPotentiallyStuckStageIndex() >= 0 &&
        this.usageInfo &&
        this.usageInfo.pendingTasks > 0,
    );
  }
}
