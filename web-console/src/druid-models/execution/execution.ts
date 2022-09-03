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

import { Column, QueryResult, SqlExpression, SqlQuery, SqlWithQuery } from 'druid-query-toolkit';

import { deepGet, deleteKeys, nonEmptyArray, oneOf } from '../../utils';
import { DruidEngine, validDruidEngine } from '../druid-engine/druid-engine';
import { QueryContext } from '../query-context/query-context';
import { Stages } from '../stages/stages';

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

type ExecutionDestination =
  | {
      type: 'taskReport';
    }
  | { type: 'dataSource'; dataSource: string; exists?: boolean }
  | { type: 'download' };

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

export interface ExecutionValue {
  engine: DruidEngine;
  id: string;
  sqlQuery?: string;
  nativeQuery?: any;
  queryContext?: QueryContext;
  status?: ExecutionStatus;
  startTime?: Date;
  duration?: number;
  stages?: Stages;
  destination?: ExecutionDestination;
  result?: QueryResult;
  error?: ExecutionError;
  warnings?: ExecutionError[];
  _payload?: { payload: any; task: string };
}

export class Execution {
  static validAsyncStatus(
    status: string | undefined,
  ): status is 'INITIALIZED' | 'RUNNING' | 'COMPLETE' | 'FAILED' | 'UNDETERMINED' {
    return oneOf(status, 'INITIALIZED', 'RUNNING', 'COMPLETE', 'FAILED', 'UNDETERMINED');
  }

  static validTaskStatus(
    status: string | undefined,
  ): status is 'WAITING' | 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS' {
    return oneOf(status, 'WAITING', 'PENDING', 'RUNNING', 'FAILED', 'SUCCESS');
  }

  static normalizeAsyncStatus(
    state: 'INITIALIZED' | 'RUNNING' | 'COMPLETE' | 'FAILED' | 'UNDETERMINED',
  ): ExecutionStatus {
    switch (state) {
      case 'COMPLETE':
        return 'SUCCESS';

      case 'INITIALIZED':
      case 'UNDETERMINED':
        return 'RUNNING';

      default:
        return state;
    }
  }

  // Treat WAITING as PENDING since they are all the same as far as the UI is concerned
  static normalizeTaskStatus(
    status: 'WAITING' | 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS',
  ): ExecutionStatus {
    switch (status) {
      case 'SUCCESS':
      case 'FAILED':
        return status;

      default:
        return 'RUNNING';
    }
  }

  static fromTaskSubmit(
    taskSubmitResult: { state: any; taskId: string; error: any },
    sqlQuery?: string,
    queryContext?: QueryContext,
  ): Execution {
    const status = Execution.normalizeTaskStatus(taskSubmitResult.state);
    return new Execution({
      engine: 'sql-msq-task',
      id: taskSubmitResult.taskId,
      status: taskSubmitResult.error ? 'FAILED' : status,
      sqlQuery,
      queryContext,
      error: taskSubmitResult.error
        ? {
            error: {
              errorCode: 'AsyncError',
              errorMessage: JSON.stringify(taskSubmitResult.error),
            },
          }
        : status === 'FAILED'
        ? {
            error: {
              errorCode: 'UnknownError',
              errorMessage:
                'Execution failed, there is no detail information, and there is no error in the status response',
            },
          }
        : undefined,
      destination: undefined,
    });
  }

  static fromTaskStatus(
    taskStatus: { status: any; task: string },
    sqlQuery?: string,
    queryContext?: QueryContext,
  ): Execution {
    const status = Execution.normalizeTaskStatus(taskStatus.status.status);
    return new Execution({
      engine: 'sql-msq-task',
      id: taskStatus.task,
      status: taskStatus.status.error ? 'FAILED' : status,
      sqlQuery,
      queryContext,
      error: taskStatus.status.error
        ? {
            error: {
              errorCode: 'AsyncError',
              errorMessage: JSON.stringify(taskStatus.status.error),
            },
          }
        : status === 'FAILED'
        ? {
            error: {
              errorCode: 'UnknownError',
              errorMessage:
                'Execution failed, there is no detail information, and there is no error in the status response',
            },
          }
        : undefined,
      destination: undefined,
    });
  }

  static fromTaskPayloadAndReport(
    taskPayload: { payload: any; task: string },
    taskReport: {
      multiStageQuery: { type: string; payload: any; taskId: string };
      error?: any;
    },
  ): Execution {
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

    let res = new Execution({
      engine: 'sql-msq-task',
      id,
      status: Execution.normalizeTaskStatus(status),
      startTime: isNaN(startTime.getTime()) ? undefined : startTime,
      duration: typeof durationMs === 'number' ? durationMs : undefined,
      stages: Array.isArray(stages)
        ? new Stages(stages, deepGet(taskReport, 'multiStageQuery.payload.counters'))
        : undefined,
      error,
      warnings: Array.isArray(warnings) ? warnings : undefined,
      destination: deepGet(taskPayload, 'payload.spec.destination'),
      result,
      nativeQuery: deepGet(taskPayload, 'payload.spec.query'),

      _payload: taskPayload,
    });

    if (deepGet(taskPayload, 'payload.sqlQuery')) {
      res = res.changeSqlQuery(
        deepGet(taskPayload, 'payload.sqlQuery'),
        deleteKeys(deepGet(taskPayload, 'payload.sqlQueryContext'), IGNORE_CONTEXT_KEYS),
      );
    }

    return res;
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

  public readonly engine: DruidEngine;
  public readonly id: string;
  public readonly sqlQuery?: string;
  public readonly nativeQuery?: any;
  public readonly queryContext?: QueryContext;
  public readonly status?: ExecutionStatus;
  public readonly startTime?: Date;
  public readonly duration?: number;
  public readonly stages?: Stages;
  public readonly destination?: ExecutionDestination;
  public readonly result?: QueryResult;
  public readonly error?: ExecutionError;
  public readonly warnings?: ExecutionError[];

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
    this.stages = value.stages;
    this.destination = value.destination;
    this.result = value.result;
    this.error = value.error;
    this.warnings = nonEmptyArray(value.warnings) ? value.warnings : undefined;

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
      stages: this.stages,
      destination: this.destination,
      result: this.result,
      error: this.error,
      warnings: this.warnings,

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

  public changeResult(result: QueryResult): Execution {
    return new Execution({
      ...this.valueOf(),
      result: result.attachQuery({}, this.sqlQuery ? parseSqlQuery(this.sqlQuery) : undefined),
    });
  }

  public updateWith(newSummary: Execution): Execution {
    let nextSummary = newSummary;
    if (this.sqlQuery && !nextSummary.sqlQuery) {
      nextSummary = nextSummary.changeSqlQuery(this.sqlQuery, this.queryContext);
    }
    if (this.destination && !nextSummary.destination) {
      nextSummary = nextSummary.changeDestination(this.destination);
    }

    return nextSummary;
  }

  public attachErrorFromStatus(status: any): Execution {
    const errorMsg = deepGet(status, 'status.errorMsg');

    return new Execution({
      ...this.valueOf(),
      error: {
        error: {
          errorCode: 'UnknownError',
          errorMessage: errorMsg,
        },
      },
    });
  }

  public markDestinationDatasourceExists(): Execution {
    const { destination } = this;
    if (destination?.type !== 'dataSource') return this;

    return new Execution({
      ...this.valueOf(),
      destination: {
        ...destination,
        exists: true,
      },
    });
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

  public isFullyComplete(): boolean {
    if (this.isWaitingForQuery()) return false;

    const { status, destination } = this;
    if (status === 'SUCCESS' && destination?.type === 'dataSource') {
      return Boolean(destination.exists);
    }

    return true;
  }

  public getIngestDatasource(): string | undefined {
    const { destination } = this;
    if (destination?.type !== 'dataSource') return;
    return destination.dataSource;
  }

  public isSuccessfulInsert(): boolean {
    return Boolean(
      this.isFullyComplete() && this.getIngestDatasource() && this.status === 'SUCCESS',
    );
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
}
