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

import { Code, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import { QueryResult, QueryRunner, SqlQuery } from 'druid-query-toolkit';
import type { JSX } from 'react';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useStore } from 'zustand';

import { Loader, QueryErrorPane, SplitterLayout } from '../../../components';
import type { CapacityInfo, DruidEngine, LastExecution, QueryContext } from '../../../druid-models';
import { DEFAULT_SERVER_QUERY_CONTEXT, Execution, WorkbenchQuery } from '../../../druid-models';
import {
  executionBackgroundStatusCheck,
  reattachTaskExecution,
  submitTaskQuery,
} from '../../../helpers';
import { usePermanentCallback, useQueryManager } from '../../../hooks';
import { Api, AppToaster } from '../../../singletons';
import { ExecutionStateCache } from '../../../singletons/execution-state-cache';
import { WorkbenchHistory } from '../../../singletons/workbench-history';
import type { WorkbenchRunningPromise } from '../../../singletons/workbench-running-promises';
import { WorkbenchRunningPromises } from '../../../singletons/workbench-running-promises';
import type { ColumnMetadata, QueryAction, QuerySlice, RowColumn } from '../../../utils';
import {
  deepGet,
  DruidError,
  findAllSqlQueriesInText,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSetJson,
  QueryManager,
} from '../../../utils';
import { CapacityAlert } from '../capacity-alert/capacity-alert';
import type { ExecutionDetailsTab } from '../execution-details-pane/execution-details-pane';
import { ExecutionErrorPane } from '../execution-error-pane/execution-error-pane';
import { ExecutionProgressPane } from '../execution-progress-pane/execution-progress-pane';
import { ExecutionStagesPane } from '../execution-stages-pane/execution-stages-pane';
import { ExecutionSummaryPanel } from '../execution-summary-panel/execution-summary-panel';
import { ExecutionTimerPanel } from '../execution-timer-panel/execution-timer-panel';
import { FlexibleQueryInput } from '../flexible-query-input/flexible-query-input';
import { IngestSuccessPane } from '../ingest-success-pane/ingest-success-pane';
import { metadataStateStore } from '../metadata-state-store';
import { ResultTablePane } from '../result-table-pane/result-table-pane';
import type { RunPanelProps } from '../run-panel/run-panel';
import { RunPanel } from '../run-panel/run-panel';
import { WORK_STATE_STORE } from '../work-state-store';

import './query-tab.scss';

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'none',
});

function handleSecondaryPaneSizeChange(secondaryPaneSize: number) {
  localStorageSetJson(LocalStorageKeys.WORKBENCH_PANE_SIZE, secondaryPaneSize);
}

export interface QueryTabProps
  extends Pick<
    RunPanelProps,
    | 'maxTasksMenuHeader'
    | 'enginesLabelFn'
    | 'maxTasksLabelFn'
    | 'fullClusterCapacityLabelFn'
    | 'maxTasksOptions'
    | 'hiddenOptions'
  > {
  query: WorkbenchQuery;
  id: string;
  mandatoryQueryContext: QueryContext | undefined;
  baseQueryContext: QueryContext | undefined;
  serverQueryContext: QueryContext;
  columnMetadata: readonly ColumnMetadata[] | undefined;
  onQueryChange(newQuery: WorkbenchQuery): void;
  onQueryTab(newQuery: WorkbenchQuery, tabName?: string): void;
  onDetails(execution: Execution, initTab?: ExecutionDetailsTab): void;
  queryEngines: DruidEngine[];
  runMoreMenu: JSX.Element;
  clusterCapacity: number | undefined;
  goToTask(taskId: string): void;
  getClusterCapacity: (() => Promise<CapacityInfo | undefined>) | undefined;
}

export const QueryTab = React.memo(function QueryTab(props: QueryTabProps) {
  const {
    query,
    id,
    columnMetadata,
    mandatoryQueryContext,
    baseQueryContext,
    serverQueryContext = DEFAULT_SERVER_QUERY_CONTEXT,
    onQueryChange,
    onQueryTab,
    onDetails,
    queryEngines,
    runMoreMenu,
    clusterCapacity,
    goToTask,
    getClusterCapacity,
    maxTasksMenuHeader,
    enginesLabelFn,
    maxTasksLabelFn,
    maxTasksOptions,
    fullClusterCapacityLabelFn,
    hiddenOptions,
  } = props;
  const [alertElement, setAlertElement] = useState<JSX.Element | undefined>();

  // Store the cancellation function for natively run queries allowing us to trigger it only when the user explicitly clicks "cancel" (vs changing tab)
  const nativeQueryCancelFnRef = useRef<() => void>();

  const handleQueryStringChange = usePermanentCallback((queryString: string) => {
    if (query.isEmptyQuery() && queryString.split('=====').length > 2) {
      let parsedWorkbenchQuery: WorkbenchQuery | undefined;
      try {
        parsedWorkbenchQuery = WorkbenchQuery.fromString(queryString);
      } catch (e) {
        AppToaster.show({
          icon: IconNames.CLIPBOARD,
          intent: Intent.DANGER,
          message: `Could not paste tab content due to: ${e.message}`,
        });
      }
      if (parsedWorkbenchQuery) {
        onQueryChange(parsedWorkbenchQuery);
        return;
      }
    }
    onQueryChange(query.changeQueryString(queryString));
  });

  const handleQueryAction = usePermanentCallback(
    (queryAction: QueryAction, sliceIndex: number | undefined) => {
      let newQueryString: string;
      if (typeof sliceIndex === 'number') {
        const { queryString } = query;
        const foundQuery = findAllSqlQueriesInText(queryString)[sliceIndex];
        if (!foundQuery) return;
        const parsedQuery = SqlQuery.maybeParse(foundQuery.sql);
        if (!parsedQuery) return;
        newQueryString =
          queryString.slice(0, foundQuery.startOffset) +
          parsedQuery.apply(queryAction) +
          queryString.slice(foundQuery.endOffset);
      } else {
        const parsedQuery = query.getParsedQuery();
        if (!(parsedQuery instanceof SqlQuery)) return;
        newQueryString = parsedQuery.apply(queryAction).toString();
      }
      onQueryChange(query.changeQueryString(newQueryString));

      if (shouldAutoRun()) {
        setTimeout(() => {
          if (typeof sliceIndex === 'number') {
            const slice = findAllSqlQueriesInText(newQueryString)[sliceIndex];
            if (slice) {
              void handleRun(false, slice);
            }
          } else {
            void handleRun(false);
          }
        }, 20);
      }
    },
  );

  function shouldAutoRun(): boolean {
    const effectiveEngine = query.getEffectiveEngine();
    if (effectiveEngine !== 'sql-native' && effectiveEngine !== 'sql-msq-dart') return false;
    const queryDuration = executionState.data?.result?.queryDuration;
    return Boolean(queryDuration && queryDuration < 10000);
  }

  const queryInputRef = useRef<FlexibleQueryInput | null>(null);

  const cachedExecutionState = ExecutionStateCache.getState(id);
  const currentRunningPromise = WorkbenchRunningPromises.getPromise(id);
  const [executionState, queryManager] = useQueryManager<
    WorkbenchQuery | WorkbenchRunningPromise | LastExecution,
    Execution,
    Execution,
    DruidError
  >({
    initQuery: cachedExecutionState ? undefined : currentRunningPromise || query.getLastExecution(),
    initState: cachedExecutionState,
    processQuery: async (q, cancelToken) => {
      if (q instanceof WorkbenchQuery) {
        ExecutionStateCache.deleteState(id);
        const { engine, query, prefixLines, cancelQueryId } = q.getApiQuery();

        const startTime = new Date();
        switch (engine) {
          case 'sql-msq-task':
            return await submitTaskQuery({
              query,
              context: mandatoryQueryContext,
              baseQueryContext,
              prefixLines,
              cancelToken,
              preserveOnTermination: true,
              onSubmitted: id => {
                onQueryChange(props.query.changeLastExecution({ engine, id }));
              },
            });

          case 'native':
          case 'sql-native': {
            if (cancelQueryId) {
              void cancelToken.promise
                .then(cancel => {
                  if (cancel.message === QueryManager.TERMINATION_MESSAGE) return;
                  return Api.instance.delete(
                    `/druid/v2${engine === 'sql-native' ? '/sql' : ''}/${Api.encodePath(
                      cancelQueryId,
                    )}`,
                  );
                })
                .catch(() => {});
            }

            onQueryChange(props.query.changeLastExecution(undefined));

            const executionPromise = queryRunner
              .runQuery({
                query,
                extraQueryContext: mandatoryQueryContext,
                defaultQueryContext: baseQueryContext,
                cancelToken: new axios.CancelToken(cancelFn => {
                  nativeQueryCancelFnRef.current = cancelFn;
                }),
              })
              .then(
                queryResult => Execution.fromResult(engine, queryResult),
                e => {
                  const druidError = new DruidError(e, prefixLines);
                  druidError.queryDuration = Date.now() - startTime.valueOf();
                  throw druidError;
                },
              );

            WorkbenchRunningPromises.storePromise(id, {
              executionPromise,
              startTime,
            });

            let execution: Execution;
            try {
              execution = await executionPromise;
              nativeQueryCancelFnRef.current = undefined;
            } catch (e) {
              nativeQueryCancelFnRef.current = undefined;
              throw e;
            }

            return execution;
          }

          case 'sql-msq-dart': {
            if (cancelQueryId) {
              void cancelToken.promise
                .then(cancel => {
                  if (cancel.message === QueryManager.TERMINATION_MESSAGE) return;
                  return Api.instance.delete(`/druid/v2/sql/dart/${Api.encodePath(cancelQueryId)}`);
                })
                .catch(() => {});
            }

            onQueryChange(props.query.changeLastExecution(undefined));

            const executionPromise = Api.instance
              .post(`/druid/v2/sql/dart`, query, {
                cancelToken: new axios.CancelToken(cancelFn => {
                  nativeQueryCancelFnRef.current = cancelFn;
                }),
              })
              .then(
                ({ data: dartResponse }) => {
                  if (deepGet(query, 'context.fullReport') && dartResponse[0][0] === 'fullReport') {
                    const dartReport = dartResponse[dartResponse.length - 1][0];

                    return Execution.fromDartReport(dartReport).changeSqlQuery(
                      query.query,
                      query.context,
                    );
                  } else {
                    return Execution.fromResult(
                      engine,
                      QueryResult.fromRawResult(
                        dartResponse,
                        false,
                        query.header,
                        query.typesHeader,
                        query.sqlTypesHeader,
                      ).changeQueryDuration(Date.now() - startTime.valueOf()),
                    ).changeSqlQuery(query.query, query.context);
                  }
                },
                e => {
                  throw new DruidError(e, prefixLines);
                },
              );

            WorkbenchRunningPromises.storePromise(id, {
              executionPromise,
              startTime,
            });

            let execution: Execution;
            try {
              execution = await executionPromise;
              nativeQueryCancelFnRef.current = undefined;
            } catch (e) {
              nativeQueryCancelFnRef.current = undefined;
              throw e;
            }

            return execution;
          }
        }
      } else if (WorkbenchRunningPromises.isWorkbenchRunningPromise(q)) {
        return await q.executionPromise;
      } else {
        switch (q.engine) {
          case 'sql-msq-task':
            return await reattachTaskExecution({
              id: q.id,
              cancelToken,
              preserveOnTermination: true,
            });

          default:
            throw new Error(`Can not reattach on ${q.engine}`);
        }
      }
    },
    backgroundStatusCheck: executionBackgroundStatusCheck,
    swallowBackgroundError: Api.isNetworkError,
  });

  useEffect(() => {
    if (!executionState.data && !executionState.error) return;
    WorkbenchRunningPromises.deletePromise(id);
    ExecutionStateCache.storeState(id, executionState);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionState.data, executionState.error]);

  useEffect(() => {
    const effectiveEngine = query.getEffectiveEngine();
    if (effectiveEngine === 'sql-msq-task') {
      WORK_STATE_STORE.getState().incrementMsqTask();
    } else if (effectiveEngine === 'sql-msq-dart') {
      WORK_STATE_STORE.getState().incrementMsqDart();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionState.loading, Boolean(executionState.intermediate)]);

  const execution = executionState.data;

  // This is the execution that would be shown in the output pane, it is either the actual execution or a result
  // execution that will be shown under the loader
  const executionToShow =
    execution ||
    (() => {
      if (executionState.intermediate) return;
      const e = executionState.getSomeData();
      return e?.result ? e : undefined;
    })();

  const incrementMetadataVersion = useStore(
    metadataStateStore,
    useCallback(state => state.increment, []),
  );
  useEffect(() => {
    if (execution?.isSuccessfulIngest()) {
      incrementMetadataVersion();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [Boolean(execution?.isSuccessfulIngest())]);

  function moveToPosition(position: RowColumn) {
    const currentQueryInput = queryInputRef.current;
    if (!currentQueryInput) return;
    currentQueryInput.goToPosition(position);
  }

  const handleRun = usePermanentCallback(async (preview: boolean, querySlice?: QuerySlice) => {
    const queryIssue = query.getIssue();
    if (queryIssue) {
      const position = WorkbenchQuery.getRowColumnFromIssue(queryIssue);

      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        timeout: 90000,
        message: queryIssue,
        action: position
          ? {
              text: 'Go to issue',
              onClick: () => moveToPosition(position),
            }
          : undefined,
      });
      return;
    }

    let effectiveQuery = query;
    if (querySlice) {
      effectiveQuery = effectiveQuery
        .changeQueryString(querySlice.sql)
        .changeQueryContext({ ...effectiveQuery.queryContext, sliceIndex: querySlice.index })
        .changePrefixLines(querySlice.startRowColumn.row);
    }

    if (effectiveQuery.getEffectiveEngine() === 'sql-msq-task') {
      effectiveQuery = preview
        ? effectiveQuery.makePreview()
        : effectiveQuery.setMaxNumTasksIfUnset(clusterCapacity);

      const capacityInfo = await getClusterCapacity?.();

      const effectiveMaxNumTasks = effectiveQuery.queryContext.maxNumTasks ?? 2;
      if (capacityInfo && capacityInfo.availableTaskSlots < effectiveMaxNumTasks) {
        setAlertElement(
          <CapacityAlert
            maxNumTasks={effectiveMaxNumTasks}
            capacityInfo={capacityInfo}
            onRun={() => {
              queryManager.runQuery(effectiveQuery);
            }}
            onClose={() => {
              setAlertElement(undefined);
            }}
          />,
        );
        return;
      }
    } else {
      WorkbenchHistory.addQueryToHistory(effectiveQuery);
    }
    queryManager.runQuery(effectiveQuery);
  });

  const onUserCancel = (message?: string) => {
    queryManager.cancelCurrent(message);
    nativeQueryCancelFnRef.current?.();
  };

  return (
    <div className="query-tab">
      <SplitterLayout
        vertical
        percentage
        secondaryInitialSize={
          Number(localStorageGetJson(LocalStorageKeys.WORKBENCH_PANE_SIZE)) || 40
        }
        primaryMinSize={20}
        secondaryMinSize={20}
        onSecondaryPaneSizeChange={handleSecondaryPaneSizeChange}
      >
        <div className="top-section">
          <div className="query-section">
            <FlexibleQueryInput
              ref={queryInputRef}
              queryString={query.getQueryString()}
              onQueryStringChange={handleQueryStringChange}
              runQuerySlice={slice => void handleRun(false, slice)}
              running={executionState.loading}
              columnMetadata={columnMetadata}
              editorStateId={id}
            />
          </div>
          <div className="run-bar">
            <RunPanel
              query={query}
              onQueryChange={onQueryChange}
              onRun={handleRun}
              running={executionState.loading}
              queryEngines={queryEngines}
              clusterCapacity={clusterCapacity}
              defaultQueryContext={{ ...serverQueryContext, ...baseQueryContext }}
              moreMenu={runMoreMenu}
              maxTasksMenuHeader={maxTasksMenuHeader}
              enginesLabelFn={enginesLabelFn}
              maxTasksLabelFn={maxTasksLabelFn}
              maxTasksOptions={maxTasksOptions}
              fullClusterCapacityLabelFn={fullClusterCapacityLabelFn}
              hiddenOptions={hiddenOptions}
            />
            {executionState.isLoading() && (
              <ExecutionTimerPanel
                execution={executionState.intermediate}
                startTime={currentRunningPromise?.startTime}
                onCancel={() => queryManager.cancelCurrent()}
              />
            )}
            {(execution || executionState.error) && (
              <ExecutionSummaryPanel
                execution={execution}
                queryErrorDuration={executionState.error?.queryDuration}
                onExecutionDetail={() => onDetails(execution!)}
                onReset={() => {
                  queryManager.reset();
                  onQueryChange(props.query.changeLastExecution(undefined));
                  ExecutionStateCache.deleteState(id);
                }}
              />
            )}
          </div>
        </div>
        <div className="output-section">
          {executionState.isInit() && (
            <div className="init-placeholder">
              {query.isEmptyQuery() ? (
                <p>
                  Enter a query and click <Code>Run</Code>
                </p>
              ) : (
                <p>
                  Click <Code>Run</Code> to execute the query
                </p>
              )}
            </div>
          )}
          {executionToShow &&
            (executionToShow.error ? (
              <div className="error-container">
                <ExecutionErrorPane execution={executionToShow} />
                {executionToShow.stages && (
                  <ExecutionStagesPane
                    execution={executionToShow}
                    onErrorClick={() => onDetails(executionToShow, 'error')}
                    onWarningClick={() => onDetails(executionToShow, 'warnings')}
                    goToTask={goToTask}
                  />
                )}
              </div>
            ) : executionToShow.result ? (
              <ResultTablePane
                runeMode={executionToShow.engine === 'native'}
                queryResult={executionToShow.result}
                onQueryAction={handleQueryAction}
              />
            ) : executionToShow.isSuccessfulIngest() ? (
              <IngestSuccessPane
                execution={executionToShow}
                onDetails={onDetails}
                onQueryTab={onQueryTab}
              />
            ) : (
              <div className="generic-status-container">
                <div className="generic-status-container-info">
                  {`Execution completed with status: ${executionToShow.status}`}
                </div>
                <ExecutionStagesPane
                  execution={executionToShow}
                  onErrorClick={() => onDetails(executionToShow, 'error')}
                  onWarningClick={() => onDetails(executionToShow, 'warnings')}
                  goToTask={goToTask}
                />
              </div>
            ))}
          {executionState.error && (
            <QueryErrorPane
              error={executionState.error}
              moveCursorTo={position => {
                moveToPosition(position);
              }}
              queryString={query.getQueryString()}
              onQueryStringChange={handleQueryStringChange}
            />
          )}
          {executionState.isLoading() &&
            (executionState.intermediate ? (
              <ExecutionProgressPane
                execution={executionState.intermediate}
                intermediateError={executionState.intermediateError}
                goToTask={goToTask}
                onCancel={onUserCancel}
                allowLiveReportsPane
              />
            ) : (
              <Loader cancelText="Cancel query" onCancel={onUserCancel} />
            ))}
        </div>
      </SplitterLayout>
      {alertElement}
    </div>
  );
});
