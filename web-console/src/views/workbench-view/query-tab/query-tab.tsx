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
import type { QueryResult } from '@druid-toolkit/query';
import { QueryRunner, SqlQuery } from '@druid-toolkit/query';
import axios from 'axios';
import type { JSX } from 'react';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import SplitterLayout from 'react-splitter-layout';
import { useStore } from 'zustand';

import { Loader, QueryErrorPane } from '../../../components';
import type { DruidEngine, LastExecution, QueryContext } from '../../../druid-models';
import { Execution, WorkbenchQuery } from '../../../druid-models';
import {
  executionBackgroundStatusCheck,
  maybeGetClusterCapacity,
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
  DruidError,
  findAllSqlQueriesInText,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
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
import { RunPanel } from '../run-panel/run-panel';
import { workStateStore } from '../work-state-store';

import './query-tab.scss';

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'none',
});

export interface QueryTabProps {
  query: WorkbenchQuery;
  id: string;
  mandatoryQueryContext: QueryContext | undefined;
  columnMetadata: readonly ColumnMetadata[] | undefined;
  onQueryChange(newQuery: WorkbenchQuery): void;
  onQueryTab(newQuery: WorkbenchQuery, tabName?: string): void;
  onDetails(id: string, initTab?: ExecutionDetailsTab): void;
  queryEngines: DruidEngine[];
  runMoreMenu: JSX.Element;
  clusterCapacity: number | undefined;
  goToTask(taskId: string): void;
}

export const QueryTab = React.memo(function QueryTab(props: QueryTabProps) {
  const {
    query,
    id,
    columnMetadata,
    mandatoryQueryContext,
    onQueryChange,
    onQueryTab,
    onDetails,
    queryEngines,
    runMoreMenu,
    clusterCapacity,
    goToTask,
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
    if (query.getEffectiveEngine() !== 'sql-native') return false;
    const queryDuration = executionState.data?.result?.queryDuration;
    return Boolean(queryDuration && queryDuration < 10000);
  }

  const handleSecondaryPaneSizeChange = useCallback((secondaryPaneSize: number) => {
    localStorageSet(LocalStorageKeys.WORKBENCH_PANE_SIZE, String(secondaryPaneSize));
  }, []);

  const queryInputRef = useRef<FlexibleQueryInput | null>(null);

  const [executionState, queryManager] = useQueryManager<
    WorkbenchQuery | WorkbenchRunningPromise | LastExecution,
    Execution,
    Execution,
    DruidError
  >({
    initQuery: ExecutionStateCache.getState(id)
      ? undefined
      : WorkbenchRunningPromises.getPromise(id) || query.getLastExecution(),
    initState: ExecutionStateCache.getState(id),
    processQuery: async (q, cancelToken) => {
      if (q instanceof WorkbenchQuery) {
        ExecutionStateCache.deleteState(id);
        const { engine, query, prefixLines, cancelQueryId } = q.getApiQuery();

        switch (engine) {
          case 'sql-msq-task':
            return await submitTaskQuery({
              query,
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

            let result: QueryResult;
            try {
              const resultPromise = queryRunner.runQuery({
                query,
                extraQueryContext: mandatoryQueryContext,
                cancelToken: new axios.CancelToken(cancelFn => {
                  nativeQueryCancelFnRef.current = cancelFn;
                }),
              });
              WorkbenchRunningPromises.storePromise(id, { promise: resultPromise, prefixLines });

              result = await resultPromise;
              nativeQueryCancelFnRef.current = undefined;
            } catch (e) {
              nativeQueryCancelFnRef.current = undefined;
              throw new DruidError(e, prefixLines);
            }

            return Execution.fromResult(engine, result);
          }
        }
      } else if (WorkbenchRunningPromises.isWorkbenchRunningPromise(q)) {
        let result: QueryResult;
        try {
          result = await q.promise;
        } catch (e) {
          WorkbenchRunningPromises.deletePromise(id);
          throw new DruidError(e, q.prefixLines);
        }

        WorkbenchRunningPromises.deletePromise(id);
        return Execution.fromResult('sql-native', result);
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
    if (!executionState.data) return;
    ExecutionStateCache.storeState(id, executionState);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionState.data, executionState.error]);

  const incrementWorkVersion = useStore(
    workStateStore,
    useCallback(state => state.increment, []),
  );
  useEffect(() => {
    incrementWorkVersion();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionState.loading, Boolean(executionState.intermediate)]);

  const execution = executionState.data;

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

      const capacityInfo = await maybeGetClusterCapacity();

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

  const statsTaskId: string | undefined = execution?.id;

  const onUserCancel = (message?: string) => {
    queryManager.cancelCurrent(message);
    nativeQueryCancelFnRef.current?.();
  };

  return (
    <div className="query-tab">
      <SplitterLayout
        vertical
        percentage
        secondaryInitialSize={Number(localStorageGet(LocalStorageKeys.WORKBENCH_PANE_SIZE)!) || 40}
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
              moreMenu={runMoreMenu}
            />
            {executionState.isLoading() && (
              <ExecutionTimerPanel
                execution={executionState.intermediate}
                onCancel={() => queryManager.cancelCurrent()}
              />
            )}
            {(execution || executionState.error) && (
              <ExecutionSummaryPanel
                execution={execution}
                onExecutionDetail={() => onDetails(statsTaskId!)}
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
          {execution &&
            (execution.result ? (
              <ResultTablePane
                runeMode={execution.engine === 'native'}
                queryResult={execution.result}
                onQueryAction={handleQueryAction}
              />
            ) : execution.error ? (
              <div className="error-container">
                <ExecutionErrorPane execution={execution} />
                {execution.stages && (
                  <ExecutionStagesPane
                    execution={execution}
                    onErrorClick={() => onDetails(statsTaskId!, 'error')}
                    onWarningClick={() => onDetails(statsTaskId!, 'warnings')}
                    goToTask={goToTask}
                  />
                )}
              </div>
            ) : execution.isSuccessfulIngest() ? (
              <IngestSuccessPane
                execution={execution}
                onDetails={onDetails}
                onQueryTab={onQueryTab}
              />
            ) : (
              <div className="generic-status-container">
                <div className="generic-status-container-info">
                  {`Execution completed with status: ${execution.status}`}
                </div>
                <ExecutionStagesPane
                  execution={execution}
                  onErrorClick={() => onDetails(statsTaskId!, 'error')}
                  onWarningClick={() => onDetails(statsTaskId!, 'warnings')}
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
