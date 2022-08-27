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

import { Button, ButtonGroup, InputGroup, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import { QueryResult, QueryRunner, SqlQuery } from 'druid-query-toolkit';
import React, { useEffect, useRef } from 'react';

import { Loader, QueryErrorPane } from '../../../components';
import {
  DruidEngine,
  Execution,
  fitExternalConfigPattern,
  LastExecution,
  QueryContext,
  summarizeExternalConfig,
  WorkbenchQuery,
} from '../../../druid-models';
import {
  executionBackgroundStatusCheck,
  reattachTaskExecution,
  submitTaskQuery,
} from '../../../helpers';
import { usePermanentCallback, useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import { ExecutionStateCache } from '../../../singletons/execution-state-cache';
import { WorkbenchHistory } from '../../../singletons/workbench-history';
import {
  WorkbenchRunningPromise,
  WorkbenchRunningPromises,
} from '../../../singletons/workbench-running-promises';
import { ColumnMetadata, DruidError, QueryAction, QueryManager, RowColumn } from '../../../utils';
import { ExecutionDetailsTab } from '../execution-details-pane/execution-details-pane';
import { ExecutionErrorPane } from '../execution-error-pane/execution-error-pane';
import { ExecutionProgressPane } from '../execution-progress-pane/execution-progress-pane';
import { ExecutionStagesPane } from '../execution-stages-pane/execution-stages-pane';
import { ExecutionSummaryPanel } from '../execution-summary-panel/execution-summary-panel';
import { ExecutionTimerPanel } from '../execution-timer-panel/execution-timer-panel';
import { FlexibleQueryInput } from '../flexible-query-input/flexible-query-input';
import { IngestSuccessPane } from '../ingest-success-pane/ingest-success-pane';
import { useMetadataStateStore } from '../metadata-state-store';
import { ResultTablePane } from '../result-table-pane/result-table-pane';
import { RunPanel } from '../run-panel/run-panel';
import { useWorkStateStore } from '../work-state-store';

import './helper-query.scss';

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'none',
});

export interface HelperQueryProps {
  query: WorkbenchQuery;
  mandatoryQueryContext: QueryContext | undefined;
  columnMetadata: readonly ColumnMetadata[] | undefined;
  onQueryChange(newQuery: WorkbenchQuery): void;
  onQueryTab(newQuery: WorkbenchQuery, tabName?: string): void;
  onDelete(): void;
  onDetails(id: string, initTab?: ExecutionDetailsTab): void;
  queryEngines: DruidEngine[];
  goToIngestion(taskId: string): void;
}

export const HelperQuery = React.memo(function HelperQuery(props: HelperQueryProps) {
  const {
    query,
    columnMetadata,
    mandatoryQueryContext,
    onQueryChange,
    onQueryTab,
    onDelete,
    onDetails,
    queryEngines,
    goToIngestion,
  } = props;
  const handleQueryStringChange = usePermanentCallback((queryString: string) => {
    onQueryChange(query.changeQueryString(queryString));
  });

  const parsedQuery = query.getParsedQuery();
  const handleQueryAction = usePermanentCallback((queryAction: QueryAction) => {
    if (!(parsedQuery instanceof SqlQuery)) return;
    onQueryChange(query.changeQueryString(parsedQuery.apply(queryAction).toString()));

    if (shouldAutoRun()) {
      setTimeout(() => handleRun(false), 20);
    }
  });

  function shouldAutoRun(): boolean {
    if (query.getEffectiveEngine() !== 'sql-native') return false;
    const queryDuration = executionState.data?.result?.queryDuration;
    return Boolean(queryDuration && queryDuration < 10000);
  }

  const queryInputRef = useRef<FlexibleQueryInput | null>(null);

  const id = query.getId();
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

        const { engine, query, sqlPrefixLines, cancelQueryId } = q.getApiQuery();

        switch (engine) {
          case 'sql-msq-task':
            return await submitTaskQuery({
              query,
              prefixLines: sqlPrefixLines,
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
              });
              WorkbenchRunningPromises.storePromise(id, { promise: resultPromise, sqlPrefixLines });

              result = await resultPromise;
            } catch (e) {
              throw new DruidError(e, sqlPrefixLines);
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
          throw new DruidError(e, q.sqlPrefixLines);
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

  const incrementWorkVersion = useWorkStateStore(state => state.increment);
  useEffect(() => {
    incrementWorkVersion();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionState.loading, Boolean(executionState.intermediate)]);

  const execution = executionState.data;

  const incrementMetadataVersion = useMetadataStateStore(state => state.increment);
  useEffect(() => {
    if (execution?.isSuccessfulInsert()) {
      incrementMetadataVersion();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [Boolean(execution?.isSuccessfulInsert())]);

  function moveToPosition(position: RowColumn) {
    const currentQueryInput = queryInputRef.current;
    if (!currentQueryInput) return;
    currentQueryInput.goToPosition(position);
  }

  const handleRun = usePermanentCallback((preview: boolean) => {
    if (!query.isValid()) return;

    WorkbenchHistory.addQueryToHistory(query);
    queryManager.runQuery(preview ? query.makePreview() : query);
  });

  const collapsed = query.getCollapsed();
  const insertDatasource = query.getIngestDatasource();

  const statsTaskId: string | undefined = execution?.id;

  let extraInfo: string | undefined;
  if (collapsed && parsedQuery instanceof SqlQuery) {
    try {
      extraInfo = summarizeExternalConfig(fitExternalConfigPattern(parsedQuery));
    } catch {}
  }

  return (
    <div className="helper-query">
      <div className="query-top-bar">
        <Button
          icon={collapsed ? IconNames.CARET_RIGHT : IconNames.CARET_DOWN}
          minimal
          onClick={() => onQueryChange(query.changeCollapsed(!collapsed))}
        />
        {insertDatasource ? (
          `<insert query : ${insertDatasource}>`
        ) : (
          <>
            {collapsed ? (
              <span className="query-name">{query.getQueryName()}</span>
            ) : (
              <InputGroup
                className="query-name"
                value={query.getQueryName()}
                onChange={(e: any) => {
                  onQueryChange(query.changeQueryName(e.target.value));
                }}
              />
            )}
            <span className="as-label">AS</span>
            {extraInfo && <span className="extra-info">{extraInfo}</span>}
          </>
        )}
        <ButtonGroup className="corner">
          <Popover2
            content={
              <Menu>
                <MenuItem
                  icon={IconNames.DUPLICATE}
                  text="Duplicate"
                  onClick={() => onQueryChange(query.duplicateLast())}
                />
              </Menu>
            }
          >
            <Button icon={IconNames.MORE} minimal />
          </Popover2>
          <Button
            icon={IconNames.CROSS}
            minimal
            onClick={() => {
              ExecutionStateCache.deleteState(id);
              WorkbenchRunningPromises.deletePromise(id);
              onDelete();
            }}
          />
        </ButtonGroup>
      </div>
      {!collapsed && (
        <>
          <FlexibleQueryInput
            ref={queryInputRef}
            autoHeight
            queryString={query.getQueryString()}
            onQueryStringChange={handleQueryStringChange}
            columnMetadata={
              columnMetadata ? columnMetadata.concat(query.getInlineMetadata()) : undefined
            }
            editorStateId={query.getId()}
          />
          <div className="query-control-bar">
            <RunPanel
              query={query}
              onQueryChange={onQueryChange}
              onRun={handleRun}
              loading={executionState.loading}
              small
              queryEngines={queryEngines}
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
          {!executionState.isInit() && (
            <div className="output-pane">
              {execution &&
                (execution.result ? (
                  <ResultTablePane
                    runeMode={execution.engine === 'native'}
                    queryResult={execution.result}
                    onQueryAction={handleQueryAction}
                    initPageSize={5}
                  />
                ) : execution.isSuccessfulInsert() ? (
                  <IngestSuccessPane
                    execution={execution}
                    onDetails={onDetails}
                    onQueryTab={onQueryTab}
                  />
                ) : execution.error ? (
                  <div className="error-container">
                    <ExecutionErrorPane execution={execution} />
                    {execution.stages && (
                      <ExecutionStagesPane
                        execution={execution}
                        onErrorClick={() => onDetails(statsTaskId!, 'error')}
                        onWarningClick={() => onDetails(statsTaskId!, 'warnings')}
                        goToIngestion={goToIngestion}
                      />
                    )}
                  </div>
                ) : (
                  <div>Unknown query execution state</div>
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
                    goToIngestion={goToIngestion}
                    onCancel={() => {
                      queryManager.cancelCurrent();
                    }}
                  />
                ) : (
                  <Loader
                    cancelText="Cancel query"
                    onCancel={() => {
                      queryManager.cancelCurrent();
                    }}
                  />
                ))}
            </div>
          )}
        </>
      )}
    </div>
  );
});
