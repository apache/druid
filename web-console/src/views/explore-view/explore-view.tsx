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

import './modules';

import { Button, Intent, Menu, MenuDivider, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Column, QueryResult, SqlExpression } from '@druid-toolkit/query';
import { QueryRunner, SqlQuery } from '@druid-toolkit/query';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useStore } from 'zustand';

import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import { useHashAndLocalStorageHybridState, useQueryManager } from '../../hooks';
import { Api, AppToaster } from '../../singletons';
import { DruidError, LocalStorageKeys, queryDruidSql } from '../../utils';

import {
  ControlPane,
  DroppableContainer,
  FilterPane,
  HighlightBubble,
  ModulePane,
  ModulePicker,
  ResourcePane,
  SourcePane,
  SourceQueryPane,
} from './components';
import { ExploreState } from './explore-state';
import { highlightStore } from './highlight-store/highlight-store';
import type { Measure, ParameterValues } from './models';
import { QuerySource } from './models';
import { ModuleRepository } from './module-repository/module-repository';
import { rewriteAggregate, rewriteMaxDataTime } from './query-macros';
import type { Rename } from './utils';
import { adjustTransferValue, normalizeType } from './utils';

import './explore-view.scss';

// ---------------------------------------

interface QueryHistoryEntry {
  time: Date;
  sqlQuery: string;
}

const MAX_PAST_QUERIES = 10;
const QUERY_HISTORY: QueryHistoryEntry[] = [];

function addQueryToHistory(sqlQuery: string): void {
  QUERY_HISTORY.unshift({ time: new Date(), sqlQuery });
  while (QUERY_HISTORY.length > MAX_PAST_QUERIES) QUERY_HISTORY.pop();
}

function getFormattedQueryHistory(): string {
  return QUERY_HISTORY.map(
    ({ time, sqlQuery }) => `At ${time.toISOString()} ran query:\n\n${sqlQuery}`,
  ).join('\n\n-----------------------------------------------------\n\n');
}

// ---------------------------------------

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'fromSqlTypes',
  executor: async (sqlQueryPayload, isSql, cancelToken) => {
    if (!isSql) throw new Error('should never get here');
    addQueryToHistory(sqlQueryPayload.query);
    return Api.instance.post('/druid/v2/sql', sqlQueryPayload, { cancelToken });
  },
});

async function runSqlQuery(query: string | SqlQuery): Promise<QueryResult> {
  try {
    return await queryRunner.runQuery({
      query,
    });
  } catch (e) {
    throw new DruidError(e);
  }
}

async function introspectSource(source: string): Promise<QuerySource> {
  const query = SqlQuery.parse(source);
  const introspectResult = await runSqlQuery(QuerySource.makeLimitZeroIntrospectionQuery(query));

  const baseIntrospectResult = QuerySource.isSingleStarQuery(query)
    ? introspectResult
    : await runSqlQuery(
        QuerySource.makeLimitZeroIntrospectionQuery(QuerySource.stripToBaseSource(query)),
      );

  return QuerySource.fromIntrospectResult(
    query,
    baseIntrospectResult.header,
    introspectResult.header,
  );
}

export const ExploreView = React.memo(function ExploreView() {
  const [shownText, setShownText] = useState<string | undefined>();
  const filterPane = useRef<{ filterOn(column: Column): void }>();
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [exploreState, setExploreState] = useHashAndLocalStorageHybridState<ExploreState>(
    '#explore/v/',
    LocalStorageKeys.EXPLORE_STATE,
    ExploreState.DEFAULT_STATE,
    s => {
      return ExploreState.fromJS(s);
    },
  );

  const { dropHighlight } = useStore(highlightStore);

  // -------------------------------------------------------
  // If no table selected, change to first table if possible
  async function initWithFirstTable() {
    const tables = await queryDruidSql<{ TABLE_NAME: string }>({
      query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE' LIMIT 1`,
    });

    const firstTableName = tables[0].TABLE_NAME;
    if (firstTableName) {
      setTable(firstTableName);
    }
  }

  useEffect(() => {
    if (exploreState.isInitState()) {
      void initWithFirstTable();
    }
  });

  // -------------------------------------------------------

  const { moduleId, source, parsedSource, parseError, where, parameterValues, showSourceQuery } =
    exploreState;
  const module = ModuleRepository.getModule(moduleId);

  const [querySourceState] = useQueryManager<string, QuerySource>({
    query: parsedSource ? String(parsedSource) : undefined,
    processQuery: introspectSource,
  });

  // -------------------------------------------------------
  // If we have a __time::TIMESTAMP column and no filter add a filter

  useEffect(() => {
    const columns = querySourceState.data?.columns;
    if (!columns) return;
    const newExploreState = exploreState.addInitTimeFilterIfNeeded(columns);
    if (exploreState !== newExploreState) {
      setExploreState(newExploreState);
    }
  }, [querySourceState.data]);

  // -------------------------------------------------------

  useEffect(() => {
    const querySource = querySourceState.data;
    if (!querySource || !module) return;
    const newExploreState = exploreState.restrictToQuerySource(querySource);
    if (exploreState !== newExploreState) {
      setExploreState(newExploreState);
    }
  }, [module, parameterValues, querySourceState.data]);

  function setModuleId(moduleId: string, parameterValues: Record<string, any>) {
    if (exploreState.moduleId === moduleId) return;
    setExploreState(exploreState.change({ moduleId, parameterValues }));
  }

  function setParameterValues(newParameterValues: ParameterValues) {
    if (newParameterValues === parameterValues) return;
    setExploreState(exploreState.change({ parameterValues: newParameterValues }));
  }

  function resetParameterValues() {
    setParameterValues({});
  }

  function updateParameterValues(newParameterValues: ParameterValues) {
    setParameterValues({ ...parameterValues, ...newParameterValues });
  }

  function setSource(source: SqlQuery | string, rename?: Rename) {
    setExploreState(exploreState.changeSource(source, rename));
  }

  function setTable(tableName: string) {
    setExploreState(exploreState.changeToTable(tableName));
  }

  function setWhere(where: SqlExpression) {
    setExploreState(exploreState.change({ where }));
  }

  function onShowColumn(column: Column) {
    setExploreState(exploreState.applyShowColumn(column));
  }

  function onShowMeasure(measure: Measure) {
    setExploreState(exploreState.applyShowMeasure(measure));
  }

  function onShowSourceQuery() {
    setExploreState(exploreState.change({ showSourceQuery: true }));
  }

  const querySource = querySourceState.getSomeData();

  const runSqlPlusQuery = useMemo(() => {
    return async (query: string | SqlQuery) => {
      if (!querySource) throw new Error('no querySource');
      return await runSqlQuery(
        await rewriteMaxDataTime(rewriteAggregate(SqlQuery.parse(query), querySource.measures)),
      );
    };
  }, [querySource]);

  return (
    <div className={classNames('explore-view', { 'show-source-query': showSourceQuery })}>
      {showSourceQuery && (
        <SourceQueryPane
          source={source}
          onSourceChange={setSource}
          onClose={() => setExploreState(exploreState.change({ showSourceQuery: false }))}
        />
      )}
      {parseError && (
        <div className="source-error">
          <p>{parseError}</p>
          {source === '' && (
            <p>
              <SourcePane
                selectedSource={undefined}
                onSelectTable={setTable}
                disabled={Boolean(querySource && querySourceState.loading)}
              />
            </p>
          )}
          {!showSourceQuery && (
            <p>
              <Button text="Show source query" onClick={onShowSourceQuery} />
            </p>
          )}
        </div>
      )}
      {parsedSource && (
        <div className="explore-container">
          <SourcePane
            selectedSource={parsedSource}
            onSelectTable={setTable}
            onShowSourceQuery={onShowSourceQuery}
            fill
            minimal
            disabled={Boolean(querySource && querySourceState.loading)}
          />
          <FilterPane
            ref={filterPane}
            querySource={querySource}
            filter={where}
            onFilterChange={setWhere}
            runSqlQuery={runSqlPlusQuery}
            onAddToSourceQueryAsColumn={expression => {
              if (!querySource) return;
              setExploreState(
                exploreState.changeSource(
                  querySource.addColumn(querySource.transformToBaseColumns(expression)),
                  undefined,
                ),
              );
            }}
            onMoveToSourceQueryAsClause={(expression, changeWhere) => {
              if (!querySource) return;
              setExploreState(
                exploreState
                  .change({ where: changeWhere })
                  .changeSource(
                    querySource.addWhereClause(querySource.transformToBaseColumns(expression)),
                    undefined,
                  ),
              );
            }}
          />
          <ModulePicker
            modules={[
              { id: 'grouping-table', icon: IconNames.PANEL_TABLE, label: 'Grouping table' },
              { id: 'record-table', icon: IconNames.TH, label: 'Record table' },
              { id: 'time-chart', icon: IconNames.TIMELINE_LINE_CHART, label: 'Time chart' },
              { id: 'bar-chart', icon: IconNames.VERTICAL_BAR_CHART_DESC, label: 'Bar chart' },
              { id: 'pie-chart', icon: IconNames.PIE_CHART, label: 'Pie chart' },
              { id: 'multi-axis-chart', icon: IconNames.SERIES_ADD, label: 'Multi-axis chart' },
            ]}
            selectedModuleId={moduleId}
            onSelectedModuleIdChange={newModuleId => {
              const newParameterValues: ParameterValues = {};
              const oldModule = ModuleRepository.getModule(moduleId);
              const newModule = ModuleRepository.getModule(newModuleId);
              if (oldModule && newModule) {
                const oldModuleParameters = oldModule.parameters || {};
                const newModuleParameters = newModule.parameters || {};
                for (const paramName in oldModuleParameters) {
                  const parameterValue = parameterValues[paramName];
                  if (typeof parameterValue === 'undefined') continue;

                  const oldParameterDefinition = oldModuleParameters[paramName];
                  const transferGroup = oldParameterDefinition.transferGroup;
                  if (typeof transferGroup !== 'string') continue;

                  const normalizedType = normalizeType(oldParameterDefinition.type);
                  const target = Object.entries(newModuleParameters).find(
                    ([_, def]) =>
                      def.transferGroup === transferGroup &&
                      normalizeType(def.type) === normalizedType,
                  );
                  if (!target) continue;

                  newParameterValues[target[0]] = adjustTransferValue(
                    parameterValue,
                    oldParameterDefinition.type,
                    target[1].type,
                  );
                }
              }

              dropHighlight();
              setModuleId(newModuleId, newParameterValues);
            }}
            moreMenu={
              <Menu>
                <MenuItem
                  icon={IconNames.DUPLICATE}
                  text="Copy last query"
                  disabled={!QUERY_HISTORY.length}
                  onClick={() => {
                    copy(QUERY_HISTORY[0]?.sqlQuery, { format: 'text/plain' });
                    AppToaster.show({
                      message: `Copied query to clipboard`,
                      intent: Intent.SUCCESS,
                    });
                  }}
                />
                <MenuItem
                  icon={IconNames.HISTORY}
                  text="Show query history"
                  onClick={() => {
                    setShownText(getFormattedQueryHistory());
                  }}
                />
                <MenuItem
                  icon={IconNames.RESET}
                  text="Reset visualization parameters"
                  onClick={() => {
                    resetParameterValues();
                  }}
                />
                <MenuDivider />
                <MenuItem
                  icon={IconNames.TRASH}
                  text="Clear all view state"
                  intent={Intent.DANGER}
                  onClick={() => {
                    localStorage.removeItem(LocalStorageKeys.EXPLORE_STATE);
                    location.hash = '#explore';
                    location.reload();
                  }}
                />
              </Menu>
            }
          />
          <div className="resource-pane-cnt">
            {!querySource && querySourceState.loading && 'Loading...'}
            {querySource && (
              <ResourcePane
                querySource={querySource}
                onQueryChange={setSource}
                onFilter={c => {
                  filterPane.current?.filterOn(c);
                }}
                runSqlQuery={runSqlPlusQuery}
                onShowColumn={onShowColumn}
                onShowMeasure={onShowMeasure}
              />
            )}
          </div>
          <DroppableContainer
            className="main-cnt"
            ref={containerRef}
            onDropColumn={onShowColumn}
            onDropMeasure={onShowMeasure}
          >
            {querySourceState.error && (
              <div className="error-display">{querySourceState.getErrorMessage()}</div>
            )}
            {querySource && (
              <ModulePane
                moduleId={moduleId}
                querySource={querySource}
                where={where}
                setWhere={setWhere}
                parameterValues={parameterValues}
                setParameterValues={updateParameterValues}
                runSqlQuery={runSqlPlusQuery}
              />
            )}
          </DroppableContainer>
          <div className="control-pane-cnt">
            {module && (
              <ControlPane
                querySource={querySource}
                onUpdateParameterValues={updateParameterValues}
                parameters={module.parameters}
                parameterValues={parameterValues}
                onAddToSourceQueryAsColumn={expression => {
                  if (!querySource) return;
                  setExploreState(
                    exploreState.changeSource(
                      querySource.addColumn(querySource.transformToBaseColumns(expression)),
                      undefined,
                    ),
                  );
                }}
                onAddToSourceQueryAsMeasure={measure => {
                  if (!querySource) return;
                  setExploreState(
                    exploreState.changeSource(
                      querySource.addMeasure(
                        measure.changeExpression(
                          querySource.transformToBaseColumns(measure.expression),
                        ),
                      ),
                      undefined,
                    ),
                  );
                }}
              />
            )}
          </div>
          {shownText && (
            <ShowValueDialog
              title="Query history"
              str={shownText}
              onClose={() => {
                setShownText(undefined);
              }}
            />
          )}
        </div>
      )}
      <HighlightBubble referenceContainer={containerRef.current} />
    </div>
  );
});
