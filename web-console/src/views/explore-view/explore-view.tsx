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

import {
  Button,
  ButtonGroup,
  Icon,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';
import type { CancelToken } from 'axios';
import { Timezone } from 'chronoshift';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import type { Column, QueryResult, SqlExpression } from 'druid-query-toolkit';
import { QueryRunner, SqlLiteral, SqlQuery } from 'druid-query-toolkit';
import React, { useEffect, useMemo, useRef, useState } from 'react';

import { Loader, SplitterLayout, TimezoneMenuItems } from '../../components';
import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import { useHashAndLocalStorageHybridState, useQueryManager } from '../../hooks';
import { Api, AppToaster } from '../../singletons';
import { capitalizeFirst, DruidError, LocalStorageKeys, queryDruidSql } from '../../utils';

import {
  DroppableContainer,
  FilterPane,
  HelperTable,
  ModulePane,
  ResourcePane,
  SourcePane,
  SourceQueryPane,
} from './components';
import type { ExploreModuleLayout, Measure, ModuleState } from './models';
import { ExploreState, ExpressionMeta, QuerySource } from './models';
import { rewriteAggregate, rewriteMaxDataTime } from './query-macros';
import type { Rename } from './utils';
import { QueryLog } from './utils';

import './explore-view.scss';

const QUERY_LOG = new QueryLog();

const LAYOUT_TO_ICON: Record<ExploreModuleLayout, IconName> = {
  'single': IconNames.SYMBOL_RECTANGLE,
  'two-by-two': IconNames.GRID_VIEW,
  'two-rows': IconNames.LAYOUT_TWO_ROWS,
  'two-columns': IconNames.LAYOUT_TWO_COLUMNS,
  'three-rows': IconNames.LAYOUT_THREE_ROWS,
  'three-columns': IconNames.LAYOUT_THREE_COLUMNS,
  'top-row-two-tiles': IconNames.LAYOUT_TOP_ROW_TWO_TILES,
  'bottom-row-two-tiles': IconNames.LAYOUT_BOTTOM_ROW_TWO_TILES,
  'left-column-two-tiles': IconNames.LAYOUT_LEFT_COLUMN_TWO_TILES,
  'right-column-two-tiles': IconNames.LAYOUT_RIGHT_COLUMN_TWO_TILES,
  'top-row-three-tiles': IconNames.LAYOUT_TOP_ROW_THREE_TILES,
  'bottom-row-three-tiles': IconNames.LAYOUT_BOTTOM_ROW_THREE_TILES,
  'left-column-three-tiles': IconNames.LAYOUT_LEFT_COLUMN_THREE_TILES,
  'right-column-three-tiles': IconNames.LAYOUT_RIGHT_COLUMN_THREE_TILES,
};

// ---------------------------------------

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'fromSqlTypes',
  executor: async (sqlQueryPayload, isSql, cancelToken) => {
    if (!isSql) throw new Error('should never get here');
    QUERY_LOG.addQuery(sqlQueryPayload.query);
    return Api.instance.post('/druid/v2/sql', sqlQueryPayload, { cancelToken });
  },
});

async function runSqlQuery(
  query: string | SqlQuery,
  timezone: Timezone | undefined,
  cancelToken?: CancelToken,
): Promise<QueryResult> {
  try {
    return await queryRunner.runQuery({
      query,
      defaultQueryContext: {
        sqlStringifyArrays: false,
      },
      extraQueryContext: timezone ? { sqlTimeZone: timezone.toString() } : undefined,
      cancelToken,
    });
  } catch (e) {
    throw new DruidError(e);
  }
}

async function introspectSource(source: string, cancelToken?: CancelToken): Promise<QuerySource> {
  const query = SqlQuery.parse(source);
  const introspectResult = await runSqlQuery(
    QuerySource.makeLimitZeroIntrospectionQuery(query),
    undefined,
    cancelToken,
  );

  cancelToken?.throwIfRequested();
  const baseIntrospectResult = QuerySource.isSingleStarQuery(query)
    ? introspectResult
    : await runSqlQuery(
        QuerySource.makeLimitZeroIntrospectionQuery(QuerySource.stripToBaseSource(query)),
        undefined,
        cancelToken,
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
    s => ExploreState.fromJS(s),
  );

  // -------------------------------------------------------
  // If no table selected, change to first table if possible
  async function initializeWithFirstTable() {
    const tables = await queryDruidSql<{ TABLE_NAME: string }>({
      query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE' LIMIT 1`,
    });

    const firstTableName = tables[0].TABLE_NAME;
    if (firstTableName) {
      setExploreState(exploreState.initToTable(firstTableName));
    }
  }

  useEffect(() => {
    if (exploreState.isInitState()) {
      void initializeWithFirstTable();
    }
  });

  // -------------------------------------------------------

  const { parsedSource } = exploreState;

  const [querySourceState] = useQueryManager<string, QuerySource>({
    query: parsedSource ? String(parsedSource) : undefined,
    processQuery: introspectSource,
  });

  // -------------------------------------------------------
  // If we have a TIMESTAMP column and no filter add a filter

  useEffect(() => {
    const columns = querySourceState.data?.columns;
    if (!columns) return;
    const newExploreState = exploreState.addInitTimeFilterIfNeeded(columns);
    if (exploreState !== newExploreState) {
      setExploreState(newExploreState);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [querySourceState.data]);

  // -------------------------------------------------------

  const effectiveExploreState = useMemo(
    () =>
      querySourceState.data
        ? exploreState.restrictToQuerySource(querySourceState.data)
        : exploreState,
    [exploreState, querySourceState.data],
  );

  const { source, parseError, where, showSourceQuery, hideResources, hideHelpers } =
    effectiveExploreState;
  const timezone = effectiveExploreState.getEffectiveTimezone();

  function setModuleState(index: number, moduleState: ModuleState) {
    setExploreState(effectiveExploreState.changeModuleState(index, moduleState));
  }

  function setSource(source: SqlQuery | string, rename?: Rename) {
    setExploreState(effectiveExploreState.changeSource(source, rename));
  }

  function setTable(tableName: string) {
    setExploreState(effectiveExploreState.changeToTable(tableName));
  }

  function setWhere(where: SqlExpression) {
    setExploreState(effectiveExploreState.change({ where }));
  }

  function onShowColumn(column: Column) {
    setExploreState(effectiveExploreState.applyShowColumn(column, undefined));
  }

  function onShowMeasure(measure: Measure) {
    setExploreState(effectiveExploreState.applyShowMeasure(measure, undefined));
  }

  function onShowSourceQuery() {
    setExploreState(effectiveExploreState.change({ showSourceQuery: true }));
  }

  const querySource = querySourceState.getSomeData();

  const runSqlPlusQuery = useMemo(() => {
    return async (
      query: string | SqlQuery | { query: string | SqlQuery; timezone?: Timezone },
      cancelToken?: CancelToken,
    ) => {
      if (!querySource) throw new Error('no querySource');
      let parsedQuery: SqlQuery;
      let queryTimezone: Timezone;
      if (typeof query === 'string' || query instanceof SqlQuery) {
        parsedQuery = SqlQuery.parse(query);
        queryTimezone = timezone;
      } else if (query.query) {
        parsedQuery = SqlQuery.parse(query.query);
        queryTimezone = query.timezone || timezone;
      } else {
        throw new TypeError('invalid arguments');
      }

      const { query: rewrittenQuery, maxTime } = await rewriteMaxDataTime(
        rewriteAggregate(parsedQuery, querySource.measures),
      );
      const results = await runSqlQuery(rewrittenQuery, queryTimezone, cancelToken);

      return results
        .attachQuery({ query: '' }, parsedQuery)
        .changeResultContext({ ...results.resultContext, maxTime });
    };
  }, [querySource, timezone]);

  const selectedLayout = effectiveExploreState.getLayout();
  return (
    <div className="explore-view">
      <SplitterLayout
        className="source-query-module-splitter"
        vertical
        primaryIndex={1}
        secondaryInitialSize={200}
        secondaryMinSize={150}
        primaryMinSize={400}
        splitterSize={8}
      >
        {showSourceQuery && (
          <SourceQueryPane
            source={source}
            onSourceChange={setSource}
            onClose={() =>
              setExploreState(effectiveExploreState.change({ showSourceQuery: false }))
            }
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
          <div className="filter-explore-wrapper">
            <div className="filter-pane-container">
              {!showSourceQuery && (
                <div className="source-pane-container">
                  <SourcePane
                    selectedSource={parsedSource}
                    onSelectTable={setTable}
                    onShowSourceQuery={onShowSourceQuery}
                    fill
                    minimal
                    disabled={Boolean(querySource && querySourceState.loading)}
                  />
                </div>
              )}
              <FilterPane
                ref={filterPane}
                querySource={querySource}
                extraFilter={SqlLiteral.TRUE}
                timezone={timezone}
                filter={where}
                onFilterChange={setWhere}
                runSqlQuery={runSqlPlusQuery}
                onAddToSourceQueryAsColumn={expression => {
                  if (!querySource) return;
                  setExploreState(
                    effectiveExploreState.changeSource(
                      querySource.addColumn(
                        querySource.transformExpressionToBaseColumns(expression),
                      ),
                      undefined,
                    ),
                  );
                }}
                onMoveToSourceQueryAsClause={(expression, changeWhere) => {
                  if (!querySource) return;
                  setExploreState(
                    effectiveExploreState
                      .change({ where: changeWhere })
                      .changeSource(
                        querySource.addWhereClause(
                          querySource.transformExpressionToBaseColumns(expression),
                        ),
                        undefined,
                      ),
                  );
                }}
              />
              <ButtonGroup className="action-buttons">
                <Popover
                  position={Position.BOTTOM_RIGHT}
                  content={
                    <Menu>
                      <MenuItem
                        icon={IconNames.GLOBE_NETWORK}
                        text="Timezone"
                        label={
                          effectiveExploreState.timezone
                            ? effectiveExploreState.timezone.toString()
                            : 'Etc/UTC'
                        }
                      >
                        <TimezoneMenuItems
                          sqlTimeZone={effectiveExploreState.timezone?.toString()}
                          setSqlTimeZone={timezone =>
                            setExploreState(
                              effectiveExploreState.changeTimezone(
                                timezone ? Timezone.fromJS(timezone) : undefined,
                              ),
                            )
                          }
                          defaultSqlTimeZone="Etc/UTC"
                          namedOnly
                        />
                      </MenuItem>
                      <MenuDivider />
                      <MenuItem
                        icon={IconNames.DUPLICATE}
                        text="Copy last query"
                        disabled={!QUERY_LOG.length()}
                        onClick={() => {
                          copy(QUERY_LOG.getLastQuery()!, { format: 'text/plain' });
                          AppToaster.show({
                            message: `Copied query to clipboard`,
                            intent: Intent.SUCCESS,
                          });
                        }}
                      />
                      <MenuItem
                        icon={IconNames.HISTORY}
                        text="Show query log"
                        onClick={() => {
                          setShownText(QUERY_LOG.getFormatted());
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
                >
                  <Button icon={IconNames.MORE} data-tooltip="More options" minimal />
                </Popover>
                <Popover
                  content={
                    <Menu>
                      {ExploreState.LAYOUTS.map(layout => (
                        <MenuItem
                          key={layout}
                          icon={LAYOUT_TO_ICON[layout]}
                          text={capitalizeFirst(layout.replace(/-/g, ' '))}
                          labelElement={
                            selectedLayout === layout ? <Icon icon={IconNames.TICK} /> : undefined
                          }
                          onClick={() => {
                            setExploreState(effectiveExploreState.change({ layout }));
                          }}
                        />
                      ))}
                    </Menu>
                  }
                >
                  <Button icon={IconNames.CONTROL} data-tooltip="Layout" minimal />
                </Popover>
                <Button
                  icon={IconNames.PANEL_STATS}
                  data-tooltip="Show/hide side panels"
                  minimal
                  onClick={e => {
                    if (e.altKey) {
                      setExploreState(
                        effectiveExploreState.change({ hideResources: !hideResources }),
                      );
                    } else {
                      setExploreState(effectiveExploreState.change({ hideHelpers: !hideHelpers }));
                    }
                  }}
                />
              </ButtonGroup>
            </div>
            <SplitterLayout
              className="resource-explore-splitter"
              primaryIndex={1}
              secondaryInitialSize={250}
              secondaryMinSize={250}
              secondaryMaxSize={500}
              splitterSize={8}
            >
              {!hideResources && (
                <div className="resource-pane-cnt">
                  {!querySource && querySourceState.loading && 'Loading...'}
                  {querySource && (
                    <ResourcePane
                      querySource={querySource}
                      onQueryChange={setSource}
                      where={where}
                      onFilter={c => {
                        filterPane.current?.filterOn(c);
                      }}
                      runSqlQuery={runSqlPlusQuery}
                      onShowColumn={onShowColumn}
                      onShowMeasure={onShowMeasure}
                    />
                  )}
                </div>
              )}
              <SplitterLayout
                className="module-helpers-splitter"
                secondaryInitialSize={250}
                secondaryMinSize={250}
                splitterSize={8}
              >
                {querySourceState.error ? (
                  <div className="query-source-error">{querySourceState.getErrorMessage()}</div>
                ) : querySource ? (
                  <div
                    className={classNames('modules-pane', `layout-${selectedLayout}`)}
                    ref={containerRef}
                  >
                    {effectiveExploreState.getModuleStatesToShow().map((moduleState, i) =>
                      moduleState ? (
                        <ModulePane
                          key={i}
                          className={`m${i}`}
                          moduleState={moduleState}
                          setModuleState={moduleState => setModuleState(i, moduleState)}
                          onDelete={() => setExploreState(effectiveExploreState.removeModule(i))}
                          querySource={querySource}
                          timezone={timezone}
                          where={where}
                          setWhere={setWhere}
                          runSqlQuery={runSqlPlusQuery}
                          onAddToSourceQueryAsColumn={expression => {
                            if (!querySource) return;
                            setExploreState(
                              effectiveExploreState.changeSource(
                                querySource.addColumn(
                                  querySource.transformExpressionToBaseColumns(expression),
                                ),
                                undefined,
                              ),
                            );
                          }}
                          onAddToSourceQueryAsMeasure={measure => {
                            if (!querySource) return;
                            setExploreState(
                              effectiveExploreState.changeSource(
                                querySource.addMeasure(
                                  measure.changeExpression(
                                    querySource.transformExpressionToBaseColumns(
                                      measure.expression,
                                    ),
                                  ),
                                ),
                                undefined,
                              ),
                            );
                          }}
                        />
                      ) : (
                        <DroppableContainer
                          key={i}
                          className={`no-module-placeholder m${i}`}
                          onDropColumn={column =>
                            setExploreState(effectiveExploreState.applyShowColumn(column, i))
                          }
                          onDropMeasure={measure =>
                            setExploreState(effectiveExploreState.applyShowMeasure(measure, i))
                          }
                        >
                          <span>Drag and drop a column or measure here</span>
                        </DroppableContainer>
                      ),
                    )}
                  </div>
                ) : querySourceState.loading ? (
                  <Loader
                    className="query-source-loader"
                    loadingText="Introspecting query source"
                  />
                ) : (
                  'should never get here'
                )}
                {!hideHelpers && (
                  <DroppableContainer
                    className="helper-bar"
                    onDropColumn={c =>
                      setExploreState(effectiveExploreState.addHelper(ExpressionMeta.fromColumn(c)))
                    }
                  >
                    {querySource && effectiveExploreState.helpers.length > 0 && (
                      <div className="helper-tables">
                        {effectiveExploreState.helpers.map((ex, i) => (
                          <HelperTable
                            key={i}
                            querySource={querySource}
                            where={where}
                            setWhere={setWhere}
                            expression={ex}
                            runSqlQuery={runSqlPlusQuery}
                            onDelete={() => setExploreState(effectiveExploreState.removeHelper(i))}
                          />
                        ))}
                      </div>
                    )}
                    {!effectiveExploreState.helpers.length && (
                      <div className="no-helper-message"> Drag columns here to see helpers</div>
                    )}
                  </DroppableContainer>
                )}
              </SplitterLayout>
            </SplitterLayout>
          </div>
        )}
      </SplitterLayout>
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
  );
});
