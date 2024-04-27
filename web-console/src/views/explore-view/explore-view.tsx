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

import { Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { SqlExpression, SqlTable } from '@druid-toolkit/query';
import { C, L, sql, SqlLiteral, SqlQuery, T } from '@druid-toolkit/query';
import type { ExpressionMeta, TransferValue } from '@druid-toolkit/visuals-core';
import {
  useModuleContainer,
  useParameterValues,
  useSingleHost,
} from '@druid-toolkit/visuals-react';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useStore } from 'zustand';

import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import { useLocalStorageState, useQueryManager } from '../../hooks';
import { deepGet, filterMap, findMap, LocalStorageKeys, oneOf, queryDruidSql } from '../../utils';

import { ControlPane } from './control-pane/control-pane';
import { DroppableContainer } from './droppable-container/droppable-container';
import { FilterPane } from './filter-pane/filter-pane';
import { HighlightBubble } from './highlight-bubble/highlight-bubble';
import { highlightStore } from './highlight-store/highlight-store';
import BarChartEcharts from './modules/bar-chart-echarts-module';
import MultiAxisChartEcharts from './modules/multi-axis-chart-echarts-module';
import PieChartEcharts from './modules/pie-chart-echarts-module';
import TableReact from './modules/table-react-module';
import TimeChartEcharts from './modules/time-chart-echarts-module';
import { ResourcePane } from './resource-pane/resource-pane';
import { SourcePane } from './source-pane/source-pane';
import { TilePicker } from './tile-picker/tile-picker';
import type { Dataset } from './utils';
import { adjustTransferValue, normalizeType } from './utils';

import './explore-view.scss';

const VISUAL_MODULES = [
  {
    moduleName: 'time_chart_echarts',
    icon: IconNames.TIMELINE_LINE_CHART,
    label: 'Time chart',
    module: TimeChartEcharts,
    transfer: ['splitColumn', 'metric'],
  },
  {
    moduleName: 'bar_chart_echarts',
    icon: IconNames.TIMELINE_BAR_CHART,
    label: 'Bar chart',
    module: BarChartEcharts,
    transfer: ['splitColumn', 'metric'],
  },
  {
    moduleName: 'table_react',
    icon: IconNames.TH,
    label: 'Table',
    module: TableReact,
    transfer: ['splitColumns', 'metrics'],
  },
  {
    moduleName: 'pie_chart_echarts',
    icon: IconNames.PIE_CHART,
    label: 'Pie chart',
    module: PieChartEcharts,
    transfer: ['splitColumn', 'metric'],
  },
  {
    moduleName: 'multi-axis_chart_echarts',
    icon: IconNames.SERIES_ADD,
    label: 'Multi-axis chart',
    module: MultiAxisChartEcharts,
    transfer: ['metrics'],
  },
] as const;

type ModuleType = (typeof VISUAL_MODULES)[number]['moduleName'];

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

async function introspect(tableName: SqlTable): Promise<Dataset> {
  const columns = await queryDruidSql({
    query: `SELECT COLUMN_NAME AS "name", DATA_TYPE AS "sqlType" FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = ${L(tableName.getName())}`,
  });

  return {
    table: tableName,
    columns: columns.map(({ name, sqlType }) => ({ name, expression: C(name), sqlType })),
  };
}

// micro-cache
const MAX_TIME_TTL = 60000;
let lastMaxTimeTable: string | undefined;
let lastMaxTimeValue: Date | undefined;
let lastMaxTimeTimestamp = 0;

async function getMaxTimeForTable(tableName: string): Promise<Date | undefined> {
  // micro-cache get
  if (
    lastMaxTimeTable === tableName &&
    lastMaxTimeValue &&
    Date.now() < lastMaxTimeTimestamp + MAX_TIME_TTL
  ) {
    return lastMaxTimeValue;
  }

  const d = await queryDruidSql({
    query: sql`SELECT MAX(__time) AS "maxTime" FROM ${T(tableName)}`,
  });

  const maxTime = new Date(deepGet(d, '0.maxTime'));
  if (isNaN(maxTime.valueOf())) return;

  // micro-cache set
  lastMaxTimeTable = tableName;
  lastMaxTimeValue = maxTime;
  lastMaxTimeTimestamp = Date.now();

  return maxTime;
}

function getFirstTableName(q: SqlQuery): string | undefined {
  return (
    findMap(q.getWithParts(), withPart => {
      if (!(withPart.query instanceof SqlQuery)) return;
      return getFirstTableName(withPart.query);
    }) ?? q.getFirstTableName()
  );
}

async function extendedQueryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]> {
  if (sqlQueryPayload.query.includes('MAX_DATA_TIME()')) {
    const parsed = SqlQuery.parse(sqlQueryPayload.query);
    const tableName = getFirstTableName(parsed);
    if (tableName) {
      const maxTime = await getMaxTimeForTable(tableName);
      if (maxTime) {
        sqlQueryPayload = {
          ...sqlQueryPayload,
          query: sqlQueryPayload.query.replace(/MAX_DATA_TIME\(\)/g, L(maxTime)),
        };
      }
    }
  }

  addQueryToHistory(sqlQueryPayload.query);
  console.debug(`Running query:\n${sqlQueryPayload.query}`);

  return queryDruidSql(sqlQueryPayload);
}

export const ExploreView = React.memo(function ExploreView() {
  const [shownText, setShownText] = useState<string | undefined>();
  const filterPane = useRef<{ filterOn(column: ExpressionMeta): void }>();

  const [moduleName, setModuleName] = useLocalStorageState<ModuleType>(
    LocalStorageKeys.EXPLORE_CONTENT,
    VISUAL_MODULES[0].moduleName,
  );

  const { dropHighlight } = useStore(highlightStore);

  const [timezone] = useState('Etc/UTC');

  const [columns, setColumns] = useState<ExpressionMeta[]>([]);

  const { host, where, table, visualModule, updateWhere, updateTable } = useSingleHost({
    sqlQuery: extendedQueryDruidSql,
    persist: { name: LocalStorageKeys.EXPLORE_ESSENCE, storage: 'localStorage' },
    visualModules: Object.fromEntries(VISUAL_MODULES.map(v => [v.moduleName, v.module])),
    selectedModule: moduleName,
    moduleState: {
      parameterValues: {},
      table: T('select source'),
      where: SqlLiteral.TRUE,
    },
  });

  useEffect(() => {
    host.store.setState({ context: { timezone } });
  }, [timezone, host.store]);

  const { parameterValues, updateParameterValues, resetParameterValues } = useParameterValues({
    host,
    selectedModule: moduleName,
    columns,
  });

  const [datasetState] = useQueryManager<SqlExpression, Dataset>({
    query: table,
    processQuery: tableName => introspect(tableName as SqlTable),
  });

  const onShow = useMemo(() => {
    const currentShowTransfers =
      VISUAL_MODULES.find(vm => vm.moduleName === moduleName)?.transfer || [];
    if (currentShowTransfers.length) {
      const paramName = currentShowTransfers[0];
      const showControlType = visualModule?.parameterDefinitions?.[paramName]?.type;

      if (paramName && oneOf(showControlType, 'column', 'columns')) {
        return (column: ExpressionMeta) => {
          updateParameterValues({ [paramName]: showControlType === 'column' ? column : [column] });
        };
      }
    }
    return;
  }, [updateParameterValues, moduleName, visualModule?.parameterDefinitions]);

  const dataset = datasetState.getSomeData();

  useEffect(() => {
    setColumns(dataset?.columns ?? []);
  }, [dataset?.columns]);

  const [containerRef] = useModuleContainer({ host, selectedModule: moduleName, columns });

  return (
    <>
      <div className="explore-view">
        <SourcePane
          selectedTableName={table ? (table as SqlTable).getName() : '-'}
          onSelectedTableNameChange={t => updateTable(T(t))}
          disabled={Boolean(dataset && datasetState.loading)}
        />
        <FilterPane
          ref={filterPane}
          dataset={dataset}
          filter={where}
          onFilterChange={updateWhere}
          queryDruidSql={extendedQueryDruidSql}
        />
        <TilePicker<ModuleType>
          modules={VISUAL_MODULES}
          selectedTileName={moduleName}
          onSelectedTileNameChange={m => {
            const currentParameterDefinitions = visualModule?.parameterDefinitions || {};
            const valuesToTransfer: TransferValue[] = filterMap(
              VISUAL_MODULES.find(vm => vm.moduleName === visualModule?.moduleName)?.transfer || [],
              paramName => {
                const parameterDefinition = currentParameterDefinitions[paramName];
                if (!parameterDefinition) return;
                const parameterValue = parameterValues[paramName];
                if (typeof parameterValue === 'undefined') return;
                return [parameterDefinition.type, parameterValue];
              },
            );

            dropHighlight();
            setModuleName(m);
            resetParameterValues();

            const newModuleDef = VISUAL_MODULES.find(vm => vm.moduleName === m);
            if (newModuleDef) {
              const newParameters: any = newModuleDef.module?.parameters || {};
              const transferParameterValues: [name: string, value: any][] = filterMap(
                newModuleDef.transfer || [],
                t => {
                  const p = newParameters[t];
                  if (!p) return;
                  const normalizedTargetType = normalizeType(p.type);
                  const transferSource = valuesToTransfer.find(
                    ([t]) => normalizeType(t) === normalizedTargetType,
                  );
                  if (!transferSource) return;
                  const targetValue = adjustTransferValue(
                    transferSource[1],
                    transferSource[0],
                    p.type,
                  );
                  if (typeof targetValue === 'undefined') return;
                  return [t, targetValue];
                },
              );

              if (transferParameterValues.length) {
                updateParameterValues(Object.fromEntries(transferParameterValues));
              }
            }
          }}
          moreMenu={
            <Menu>
              <MenuItem
                icon={IconNames.HISTORY}
                text="Show query history"
                onClick={() => {
                  setShownText(getFormattedQueryHistory());
                }}
              />
              <MenuItem
                icon={IconNames.RESET}
                text="Reset visualization state"
                onClick={() => {
                  resetParameterValues();
                }}
              />
            </Menu>
          }
        />
        <div className="resource-pane-cnt">
          {!dataset && datasetState.loading && 'Loading...'}
          {dataset && (
            <ResourcePane
              dataset={dataset}
              onFilter={c => {
                filterPane.current?.filterOn(c);
              }}
              onShow={onShow}
            />
          )}
        </div>
        <DroppableContainer
          ref={containerRef}
          onDropColumn={column => {
            let nextModuleName: ModuleType;
            if (column.sqlType === 'TIMESTAMP') {
              nextModuleName = 'time_chart_echarts';
            } else {
              nextModuleName = 'table_react';
            }

            setModuleName(nextModuleName);

            if (column.sqlType === 'TIMESTAMP') {
              resetParameterValues();
            } else {
              updateParameterValues({ splitColumns: [column] });
            }
          }}
        />
        <div className="control-pane-cnt">
          {dataset && visualModule?.parameterDefinitions && (
            <ControlPane
              columns={dataset.columns}
              onUpdateParameterValues={updateParameterValues}
              parameterValues={parameterValues}
              visualModule={visualModule}
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
      <HighlightBubble referenceContainer={containerRef.current} />
    </>
  );
});
