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

import { Button, ButtonGroup } from '@blueprintjs/core';
import React from 'react';
import type { CellInfo, Column } from 'react-table';
import ReactTable from 'react-table';

import { Loader } from '../../../components/loader/loader';
import type { RowStats, RowStatsCounter, SupervisorStats } from '../../../druid-models';
import { useInterval, useQueryManager } from '../../../hooks';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../../react-table';
import { Api, UrlBaser } from '../../../singletons';
import { deepGet, formatByteRate, formatBytes, formatInteger, formatRate } from '../../../utils';

import './supervisor-statistics-table.scss';

export interface SupervisorStatisticsTableRow {
  groupId: string;
  taskId: string;
  rowStats: RowStats;
}

export function normalizeSupervisorStatisticsResults(
  data: SupervisorStats,
): SupervisorStatisticsTableRow[] {
  return Object.entries(data).flatMap(([groupId, v]) =>
    Object.entries(v).map(([taskId, rowStats]) => ({ groupId, taskId, rowStats })),
  );
}

export interface SupervisorStatisticsTableProps {
  supervisorId: string;
  downloadFilename?: string;
}

export const SupervisorStatisticsTable = React.memo(function SupervisorStatisticsTable(
  props: SupervisorStatisticsTableProps,
) {
  const { supervisorId } = props;
  const statsEndpoint = `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/stats`;

  const [supervisorStatisticsState, supervisorStatisticsQueryManager] = useQueryManager<
    null,
    SupervisorStatisticsTableRow[]
  >({
    initQuery: null,
    processQuery: async () => {
      const resp = await Api.instance.get<SupervisorStats>(statsEndpoint);
      return normalizeSupervisorStatisticsResults(resp.data);
    },
  });

  useInterval(() => {
    supervisorStatisticsQueryManager.rerunLastQuery(true);
  }, 1500);

  function renderCounters(cell: CellInfo, isRate: boolean) {
    const c: RowStatsCounter = cell.value;
    if (!c) return null;

    const formatNumber = isRate ? formatRate : formatInteger;
    const formatData = isRate ? formatByteRate : formatBytes;
    const bytes = c.processedBytes ? ` (${formatData(c.processedBytes)})` : '';
    return (
      <div>
        <div>{`Processed: ${formatNumber(c.processed)}${bytes}`}</div>
        {Boolean(c.processedWithError) && (
          <div>Processed with error: {formatNumber(c.processedWithError)}</div>
        )}
        {Boolean(c.thrownAway) && <div>Thrown away: {formatNumber(c.thrownAway)}</div>}
        {Boolean(c.unparseable) && <div>Unparseable: {formatNumber(c.unparseable)}</div>}
      </div>
    );
  }

  function renderTable() {
    let columns: Column<SupervisorStatisticsTableRow>[] = [
      {
        Header: 'Group ID',
        accessor: 'groupId',
        className: 'padded',
        width: 100,
      },
      {
        Header: 'Task ID',
        accessor: 'taskId',
        className: 'padded',
        width: 400,
      },
      {
        Header: 'Totals',
        id: 'total',
        className: 'padded',
        width: 200,
        accessor: 'rowStats.totals.buildSegments',
        Cell: c => renderCounters(c, false),
      },
    ];

    const movingAveragesBuildSegments = deepGet(
      supervisorStatisticsState.data as any,
      '0.rowStats.movingAverages.buildSegments',
    );
    if (movingAveragesBuildSegments) {
      columns = columns.concat(
        Object.keys(movingAveragesBuildSegments)
          .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
          .map((interval: string): Column<SupervisorStatisticsTableRow> => {
            return {
              Header: interval,
              id: interval,
              className: 'padded',
              width: 200,
              accessor: `rowStats.movingAverages.buildSegments.${interval}`,
              Cell: c => renderCounters(c, true),
            };
          }),
      );
    }

    const statisticsData = supervisorStatisticsState.data || [];
    return (
      <ReactTable
        data={statisticsData}
        defaultPageSize={SMALL_TABLE_PAGE_SIZE}
        pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={statisticsData.length > SMALL_TABLE_PAGE_SIZE}
        columns={columns}
        noDataText={supervisorStatisticsState.getErrorMessage() || 'No statistics data found'}
      />
    );
  }

  return (
    <div className="supervisor-statistics-table">
      <div className="top-actions">
        <ButtonGroup className="right-buttons">
          <Button
            text="View raw"
            disabled={supervisorStatisticsState.loading}
            minimal
            onClick={() => window.open(UrlBaser.base(statsEndpoint), '_blank')}
          />
        </ButtonGroup>
      </div>
      <div className="main-area">
        {supervisorStatisticsState.loading ? <Loader /> : renderTable()}
      </div>
    </div>
  );
});
