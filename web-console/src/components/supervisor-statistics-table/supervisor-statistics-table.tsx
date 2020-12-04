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
import ReactTable, { CellInfo, Column } from 'react-table';

import { useQueryManager } from '../../hooks';
import { Api, UrlBaser } from '../../singletons';
import { deepGet } from '../../utils';
import { Loader } from '../loader/loader';

import './supervisor-statistics-table.scss';

export interface TaskSummary {
  totals: Record<string, StatsEntry>;
  movingAverages: Record<string, Record<string, StatsEntry>>;
}

export interface StatsEntry {
  processed?: number;
  processedWithError?: number;
  thrownAway?: number;
  unparseable?: number;
  [key: string]: number | undefined;
}

export interface SupervisorStatisticsTableRow {
  taskId: string;
  summary: TaskSummary;
}

export function normalizeSupervisorStatisticsResults(
  data: Record<string, Record<string, TaskSummary>>,
): SupervisorStatisticsTableRow[] {
  return Object.values(data).flatMap(v => Object.keys(v).map(k => ({ taskId: k, summary: v[k] })));
}

export interface SupervisorStatisticsTableProps {
  supervisorId: string;
  downloadFilename?: string;
}

export const SupervisorStatisticsTable = React.memo(function SupervisorStatisticsTable(
  props: SupervisorStatisticsTableProps,
) {
  const { supervisorId } = props;
  const endpoint = `/druid/indexer/v1/supervisor/${supervisorId}/stats`;

  const [supervisorStatisticsState] = useQueryManager<null, SupervisorStatisticsTableRow[]>({
    processQuery: async () => {
      const resp = await Api.instance.get(endpoint);
      return normalizeSupervisorStatisticsResults(resp.data);
    },
    initQuery: null,
  });

  function renderCell(cell: CellInfo) {
    const cellValue = cell.value;
    if (!cellValue) {
      return <div>No data found</div>;
    }

    return Object.keys(cellValue)
      .sort()
      .map(key => <div key={key}>{`${key}: ${Number(cellValue[key]).toFixed(1)}`}</div>);
  }

  function renderTable() {
    let columns: Column<SupervisorStatisticsTableRow>[] = [
      {
        Header: 'Task ID',
        id: 'task_id',
        accessor: d => d.taskId,
      },
      {
        Header: 'Totals',
        id: 'total',
        accessor: d => {
          return deepGet(d, 'summary.totals.buildSegments') as StatsEntry;
        },
        Cell: renderCell,
      },
    ];

    const movingAveragesBuildSegments = deepGet(
      supervisorStatisticsState.data as any,
      '0.summary.movingAverages.buildSegments',
    );
    if (movingAveragesBuildSegments) {
      columns = columns.concat(
        Object.keys(movingAveragesBuildSegments)
          .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
          .map(
            (interval: string): Column<SupervisorStatisticsTableRow> => {
              return {
                Header: interval,
                id: interval,
                accessor: d => {
                  return deepGet(d, `summary.movingAverages.buildSegments.${interval}`);
                },
                Cell: renderCell,
              };
            },
          ),
      );
    }

    return (
      <ReactTable
        data={supervisorStatisticsState.data || []}
        showPagination={false}
        defaultPageSize={6}
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
            onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
          />
        </ButtonGroup>
      </div>
      <div className="main-area">
        {supervisorStatisticsState.loading ? <Loader /> : renderTable()}
      </div>
    </div>
  );
});
