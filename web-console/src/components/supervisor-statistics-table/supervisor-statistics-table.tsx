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
import axios from 'axios';
import React from 'react';
import ReactTable, { Column } from 'react-table';

import { UrlBaser } from '../../singletons/url-baser';
import { QueryManager } from '../../utils';
import { deepGet } from '../../utils/object-change';
import { Loader } from '../loader/loader';

import './supervisor-statistics-table.scss';

interface TaskSummary {
  totals: Record<string, StatsEntry>;
  movingAverages: Record<string, Record<string, StatsEntry>>;
}

interface StatsEntry {
  processed?: number;
  processedWithError?: number;
  thrownAway?: number;
  unparseable?: number;
  [key: string]: number | undefined;
}

interface TableRow {
  taskId: string;
  summary: TaskSummary;
}

export interface SupervisorStatisticsTableProps {
  endpoint: string;
  downloadFilename?: string;
}

export interface SupervisorStatisticsTableState {
  data?: TableRow[];
  loading: boolean;
  error?: string;
}

export class SupervisorStatisticsTable extends React.PureComponent<
  SupervisorStatisticsTableProps,
  SupervisorStatisticsTableState
> {
  private supervisorStatisticsQueryManager: QueryManager<null, TableRow[]>;

  constructor(props: SupervisorStatisticsTableProps, context: any) {
    super(props, context);
    this.state = {
      loading: true,
    };

    this.supervisorStatisticsQueryManager = new QueryManager({
      processQuery: async () => {
        const { endpoint } = this.props;
        const resp = await axios.get(endpoint);
        const data: Record<string, Record<string, TaskSummary>> = resp.data;

        return Object.values(data).flatMap(v =>
          Object.keys(v).map(k => ({ taskId: k, summary: v[k] })),
        );
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          data: result,
          error,
          loading,
        });
      },
    });
  }

  componentDidMount(): void {
    this.supervisorStatisticsQueryManager.runQuery(null);
  }

  renderCell(data: StatsEntry | undefined) {
    if (!data) {
      return <div>No data found</div>;
    }
    return Object.keys(data)
      .sort()
      .map(key => <div key={key}>{`${key}: ${Number(data[key]).toFixed(1)}`}</div>);
  }

  renderTable(error?: string) {
    const { data } = this.state;

    let columns: Column<TableRow>[] = [
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
        Cell: d => {
          return this.renderCell(d.value ? d.value : undefined);
        },
      },
    ];

    const movingAveragesBuildSegments = deepGet(
      data as any,
      '0.summary.movingAverages.buildSegments',
    );
    if (movingAveragesBuildSegments) {
      columns = columns.concat(
        Object.keys(movingAveragesBuildSegments)
          .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
          .map(
            (interval: string): Column<TableRow> => {
              return {
                Header: interval,
                id: interval,
                accessor: d => {
                  return deepGet(d, `summary.movingAverages.buildSegments.${interval}`);
                },
                Cell: d => {
                  return this.renderCell(d.value ? d.value : null);
                },
              };
            },
          ),
      );
    }

    return (
      <ReactTable
        data={this.state.data ? this.state.data : []}
        showPagination={false}
        defaultPageSize={6}
        columns={columns}
        noDataText={error ? error : 'No statistics data found'}
      />
    );
  }

  render(): JSX.Element {
    const { endpoint } = this.props;
    const { loading, error } = this.state;
    return (
      <div className="supervisor-statistics-table">
        <div className="top-actions">
          <ButtonGroup className="right-buttons">
            <Button
              text="View raw"
              disabled={loading}
              minimal
              onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
            />
          </ButtonGroup>
        </div>
        <div className="main-area">
          {loading ? <Loader loadingText="" loading /> : this.renderTable(error)}
        </div>
      </div>
    );
  }
}
