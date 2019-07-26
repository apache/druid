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

import { Button, ButtonGroup, Intent } from '@blueprintjs/core';
import axios from 'axios';
import copy from 'copy-to-clipboard';
import React from 'react';
import ReactTable from 'react-table';

import { Loader } from '..';
import { AppToaster } from '../../singletons/toaster';
import { UrlBaser } from '../../singletons/url-baser';
import { downloadFile, QueryManager } from '../../utils';

import './supervisor-statistics-table.scss';

export interface SupervisorStatisticsTableProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export interface SupervisorStatisticsTableState {
  data: JSON[];
  jsonValue: string;
  loading: boolean;
  error: string | null;
}

export class SupervisorStatisticsTable extends React.PureComponent<
  SupervisorStatisticsTableProps,
  SupervisorStatisticsTableState
> {
  private supervisorStatisticsTableQueryManager: QueryManager<null, SupervisorStatisticsTableState>;
  constructor(props: SupervisorStatisticsTableProps, context: any) {
    super(props, context);
    this.state = {
      data: [],
      jsonValue: '',
      loading: true,
      error: null,
    };
    this.supervisorStatisticsTableQueryManager = new QueryManager({
      processQuery: async () => {
        const { endpoint, transform } = this.props;
        const resp = await axios.get(endpoint);
        let data = resp.data;
        if (transform) data = transform(data);
        return data;
      },
      onStateChange: ({ result, loading, error }) => {
        const dataArray: JSON[] = [];
        if (result) {
          Object.keys(result).forEach(key => {
            dataArray.push(result[key]);
          });
        }
        this.setState({
          jsonValue: result ? JSON.stringify(result) : '',
          data: dataArray,
          loading,
          error,
        });
      },
    });
  }

  componentDidMount(): void {
    this.supervisorStatisticsTableQueryManager.runQuery(null);
  }

  renderCell(data: any) {
    return (
      <div>
        <div>{`Processed: ${data.processed.toFixed(1)}`}</div>
        <div>{`Unparseable: ${data.unparseable.toFixed(1)}`}</div>
        <div>{`ThrownAway: ${data.thrownAway.toFixed(1)}`}</div>
        <div>{`ProcessedWithError: ${data.processedWithError.toFixed(1)}`}</div>
      </div>
    );
  }

  renderTable(error: string | null) {
    const columns = [
      {
        Header: 'Task Id',
        id: 'task_id',
        accessor: (d: {}) => Object.keys(d)[0],
      },
      {
        Header: 'Totals',
        id: 'totals',
        accessor: (d: any) => d[Object.keys(d)[0]].totals.buildSegments,
        Cell: (d: any) => {
          return this.renderCell(d.value);
        },
      },
      {
        Header: '1m',
        id: '1m',
        accessor: (d: any) => d[Object.keys(d)[0]].movingAverages.buildSegments['1m'],
        Cell: (d: any) => {
          return this.renderCell(d.value);
        },
      },
      {
        Header: '5m',
        id: '5m',
        accessor: (d: any) => d[Object.keys(d)[0]].movingAverages.buildSegments['5m'],
        Cell: (d: any) => {
          return this.renderCell(d.value);
        },
      },
      {
        Header: '15m',
        id: '15m',
        accessor: (d: any) => d[Object.keys(d)[0]].movingAverages.buildSegments['15m'],
        Cell: (d: any) => {
          return this.renderCell(d.value);
        },
      },
    ];

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

  render() {
    const { endpoint, downloadFilename } = this.props;
    const { jsonValue, loading, error } = this.state;
    return (
      <div className="supervisor-statistics-table">
        <div className="top-actions">
          <ButtonGroup className="right-buttons">
            {downloadFilename && (
              <Button
                disabled={!jsonValue}
                text="Save"
                minimal
                onClick={() =>
                  jsonValue ? downloadFile(jsonValue, 'json', downloadFilename) : null
                }
              />
            )}
            <Button
              text="Copy"
              disabled={!jsonValue}
              minimal
              onClick={() => {
                if (jsonValue != null) {
                  copy(jsonValue, { format: 'text/plain' });
                }
                AppToaster.show({
                  message: 'JSON copied to clipboard',
                  intent: Intent.SUCCESS,
                });
              }}
            />
            <Button
              text="View raw"
              disabled={!jsonValue}
              minimal
              onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
            />
          </ButtonGroup>
        </div>
        <div className="main-area">
          {loading && <Loader loadingText="" loading />}
          {!loading && this.renderTable(error)}
        </div>
      </div>
    );
  }
}
