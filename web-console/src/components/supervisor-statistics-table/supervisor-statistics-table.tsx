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
import { deepGet } from '../../utils/object-change';

import './supervisor-statistics-table.scss';

export interface SupervisorStatisticsTableProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export interface SupervisorStatisticsTableState {
  data: any[];
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
          error: error ? error : null,
          loading,
        });
      },
    });
  }

  componentDidMount(): void {
    this.supervisorStatisticsTableQueryManager.runQuery(null);
  }

  renderCell(data: {
    processed?: number;
    unparseable?: number;
    thrownAway?: number;
    processedWithError?: number;
  }) {
    if (data) {
      // @ts-ignore
      return Object.keys(data).map(key => this.renderData(key, data[key]));
    }
    return <div> no data found</div>;
  }

  renderData(key: string, data: number | null) {
    if (data !== null) {
      return <div key={key}>{`${key}: ${data.toFixed(1)}`}</div>;
    }
    return <div> no data found</div>;
  }

  renderTable(error: string | null) {
    const { data } = this.state;

    let columns = [
      {
        Header: 'Task Id',
        id: 'task_id',
        accessor: (d: {}) => Object.keys(d)[0],
      },
      {
        Header: 'Totals',
        id: 'total',
        accessor: (d: any) => {
          return deepGet(d[Object.keys(d)[0]], 'totals.buildSegments');
        },
        Cell: (d: any) => {
          return this.renderCell(d.value ? d.value : null);
        },
      },
    ];

    columns = columns.concat(
      Object.keys(deepGet(data[0][Object.keys(data[0])[0]], 'movingAverages.buildSegments'))
        .map((interval: string, index: number) => {
          return {
            Header: interval,
            id: interval,
            key: index,
            accessor: (d: any) => {
              return deepGet(d[Object.keys(d)[0]], 'movingAverages.buildSegments');
            },
            Cell: (d: any) => {
              return this.renderCell(d.value ? d.value[interval] : null);
            },
          };
        })
        .sort((a, b) => (a.Header.localeCompare(b.Header, undefined, { numeric: true }) ? -1 : 1)),
    );
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
          {loading ? <Loader loadingText="" loading /> : !loading && this.renderTable(error)}
        </div>
      </div>
    );
  }
}
