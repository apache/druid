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
import { downloadFile } from '../../utils';

import './supervisor-statistics-table.scss';

export interface SupervisorStatisticsTableProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export interface SupervisorStatisticsTableState {
  data: JSON[];
  jsonValue: string;
}

export class SupervisorStatisticsTable extends React.PureComponent<
  SupervisorStatisticsTableProps,
  SupervisorStatisticsTableState
> {
  constructor(props: SupervisorStatisticsTableProps, context: any) {
    super(props, context);
    this.state = {
      data: [],
      jsonValue: '',
    };

    this.getJsonInfo();
  }

  private getJsonInfo = async (): Promise<void> => {
    const { endpoint, transform } = this.props;

    try {
      const resp = await axios.get(endpoint);
      let data = resp.data;
      if (transform) data = transform(data);
      const dataArray = this.getDataArray(data);
      this.setState({
        jsonValue: typeof data === 'string' ? data : JSON.stringify(data, undefined, 2),
        data: dataArray,
      });
    } catch (e) {
      this.setState({
        jsonValue: `Error: ` + e.response.data,
      });
    }
  };

  getDataArray(jsonValue: any) {
    const data: any[] = [];
    Object.keys(jsonValue).forEach(key => {
      data.push(jsonValue[key]);
    });
    return data;
  }

  renderTable() {
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
          return (
            <div>
              <div> Processed : {d.value.processed.toFixed(1)} </div>
              <div> Unparseable : {d.value.unparseable.toFixed(1)} </div>
              <div> ThrownAway : {d.value.thrownAway.toFixed(1)} </div>
              <div> ProcessedWithError : {d.value.processedWithError.toFixed(1)} </div>
            </div>
          );
        },
      },
      {
        Header: '1m',
        id: '1m',
        accessor: (d: any) => d[Object.keys(d)[0]].movingAverages.buildSegments['1m'],
        Cell: (d: any) => {
          return (
            <div>
              <div> Processed : {d.value.processed.toFixed(1)} </div>
              <div> Unparseable : {d.value.unparseable.toFixed(1)} </div>
              <div> ThrownAway : {d.value.thrownAway.toFixed(1)} </div>
              <div> ProcessedWithError : {d.value.processedWithError.toFixed(1)} </div>
            </div>
          );
        },
      },
      {
        Header: '5m',
        id: '5m',
        accessor: (d: any) => d[Object.keys(d)[0]].movingAverages.buildSegments['5m'],
        Cell: (d: any) => {
          return (
            <div>
              <div> Processed : {d.value.processed.toFixed(1)} </div>
              <div> Unparseable : {d.value.unparseable.toFixed(1)} </div>
              <div> ThrownAway : {d.value.thrownAway.toFixed(1)} </div>
              <div> ProcessedWithError : {d.value.processedWithError.toFixed(1)} </div>
            </div>
          );
        },
      },
      {
        Header: '15m',
        id: '15m',
        accessor: (d: any) => d[Object.keys(d)[0]].movingAverages.buildSegments['15m'],
        Cell: (d: any) => {
          return (
            <div>
              <div> Processed : {d.value.processed.toFixed(1)} </div>
              <div> Unparseable : {d.value.unparseable.toFixed(1)} </div>
              <div> ThrownAway : {d.value.thrownAway.toFixed(1)} </div>
              <div> ProcessedWithError : {d.value.processedWithError.toFixed(1)} </div>
            </div>
          );
        },
      },
    ];

    return (
      <ReactTable
        data={this.state.data}
        showPagination={false}
        defaultPageSize={6}
        columns={columns}
        noDataText={'No data found'}
      />
    );
  }

  render() {
    const { endpoint, downloadFilename } = this.props;
    const { jsonValue } = this.state;
    return (
      <div className="supervisor-statistics-table">
        <div className="top-actions">
          <ButtonGroup className="right-buttons">
            {downloadFilename && (
              <Button
                disabled={!jsonValue}
                text="Save"
                minimal
                onClick={() => downloadFile(jsonValue, 'json', downloadFilename)}
              />
            )}
            <Button
              text="Copy"
              disabled={!jsonValue}
              minimal
              onClick={() => {
                copy(jsonValue, { format: 'text/plain' });
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
          {!jsonValue && <Loader loadingText="" loading />}
          {jsonValue && this.renderTable()}
        </div>
      </div>
    );
  }
}
