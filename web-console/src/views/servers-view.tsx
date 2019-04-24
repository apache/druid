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

import { Button, Switch } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import { sum } from 'd3-array';
import * as React from 'react';
import ReactTable from 'react-table';
import { Filter } from 'react-table';

import { TableColumnSelection } from '../components/table-column-selection';
import { ViewControlBar } from '../components/view-control-bar';
import {
  addFilter,
  formatBytes,
  formatBytesCompact, LocalStorageKeys,
  queryDruidSql,
  QueryManager, TableColumnSelectionHandler
} from '../utils';

import './servers-view.scss';

const serverTableColumns: string[] = ['Server', 'Tier', 'Curr size', 'Max size', 'Usage', 'Load/drop queues', 'Host', 'Port'];
const middleManagerTableColumns: string[] = ['Host', 'Usage', 'Availability groups', 'Last completed task time', 'Blacklisted until'];

function formatQueues(segmentsToLoad: number, segmentsToLoadSize: number, segmentsToDrop: number, segmentsToDropSize: number): string {
  const queueParts: string[] = [];
  if (segmentsToLoad) {
    queueParts.push(`${segmentsToLoad} segments to load (${formatBytesCompact(segmentsToLoadSize)})`);
  }
  if (segmentsToDrop) {
    queueParts.push(`${segmentsToDrop} segments to drop (${formatBytesCompact(segmentsToDropSize)})`);
  }
  return queueParts.join(', ') || 'Empty queues';
}

export interface ServersViewProps extends React.Props<any> {
  middleManager: string | null;
  goToSql: (initSql: string) => void;
  goToTask: (taskId: string) => void;
  noSqlMode: boolean;
}

export interface ServersViewState {
  serversLoading: boolean;
  servers: any[] | null;
  serversError: string | null;
  serverFilter: Filter[];
  groupByTier: boolean;

  middleManagersLoading: boolean;
  middleManagers: any[] | null;
  middleManagersError: string | null;
  middleManagerFilter: Filter[];
}

interface ServerQueryResultRow {
  curr_size: number;
  host: string;
  max_size: number;
  plaintext_port: number;
  server: string;
  tier: string;
  tls_port: number;
  segmentsToDrop?: number;
  segmentsToDropSize?: number;
  segmentsToLoad?: number;
  segmentsToLoadSize?: number;
}

interface MiddleManagerQueryResultRow {
  availabilityGroups: string[];
  blacklistedUntil: string | null;
  currCapacityUsed: number;
  lastCompletedTaskTime: string;
  runningTasks: string[];
  worker: any;
}

export class ServersView extends React.Component<ServersViewProps, ServersViewState> {
  private serverQueryManager: QueryManager<string, ServerQueryResultRow[]>;
  private middleManagerQueryManager: QueryManager<string, MiddleManagerQueryResultRow[]>;
  private serverTableColumnSelectionHandler: TableColumnSelectionHandler;
  private middleManagerTableColumnSelectionHandler: TableColumnSelectionHandler;

  constructor(props: ServersViewProps, context: any) {
    super(props, context);
    this.state = {
      serversLoading: true,
      servers: null,
      serversError: null,
      serverFilter: [],
      groupByTier: false,

      middleManagersLoading: true,
      middleManagers: null,
      middleManagersError: null,
      middleManagerFilter: props.middleManager ? [{ id: 'host', value: props.middleManager }] : []
    };

    this.serverTableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.SERVER_TABLE_COLUMN_SELECTION, () => this.setState({})
    );

    this.middleManagerTableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.MIDDLEMANAGER_TABLE_COLUMN_SELECTION, () => this.setState({})
    );
  }

  static getServers = async (): Promise<ServerQueryResultRow[]> => {
    const allServerResp = await axios.get('/druid/coordinator/v1/servers?simple');
    const allServers = allServerResp.data;
    return allServers.filter((s: any) => s.type === 'historical').map((s: any) => {
      return {
        host: s.host.split(':')[0],
        plaintext_port: parseInt(s.host.split(':')[1], 10),
        server: s.host,
        curr_size: s.currSize,
        max_size: s.maxSize,
        tier: s.tier,
        tls_port: -1
      };
    });
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;
    this.serverQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        let servers: ServerQueryResultRow[];
        if (!noSqlMode) {
          servers = await queryDruidSql({ query });
          if (servers.length === 0) {
            servers = await ServersView.getServers();
          }
        } else {
          servers = await ServersView.getServers();
        }
        const loadQueueResponse = await axios.get('/druid/coordinator/v1/loadqueue?simple');
        const loadQueues = loadQueueResponse.data;
        return servers.map((s: any) => {
          const loadQueueInfo = loadQueues[s.server];
          if (loadQueueInfo) {
            s = Object.assign(s, loadQueueInfo);
          }
          return s;
        });
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          servers: result,
          serversLoading: loading,
          serversError: error
        });
      }
    });

    this.serverQueryManager.runQuery(`SELECT
  "tier", "server", "host", "plaintext_port", "tls_port", "curr_size", "max_size"
FROM sys.servers
WHERE "server_type" = 'historical'`);

    this.middleManagerQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const resp = await axios.get('/druid/indexer/v1/workers');
        return resp.data;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          middleManagers: result,
          middleManagersLoading: loading,
          middleManagersError: error
        });
      }
    });

    this.middleManagerQueryManager.runQuery('dummy');

  }

  componentWillUnmount(): void {
    this.serverQueryManager.terminate();
    this.middleManagerQueryManager.terminate();
  }

  renderServersTable() {
    const { servers, serversLoading, serversError, serverFilter, groupByTier } = this.state;
    const { serverTableColumnSelectionHandler } = this;

    const fillIndicator = (value: number) => {
      return <div className="fill-indicator">
        <div className="bar" style={{ width: `${value * 100}%` }}/>
        <div className="label">{(value * 100).toFixed(1) + '%'}</div>
      </div>;
    };

    return <ReactTable
      data={servers || []}
      loading={serversLoading}
      noDataText={!serversLoading && servers && !servers.length ? 'No historicals' : (serversError || '')}
      filterable
      filtered={serverFilter}
      onFilteredChange={(filtered, column) => {
        this.setState({ serverFilter: filtered });
      }}
      pivotBy={groupByTier ? ['tier'] : []}
      columns={[
        {
          Header: 'Server',
          accessor: 'server',
          width: 300,
          Aggregated: row => '',
          show: serverTableColumnSelectionHandler.showColumn('Server')
        },
        {
          Header: 'Tier',
          accessor: 'tier',
          Cell: row => {
            const value = row.value;
            return <a onClick={() => { this.setState({ serverFilter: addFilter(serverFilter, 'tier', value) }); }}>{value}</a>;
          },
          show: serverTableColumnSelectionHandler.showColumn('Tier')
        },
        {
          Header: 'Curr size',
          id: 'curr_size',
          width: 100,
          filterable: false,
          accessor: 'curr_size',
          Aggregated: row => {
            const originals = row.subRows.map(r => r._original);
            const totalCurr = sum(originals, s => s.curr_size);
            return formatBytes(totalCurr);
          },
          Cell: row => {
            if (row.aggregated) return '';
            if (row.value === null) return '';
            return formatBytes(row.value);
          },
          show: serverTableColumnSelectionHandler.showColumn('Curr size')
        },
        {
          Header: 'Max size',
          id: 'max_size',
          width: 100,
          filterable: false,
          accessor: 'max_size',
          Aggregated: row => {
            const originals = row.subRows.map(r => r._original);
            const totalMax = sum(originals, s => s.max_size);
            return formatBytes(totalMax);
          },
          Cell: row => {
            if (row.aggregated) return '';
            if (row.value === null) return '';
            return formatBytes(row.value);
          },
          show: serverTableColumnSelectionHandler.showColumn('Max size')
        },
        {
          Header: 'Usage',
          id: 'usage',
          width: 100,
          filterable: false,
          accessor: (row) => row.max_size ? (row.curr_size / row.max_size) : null,
          Aggregated: row => {
            const originals = row.subRows.map(r => r._original);
            const totalCurr = sum(originals, s => s.curr_size);
            const totalMax = sum(originals, s => s.max_size);
            return fillIndicator(totalCurr / totalMax);
          },
          Cell: row => {
            if (row.aggregated) return '';
            if (row.value === null) return '';
            return fillIndicator(row.value);
          },
          show: serverTableColumnSelectionHandler.showColumn('Usage')
        },
        {
          Header: 'Load/drop queues',
          id: 'queue',
          width: 400,
          filterable: false,
          accessor: (row) => (row.segmentsToLoad || 0) + (row.segmentsToDrop || 0),
          Cell: (row => {
            if (row.aggregated) return '';
            const { segmentsToLoad, segmentsToLoadSize, segmentsToDrop, segmentsToDropSize } = row.original;
            return formatQueues(segmentsToLoad, segmentsToLoadSize, segmentsToDrop, segmentsToDropSize);
          }),
          Aggregated: row => {
            const originals = row.subRows.map(r => r._original);
            const segmentsToLoad = sum(originals, s => s.segmentsToLoad);
            const segmentsToLoadSize = sum(originals, s => s.segmentsToLoadSize);
            const segmentsToDrop = sum(originals, s => s.segmentsToDrop);
            const segmentsToDropSize = sum(originals, s => s.segmentsToDropSize);
            return formatQueues(segmentsToLoad, segmentsToLoadSize, segmentsToDrop, segmentsToDropSize);
          },
          show: serverTableColumnSelectionHandler.showColumn('Load/drop queues')
        },
        {
          Header: 'Host',
          accessor: 'host',
          Aggregated: () => '',
          show: serverTableColumnSelectionHandler.showColumn('Host')
        },
        {
          Header: 'Port',
          id: 'port',
          accessor: (row) => {
            const ports: string[] = [];
            if (row.plaintext_port !== -1) {
              ports.push(`${row.plaintext_port} (plain)`);
            }
            if (row.tls_port !== -1) {
              ports.push(`${row.tls_port} (TLS)`);
            }
            return ports.join(', ') || 'No port';
          },
          Aggregated: () => '',
          show: serverTableColumnSelectionHandler.showColumn('Port')
        }
      ]}
      defaultPageSize={10}
      className="-striped -highlight"
    />;
  }

  renderMiddleManagerTable() {
    const { goToTask } = this.props;
    const { middleManagers, middleManagersLoading, middleManagersError, middleManagerFilter } = this.state;
    const { middleManagerTableColumnSelectionHandler } = this;

    return <ReactTable
      data={middleManagers || []}
      loading={middleManagersLoading}
      noDataText={!middleManagersLoading && middleManagers && !middleManagers.length ? 'No MiddleManagers' : (middleManagersError || '')}
      filterable
      filtered={middleManagerFilter}
      onFilteredChange={(filtered, column) => {
        this.setState({ middleManagerFilter: filtered });
      }}
      columns={[
        {
          Header: 'Host',
          id: 'host',
          accessor: (row) => row.worker.host,
          Cell: row => {
            const value = row.value;
            return <a onClick={() => { this.setState({ middleManagerFilter: addFilter(middleManagerFilter, 'host', value) }); }}>{value}</a>;
          },
          show: middleManagerTableColumnSelectionHandler.showColumn('Host')
        },
        {
          Header: 'Usage',
          id: 'usage',
          width: 60,
          accessor: (row) => `${row.currCapacityUsed} / ${row.worker.capacity}`,
          filterable: false,
          show: middleManagerTableColumnSelectionHandler.showColumn('Usage')
        },
        {
          Header: 'Availability groups',
          id: 'availabilityGroups',
          width: 60,
          accessor: (row) => row.availabilityGroups.length,
          filterable: false,
          show: middleManagerTableColumnSelectionHandler.showColumn('Availability groups')
        },
        {
          Header: 'Last completed task time',
          accessor: 'lastCompletedTaskTime',
          show: middleManagerTableColumnSelectionHandler.showColumn('Last completed task time')
        },
        {
          Header: 'Blacklisted until',
          accessor: 'blacklistedUntil',
          show: middleManagerTableColumnSelectionHandler.showColumn('Blacklisted until')
        }
      ]}
      defaultPageSize={10}
      className="-striped -highlight"
      SubComponent={rowInfo => {
        const runningTasks = rowInfo.original.runningTasks;
        return <div style={{ padding: '20px' }}>
          {
            runningTasks.length ?
              <>
                <span>Running tasks:</span>
                <ul>{runningTasks.map((t: string) => <li key={t}>{t}&nbsp;<a onClick={() => goToTask(t)}>&#x279A;</a></li>)}</ul>
              </> :
              <span>No running tasks</span>
          }
        </div>;
      }}
    />;
  }

  render() {
    const { goToSql, noSqlMode } = this.props;
    const { groupByTier } = this.state;
    const { serverTableColumnSelectionHandler, middleManagerTableColumnSelectionHandler } = this;

    return <div className="servers-view app-view">
      <ViewControlBar label="Historicals">
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.serverQueryManager.rerunLastQuery()}
        />
        {
          !noSqlMode &&
          <Button
            icon={IconNames.APPLICATION}
            text="Go to SQL"
            onClick={() => goToSql(this.serverQueryManager.getLastQuery())}
          />
        }
        <Switch
          checked={groupByTier}
          label="Group by tier"
          onChange={() => this.setState({ groupByTier: !groupByTier })}
        />
        <TableColumnSelection
          columns={serverTableColumns}
          onChange={(column) => serverTableColumnSelectionHandler.changeTableColumnSelection(column)}
          tableColumnsHidden={serverTableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderServersTable()}

      <div className="control-separator"/>

      <ViewControlBar label="MiddleManagers">
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.middleManagerQueryManager.rerunLastQuery()}
        />
        <TableColumnSelection
          columns={middleManagerTableColumns}
          onChange={(column) => middleManagerTableColumnSelectionHandler.changeTableColumnSelection(column)}
          tableColumnsHidden={middleManagerTableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderMiddleManagerTable()}
    </div>;
  }
}
