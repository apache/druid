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

import {
  Button,
  ButtonGroup,
  Intent,
  Label,
  Menu,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import { sum } from 'd3-array';
import React from 'react';
import ReactTable from 'react-table';
import { Filter } from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  RefreshButton,
  TableColumnSelector,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import {
  addFilter,
  formatBytes,
  formatBytesCompact,
  LocalStorageKeys,
  lookupBy,
  queryDruidSql,
  QueryManager,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';
import { deepGet } from '../../utils/object-change';

import './servers-view.scss';

const serverTableColumns: string[] = [
  'Server',
  'Type',
  'Tier',
  'Host',
  'Port',
  'Curr size',
  'Max size',
  'Usage',
  'Detail',
  ACTION_COLUMN_LABEL,
];

function formatQueues(
  segmentsToLoad: number,
  segmentsToLoadSize: number,
  segmentsToDrop: number,
  segmentsToDropSize: number,
): string {
  const queueParts: string[] = [];
  if (segmentsToLoad) {
    queueParts.push(
      `${segmentsToLoad} segments to load (${formatBytesCompact(segmentsToLoadSize)})`,
    );
  }
  if (segmentsToDrop) {
    queueParts.push(
      `${segmentsToDrop} segments to drop (${formatBytesCompact(segmentsToDropSize)})`,
    );
  }
  return queueParts.join(', ') || 'Empty load/drop queues';
}

export interface ServersViewProps {
  middleManager: string | undefined;
  goToQuery: (initSql: string) => void;
  goToTask: (taskId: string) => void;
  noSqlMode: boolean;
}

export interface ServersViewState {
  serversLoading: boolean;
  servers?: any[];
  serversError?: string;
  serverFilter: Filter[];
  groupServersBy?: 'server_type' | 'tier';

  middleManagerDisableWorkerHost?: string;
  middleManagerEnableWorkerHost?: string;

  hiddenColumns: LocalStorageBackedArray<string>;
}

interface ServerQueryResultRow {
  server: string;
  server_type: string;
  tier: string;
  curr_size: number;
  host: string;
  max_size: number;
  plaintext_port: number;
  tls_port: number;
}

interface LoadQueueStatus {
  segmentsToDrop: number;
  segmentsToDropSize: number;
  segmentsToLoad: number;
  segmentsToLoadSize: number;
}

interface MiddleManagerQueryResultRow {
  availabilityGroups: string[];
  blacklistedUntil: string | null;
  currCapacityUsed: number;
  lastCompletedTaskTime: string;
  runningTasks: string[];
  worker: {
    capacity: number;
    host: string;
    ip: string;
    scheme: string;
    version: string;
  };
}

interface ServerResultRow
  extends ServerQueryResultRow,
    Partial<LoadQueueStatus>,
    Partial<MiddleManagerQueryResultRow> {}

export class ServersView extends React.PureComponent<ServersViewProps, ServersViewState> {
  private serverQueryManager: QueryManager<boolean, ServerResultRow[]>;

  // Ranking
  //   coordinator => 7
  //   overlord => 6
  //   router => 5
  //   broker => 4
  //   historical => 3
  //   middle_manager => 2
  //   peon => 1

  static SERVER_SQL = `SELECT
  "server", "server_type", "tier", "host", "plaintext_port", "tls_port", "curr_size", "max_size",
  (
    CASE "server_type"
    WHEN 'coordinator' THEN 7
    WHEN 'overlord' THEN 6
    WHEN 'router' THEN 5
    WHEN 'broker' THEN 4
    WHEN 'historical' THEN 3
    WHEN 'middle_manager' THEN 2
    WHEN 'peon' THEN 1
    ELSE 0
    END
  ) AS "rank"
FROM sys.servers
ORDER BY "rank" DESC, "server" DESC`;

  static async getServers(): Promise<ServerQueryResultRow[]> {
    const allServerResp = await axios.get('/druid/coordinator/v1/servers?simple');
    const allServers = allServerResp.data;
    return allServers.map((s: any) => {
      return {
        server: s.host,
        server_type: s.type === 'indexer-executor' ? 'peon' : s.type,
        tier: s.tier,
        host: s.host.split(':')[0],
        plaintext_port: parseInt(s.host.split(':')[1], 10),
        curr_size: s.currSize,
        max_size: s.maxSize,
        tls_port: -1,
      };
    });
  }

  constructor(props: ServersViewProps, context: any) {
    super(props, context);
    this.state = {
      serversLoading: true,
      serverFilter: [],

      hiddenColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.SERVER_TABLE_COLUMN_SELECTION,
      ),
    };

    this.serverQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        let servers: ServerQueryResultRow[];
        if (!noSqlMode) {
          servers = await queryDruidSql({ query: ServersView.SERVER_SQL });
        } else {
          servers = await ServersView.getServers();
        }

        const loadQueueResponse = await axios.get('/druid/coordinator/v1/loadqueue?simple');
        const loadQueues: Record<string, LoadQueueStatus> = loadQueueResponse.data;
        servers = servers.map((s: any) => {
          const loadQueueInfo = loadQueues[s.server];
          if (loadQueueInfo) {
            s = Object.assign(s, loadQueueInfo);
          }
          return s;
        });

        let middleManagers: MiddleManagerQueryResultRow[];
        try {
          const middleManagerResponse = await axios.get('/druid/indexer/v1/workers');
          middleManagers = middleManagerResponse.data;
        } catch (e) {
          if (
            e.response &&
            typeof e.response.data === 'object' &&
            e.response.data.error === 'Task Runner does not support worker listing'
          ) {
            // Swallow this error because it simply a reflection of a local task runner.
            middleManagers = [];
          } else {
            // Otherwise re-throw.
            throw e;
          }
        }

        const middleManagersLookup = lookupBy(middleManagers, m => m.worker.host);

        return servers.map((s: any) => {
          const middleManagerInfo = middleManagersLookup[s.server];
          if (middleManagerInfo) {
            s = Object.assign(s, middleManagerInfo);
          }
          return s;
        });
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          servers: result,
          serversLoading: loading,
          serversError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;
    this.serverQueryManager.runQuery(noSqlMode);
  }

  componentWillUnmount(): void {
    this.serverQueryManager.terminate();
  }

  renderServersTable() {
    const {
      servers,
      serversLoading,
      serversError,
      serverFilter,
      groupServersBy,
      hiddenColumns,
    } = this.state;

    const fillIndicator = (value: number) => {
      let formattedValue = (value * 100).toFixed(1);
      if (formattedValue === '0.0' && value > 0) formattedValue = '~' + formattedValue;
      return (
        <div className="fill-indicator">
          <div className="bar" style={{ width: `${value * 100}%` }} />
          <div className="label">{formattedValue + '%'}</div>
        </div>
      );
    };

    return (
      <ReactTable
        data={servers || []}
        loading={serversLoading}
        noDataText={
          !serversLoading && servers && !servers.length ? 'No historicals' : serversError || ''
        }
        filterable
        filtered={serverFilter}
        onFilteredChange={filtered => {
          this.setState({ serverFilter: filtered });
        }}
        pivotBy={groupServersBy ? [groupServersBy] : []}
        defaultPageSize={50}
        columns={[
          {
            Header: 'Server',
            accessor: 'server',
            width: 300,
            Aggregated: () => '',
            show: hiddenColumns.exists('Server'),
          },
          {
            Header: 'Type',
            accessor: 'server_type',
            width: 150,
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({ serverFilter: addFilter(serverFilter, 'server_type', value) });
                  }}
                >
                  {value}
                </a>
              );
            },
            show: hiddenColumns.exists('Type'),
          },
          {
            Header: 'Tier',
            accessor: 'tier',
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({ serverFilter: addFilter(serverFilter, 'tier', value) });
                  }}
                >
                  {value}
                </a>
              );
            },
            show: hiddenColumns.exists('Tier'),
          },
          {
            Header: 'Host',
            accessor: 'host',
            Aggregated: () => '',
            show: hiddenColumns.exists('Host'),
          },
          {
            Header: 'Port',
            id: 'port',
            accessor: row => {
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
            show: hiddenColumns.exists('Port'),
          },
          {
            Header: 'Curr size',
            id: 'curr_size',
            width: 100,
            filterable: false,
            accessor: 'curr_size',
            Aggregated: row => {
              if (row.row._pivotVal !== 'historical') return '';
              const originals = row.subRows.map(r => r._original);
              const totalCurr = sum(originals, s => s.curr_size);
              return formatBytes(totalCurr);
            },
            Cell: row => {
              if (row.aggregated || row.original.server_type !== 'historical') return '';
              if (row.value === null) return '';
              return formatBytes(row.value);
            },
            show: hiddenColumns.exists('Curr size'),
          },
          {
            Header: 'Max size',
            id: 'max_size',
            width: 100,
            filterable: false,
            accessor: 'max_size',
            Aggregated: row => {
              if (row.row._pivotVal !== 'historical') return '';
              const originals = row.subRows.map(r => r._original);
              const totalMax = sum(originals, s => s.max_size);
              return formatBytes(totalMax);
            },
            Cell: row => {
              if (row.aggregated || row.original.server_type !== 'historical') return '';
              if (row.value === null) return '';
              return formatBytes(row.value);
            },
            show: hiddenColumns.exists('Max size'),
          },
          {
            Header: 'Usage',
            id: 'usage',
            width: 100,
            filterable: false,
            accessor: row => {
              if (row.server_type === 'middle_manager') {
                return row.worker ? row.currCapacityUsed / row.worker.capacity : null;
              } else {
                return row.max_size ? row.curr_size / row.max_size : null;
              }
            },
            Aggregated: row => {
              switch (row.row._pivotVal) {
                case 'historical':
                  const originalHistoricals = row.subRows.map(r => r._original);
                  const totalCurr = sum(originalHistoricals, s => s.curr_size);
                  const totalMax = sum(originalHistoricals, s => s.max_size);
                  return fillIndicator(totalCurr / totalMax);

                case 'middle_manager':
                  const originalMiddleManagers = row.subRows.map(r => r._original);
                  const totalCurrCapacityUsed = sum(
                    originalMiddleManagers,
                    s => s.currCapacityUsed || 0,
                  );
                  const totalWorkerCapacity = sum(
                    originalMiddleManagers,
                    s => deepGet(s, 'worker.capacity') || 0,
                  );
                  return `${totalCurrCapacityUsed} / ${totalWorkerCapacity} (total slots)`;

                default:
                  return '';
              }
            },
            Cell: row => {
              if (row.aggregated) return '';
              const { server_type } = row.original;
              switch (server_type) {
                case 'historical':
                  return fillIndicator(row.value);

                case 'middle_manager':
                  const currCapacityUsed = deepGet(row, 'original.currCapacityUsed') || 0;
                  const capacity = deepGet(row, 'original.worker.capacity');
                  if (typeof capacity === 'number') {
                    return `${currCapacityUsed} / ${capacity} (slots)`;
                  } else {
                    return '- / -';
                  }

                default:
                  return '';
              }
            },
            show: hiddenColumns.exists('Usage'),
          },
          {
            Header: 'Detail',
            id: 'queue',
            width: 400,
            filterable: false,
            accessor: row => {
              if (row.server_type === 'middle_manager') {
                if (deepGet(row, 'worker.version') === '') return 'Disabled';

                const details: string[] = [];
                if (row.lastCompletedTaskTime) {
                  details.push(`Last completed task: ${row.lastCompletedTaskTime}`);
                }
                if (row.blacklistedUntil) {
                  details.push(`Blacklisted until: ${row.blacklistedUntil}`);
                }
                return details.join(' ');
              } else {
                return (row.segmentsToLoad || 0) + (row.segmentsToDrop || 0);
              }
            },
            Cell: row => {
              if (row.aggregated) return '';
              const { server_type } = row.original;
              switch (server_type) {
                case 'historical':
                  const {
                    segmentsToLoad,
                    segmentsToLoadSize,
                    segmentsToDrop,
                    segmentsToDropSize,
                  } = row.original;
                  return formatQueues(
                    segmentsToLoad,
                    segmentsToLoadSize,
                    segmentsToDrop,
                    segmentsToDropSize,
                  );

                case 'middle_manager':
                  return row.value;

                default:
                  return '';
              }
            },
            Aggregated: row => {
              if (row.row._pivotVal !== 'historical') return '';
              const originals = row.subRows.map(r => r._original);
              const segmentsToLoad = sum(originals, s => s.segmentsToLoad);
              const segmentsToLoadSize = sum(originals, s => s.segmentsToLoadSize);
              const segmentsToDrop = sum(originals, s => s.segmentsToDrop);
              const segmentsToDropSize = sum(originals, s => s.segmentsToDropSize);
              return formatQueues(
                segmentsToLoad,
                segmentsToLoadSize,
                segmentsToDrop,
                segmentsToDropSize,
              );
            },
            show: hiddenColumns.exists('Detail'),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            id: ACTION_COLUMN_ID,
            width: ACTION_COLUMN_WIDTH,
            accessor: row => row.worker,
            filterable: false,
            Cell: row => {
              if (!row.value) return null;
              const disabled = row.value.version === '';
              const workerActions = this.getWorkerActions(row.value.host, disabled);
              return <ActionCell actions={workerActions} />;
            },
            show: hiddenColumns.exists(ACTION_COLUMN_LABEL),
          },
        ]}
      />
    );
  }

  private getWorkerActions(workerHost: string, disabled: boolean): BasicAction[] {
    if (disabled) {
      return [
        {
          icon: IconNames.TICK,
          title: 'Enable',
          onAction: () => this.setState({ middleManagerEnableWorkerHost: workerHost }),
        },
      ];
    } else {
      return [
        {
          icon: IconNames.DISABLE,
          title: 'Disable',
          onAction: () => this.setState({ middleManagerDisableWorkerHost: workerHost }),
        },
      ];
    }
  }

  renderDisableWorkerAction() {
    const { middleManagerDisableWorkerHost } = this.state;
    if (!middleManagerDisableWorkerHost) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await axios.post(
            `/druid/indexer/v1/worker/${middleManagerDisableWorkerHost}/disable`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Disable worker"
        successText="Worker has been disabled"
        failText="Could not disable worker"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ middleManagerDisableWorkerHost: undefined });
        }}
        onSuccess={() => {
          this.serverQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to disable worker '${middleManagerDisableWorkerHost}'?`}</p>
      </AsyncActionDialog>
    );
  }

  renderEnableWorkerAction() {
    const { middleManagerEnableWorkerHost } = this.state;
    if (!middleManagerEnableWorkerHost) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await axios.post(
            `/druid/indexer/v1/worker/${middleManagerEnableWorkerHost}/enable`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Enable worker"
        successText="Worker has been enabled"
        failText="Could not enable worker"
        intent={Intent.PRIMARY}
        onClose={() => {
          this.setState({ middleManagerEnableWorkerHost: undefined });
        }}
        onSuccess={() => {
          this.serverQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to enable worker '${middleManagerEnableWorkerHost}'?`}</p>
      </AsyncActionDialog>
    );
  }

  renderBulkServersActions() {
    const { goToQuery, noSqlMode } = this.props;

    const bulkserversActionsMenu = (
      <Menu>
        {!noSqlMode && (
          <MenuItem
            icon={IconNames.APPLICATION}
            text="View SQL query for table"
            onClick={() => goToQuery(ServersView.SERVER_SQL)}
          />
        )}
      </Menu>
    );

    return (
      <>
        <Popover content={bulkserversActionsMenu} position={Position.BOTTOM_LEFT}>
          <Button icon={IconNames.MORE} />
        </Popover>
      </>
    );
  }

  render(): JSX.Element {
    const { groupServersBy, hiddenColumns } = this.state;

    return (
      <div className="servers-view app-view">
        <ViewControlBar label="Servers">
          <Label>Group by</Label>
          <ButtonGroup>
            <Button
              active={!groupServersBy}
              onClick={() => this.setState({ groupServersBy: undefined })}
            >
              None
            </Button>
            <Button
              active={groupServersBy === 'server_type'}
              onClick={() => this.setState({ groupServersBy: 'server_type' })}
            >
              Type
            </Button>
            <Button
              active={groupServersBy === 'tier'}
              onClick={() => this.setState({ groupServersBy: 'tier' })}
            >
              Tier
            </Button>
          </ButtonGroup>
          <RefreshButton
            onRefresh={auto => this.serverQueryManager.rerunLastQuery(auto)}
            localStorageKey={LocalStorageKeys.SERVERS_REFRESH_RATE}
          />
          {this.renderBulkServersActions()}
          <TableColumnSelector
            columns={serverTableColumns}
            onChange={column =>
              this.setState(prevState => ({
                hiddenColumns: prevState.hiddenColumns.toggle(column),
              }))
            }
            tableColumnsHidden={hiddenColumns.storedArray}
          />
        </ViewControlBar>
        {this.renderServersTable()}
        {this.renderDisableWorkerAction()}
        {this.renderEnableWorkerAction()}
      </div>
    );
  }
}
