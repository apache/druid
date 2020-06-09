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

import { Button, ButtonGroup, Intent, Label, MenuItem } from '@blueprintjs/core';
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
  MoreButton,
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
import { Capabilities, CapabilitiesMode } from '../../utils/capabilities';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';
import { deepGet } from '../../utils/object-change';

import './services-view.scss';

const allColumns: string[] = [
  'Service',
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

const tableColumns: Record<CapabilitiesMode, string[]> = {
  full: allColumns,
  'no-sql': allColumns,
  'no-proxy': ['Service', 'Type', 'Tier', 'Host', 'Port', 'Curr size', 'Max size', 'Usage'],
};

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

export interface ServicesViewProps {
  middleManager: string | undefined;
  goToQuery: (initSql: string) => void;
  goToTask: (taskId: string) => void;
  capabilities: Capabilities;
}

export interface ServicesViewState {
  servicesLoading: boolean;
  services?: any[];
  servicesError?: string;
  serviceFilter: Filter[];
  groupServicesBy?: 'service_type' | 'tier';

  middleManagerDisableWorkerHost?: string;
  middleManagerEnableWorkerHost?: string;

  hiddenColumns: LocalStorageBackedArray<string>;
}

interface ServiceQueryResultRow {
  service: string;
  service_type: string;
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
  category: string;
  runningTasks: string[];
  worker: {
    capacity: number;
    host: string;
    ip: string;
    scheme: string;
    version: string;
  };
}

interface ServiceResultRow
  extends ServiceQueryResultRow,
    Partial<LoadQueueStatus>,
    Partial<MiddleManagerQueryResultRow> {}

export class ServicesView extends React.PureComponent<ServicesViewProps, ServicesViewState> {
  private serviceQueryManager: QueryManager<Capabilities, ServiceResultRow[]>;

  // Ranking
  //   coordinator => 8
  //   overlord => 7
  //   router => 6
  //   broker => 5
  //   historical => 4
  //   indexer => 3
  //   middle_manager => 2
  //   peon => 1

  static SERVICE_SQL = `SELECT
  "server" AS "service", "server_type" AS "service_type", "tier", "host", "plaintext_port", "tls_port", "curr_size", "max_size",
  (
    CASE "server_type"
    WHEN 'coordinator' THEN 8
    WHEN 'overlord' THEN 7
    WHEN 'router' THEN 6
    WHEN 'broker' THEN 5
    WHEN 'historical' THEN 4
    WHEN 'indexer' THEN 3
    WHEN 'middle_manager' THEN 2
    WHEN 'peon' THEN 1
    ELSE 0
    END
  ) AS "rank"
FROM sys.servers
ORDER BY "rank" DESC, "service" DESC`;

  static async getServices(): Promise<ServiceQueryResultRow[]> {
    const allServiceResp = await axios.get('/druid/coordinator/v1/servers?simple');
    const allServices = allServiceResp.data;
    return allServices.map((s: any) => {
      return {
        service: s.host,
        service_type: s.type === 'indexer-executor' ? 'peon' : s.type,
        tier: s.tier,
        host: s.host.split(':')[0],
        plaintext_port: parseInt(s.host.split(':')[1], 10),
        curr_size: s.currSize,
        max_size: s.maxSize,
        tls_port: -1,
      };
    });
  }

  constructor(props: ServicesViewProps, context: any) {
    super(props, context);
    this.state = {
      servicesLoading: true,
      serviceFilter: [],

      hiddenColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.SERVICE_TABLE_COLUMN_SELECTION,
      ),
    };

    this.serviceQueryManager = new QueryManager({
      processQuery: async capabilities => {
        let services: ServiceQueryResultRow[];
        if (capabilities.hasSql()) {
          services = await queryDruidSql({ query: ServicesView.SERVICE_SQL });
        } else if (capabilities.hasCoordinatorAccess()) {
          services = await ServicesView.getServices();
        } else {
          throw new Error(`must have SQL or coordinator access`);
        }

        if (capabilities.hasCoordinatorAccess()) {
          const loadQueueResponse = await axios.get('/druid/coordinator/v1/loadqueue?simple');
          const loadQueues: Record<string, LoadQueueStatus> = loadQueueResponse.data;
          services = services.map(s => {
            const loadQueueInfo = loadQueues[s.service];
            if (loadQueueInfo) {
              s = Object.assign(s, loadQueueInfo);
            }
            return s;
          });
        }

        if (capabilities.hasOverlordAccess()) {
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

          services = services.map(s => {
            const middleManagerInfo = middleManagersLookup[s.service];
            if (middleManagerInfo) {
              s = Object.assign(s, middleManagerInfo);
            }
            return s;
          });
        }

        return services;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          services: result,
          servicesLoading: loading,
          servicesError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;
    this.serviceQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.serviceQueryManager.terminate();
  }

  renderServicesTable() {
    const { capabilities } = this.props;
    const {
      services,
      servicesLoading,
      servicesError,
      serviceFilter,
      groupServicesBy,
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
        data={services || []}
        loading={servicesLoading}
        noDataText={
          !servicesLoading && services && !services.length ? 'No historicals' : servicesError || ''
        }
        filterable
        filtered={serviceFilter}
        onFilteredChange={filtered => {
          this.setState({ serviceFilter: filtered });
        }}
        pivotBy={groupServicesBy ? [groupServicesBy] : []}
        defaultPageSize={50}
        columns={[
          {
            Header: 'Service',
            accessor: 'service',
            width: 300,
            Aggregated: () => '',
            show: hiddenColumns.exists('Service'),
          },
          {
            Header: 'Type',
            accessor: 'service_type',
            width: 150,
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({
                      serviceFilter: addFilter(serviceFilter, 'service_type', value),
                    });
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
            id: 'tier',
            accessor: row => {
              return row.tier ? row.tier : row.worker ? row.worker.category : null;
            },
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({ serviceFilter: addFilter(serviceFilter, 'tier', value) });
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
              if (row.aggregated || row.original.service_type !== 'historical') return '';
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
              if (row.aggregated || row.original.service_type !== 'historical') return '';
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
              if (row.service_type === 'middle_manager' || row.service_type === 'indexer') {
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

                case 'indexer':
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
              const { service_type } = row.original;
              switch (service_type) {
                case 'historical':
                  return fillIndicator(row.value);

                case 'indexer':
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
              if (row.service_type === 'middle_manager' || row.service_type === 'indexer') {
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
              const { service_type } = row.original;
              switch (service_type) {
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

                case 'indexer':
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
            show: capabilities.hasCoordinatorAccess() && hiddenColumns.exists('Detail'),
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
            show: capabilities.hasOverlordAccess() && hiddenColumns.exists(ACTION_COLUMN_LABEL),
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
          this.serviceQueryManager.rerunLastQuery();
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
          this.serviceQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to enable worker '${middleManagerEnableWorkerHost}'?`}</p>
      </AsyncActionDialog>
    );
  }

  renderBulkServicesActions() {
    const { goToQuery, capabilities } = this.props;

    return (
      <MoreButton>
        {capabilities.hasSql() && (
          <MenuItem
            icon={IconNames.APPLICATION}
            text="View SQL query for table"
            onClick={() => goToQuery(ServicesView.SERVICE_SQL)}
          />
        )}
      </MoreButton>
    );
  }

  render(): JSX.Element {
    const { capabilities } = this.props;
    const { groupServicesBy, hiddenColumns } = this.state;

    return (
      <div className="services-view app-view">
        <ViewControlBar label="Services">
          <Label>Group by</Label>
          <ButtonGroup>
            <Button
              active={!groupServicesBy}
              onClick={() => this.setState({ groupServicesBy: undefined })}
            >
              None
            </Button>
            <Button
              active={groupServicesBy === 'service_type'}
              onClick={() => this.setState({ groupServicesBy: 'service_type' })}
            >
              Type
            </Button>
            <Button
              active={groupServicesBy === 'tier'}
              onClick={() => this.setState({ groupServicesBy: 'tier' })}
            >
              Tier
            </Button>
          </ButtonGroup>
          <RefreshButton
            onRefresh={auto => this.serviceQueryManager.rerunLastQuery(auto)}
            localStorageKey={LocalStorageKeys.SERVICES_REFRESH_RATE}
          />
          {this.renderBulkServicesActions()}
          <TableColumnSelector
            columns={tableColumns[capabilities.getMode()]}
            onChange={column =>
              this.setState(prevState => ({
                hiddenColumns: prevState.hiddenColumns.toggle(column),
              }))
            }
            tableColumnsHidden={hiddenColumns.storedArray}
          />
        </ViewControlBar>
        {this.renderServicesTable()}
        {this.renderDisableWorkerAction()}
        {this.renderEnableWorkerAction()}
      </div>
    );
  }
}
