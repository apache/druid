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
import { sum } from 'd3-array';
import React from 'react';
import type { Filter } from 'react-table';
import ReactTable from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  MoreButton,
  RefreshButton,
  TableColumnSelector,
  TableFilterableCell,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import type { QueryWithContext } from '../../druid-models';
import type { Capabilities, CapabilitiesMode } from '../../helpers';
import { STANDARD_TABLE_PAGE_SIZE, STANDARD_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import type { NumberLike } from '../../utils';
import {
  deepGet,
  filterMap,
  formatBytes,
  formatBytesCompact,
  hasPopoverOpen,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  lookupBy,
  oneOf,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
  QueryState,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';

import './services-view.scss';

const tableColumns: Record<CapabilitiesMode, string[]> = {
  'full': [
    'Service',
    'Type',
    'Tier',
    'Host',
    'Port',
    'Current size',
    'Max size',
    'Usage',
    'Start time',
    'Detail',
    ACTION_COLUMN_LABEL,
  ],
  'no-sql': [
    'Service',
    'Type',
    'Tier',
    'Host',
    'Port',
    'Current size',
    'Max size',
    'Usage',
    'Detail',
    ACTION_COLUMN_LABEL,
  ],
  'no-proxy': [
    'Service',
    'Type',
    'Tier',
    'Host',
    'Port',
    'Current size',
    'Max size',
    'Usage',
    'Start time',
  ],
};

function formatQueues(
  segmentsToLoad: NumberLike,
  segmentsToLoadSize: NumberLike,
  segmentsToDrop: NumberLike,
  segmentsToDropSize: NumberLike,
): string {
  const queueParts: string[] = [];
  if (segmentsToLoad) {
    queueParts.push(
      `${pluralIfNeeded(segmentsToLoad, 'segment')} to load (${formatBytesCompact(
        segmentsToLoadSize,
      )})`,
    );
  }
  if (segmentsToDrop) {
    queueParts.push(
      `${pluralIfNeeded(segmentsToDrop, 'segment')} to drop (${formatBytesCompact(
        segmentsToDropSize,
      )})`,
    );
  }
  return queueParts.join(', ') || 'Empty load/drop queues';
}

export interface ServicesViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  capabilities: Capabilities;
}

export interface ServicesViewState {
  servicesState: QueryState<ServiceResultRow[]>;
  groupServicesBy?: 'service_type' | 'tier';

  middleManagerDisableWorkerHost?: string;
  middleManagerEnableWorkerHost?: string;

  visibleColumns: LocalStorageBackedVisibility;
}

interface ServiceResultRow {
  readonly service: string;
  readonly service_type: string;
  readonly tier: string;
  readonly is_leader: number;
  readonly host: string;
  readonly curr_size: NumberLike;
  readonly max_size: NumberLike;
  readonly plaintext_port: number;
  readonly tls_port: number;
  readonly start_time: string;
  loadQueueInfo?: LoadQueueInfo;
  workerInfo?: WorkerInfo;
}

interface LoadQueueInfo {
  readonly segmentsToDrop: NumberLike;
  readonly segmentsToDropSize: NumberLike;
  readonly segmentsToLoad: NumberLike;
  readonly segmentsToLoadSize: NumberLike;
}

interface WorkerInfo {
  readonly availabilityGroups: string[];
  readonly blacklistedUntil: string | null;
  readonly currCapacityUsed: NumberLike;
  readonly lastCompletedTaskTime: string;
  readonly category: string;
  readonly runningTasks: string[];
  readonly worker: {
    readonly capacity: NumberLike;
    readonly host: string;
    readonly ip: string;
    readonly scheme: string;
    readonly version: string;
    readonly category: string;
  };
}

export class ServicesView extends React.PureComponent<ServicesViewProps, ServicesViewState> {
  private readonly serviceQueryManager: QueryManager<Capabilities, ServiceResultRow[]>;

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
  "server" AS "service",
  "server_type" AS "service_type",
  "tier",
  "host",
  "plaintext_port",
  "tls_port",
  "curr_size",
  "max_size",
  "is_leader",
  "start_time"
FROM sys.servers
ORDER BY
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
  ) DESC,
  "service" DESC`;

  static async getServices(): Promise<ServiceResultRow[]> {
    const allServiceResp = await Api.instance.get('/druid/coordinator/v1/servers?simple');
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

  constructor(props: ServicesViewProps) {
    super(props);
    this.state = {
      servicesState: QueryState.INIT,

      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.SERVICE_TABLE_COLUMN_SELECTION,
      ),
    };

    this.serviceQueryManager = new QueryManager({
      processQuery: async capabilities => {
        let services: ServiceResultRow[];
        if (capabilities.hasSql()) {
          services = await queryDruidSql({ query: ServicesView.SERVICE_SQL });
        } else if (capabilities.hasCoordinatorAccess()) {
          services = await ServicesView.getServices();
        } else {
          throw new Error(`must have SQL or coordinator access`);
        }

        if (capabilities.hasCoordinatorAccess()) {
          try {
            const loadQueueInfos = (
              await Api.instance.get<Record<string, LoadQueueInfo>>(
                '/druid/coordinator/v1/loadqueue?simple',
              )
            ).data;
            services.forEach(s => {
              s.loadQueueInfo = loadQueueInfos[s.service];
            });
          } catch {
            AppToaster.show({
              icon: IconNames.ERROR,
              intent: Intent.DANGER,
              message: 'There was an error getting the load queue info',
            });
          }
        }

        if (capabilities.hasOverlordAccess()) {
          try {
            const workerInfos = (await Api.instance.get<WorkerInfo[]>('/druid/indexer/v1/workers'))
              .data;

            const workerInfoLookup: Record<string, WorkerInfo> = lookupBy(
              workerInfos,
              m => m.worker?.host,
            );

            services.forEach(s => {
              s.workerInfo = workerInfoLookup[s.service];
            });
          } catch (e) {
            // Swallow this error because it simply a reflection of a local task runner.
            if (
              deepGet(e, 'response.data.error') !== 'Task Runner does not support worker listing'
            ) {
              AppToaster.show({
                icon: IconNames.ERROR,
                intent: Intent.DANGER,
                message: 'There was an error getting the worker info',
              });
            }
          }
        }

        return services;
      },
      onStateChange: servicesState => {
        this.setState({
          servicesState,
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

  private renderFilterableCell(field: string) {
    const { filters, onFiltersChange } = this.props;

    return (row: { value: any }) => (
      <TableFilterableCell
        field={field}
        value={row.value}
        filters={filters}
        onFiltersChange={onFiltersChange}
      >
        {row.value}
      </TableFilterableCell>
    );
  }

  renderServicesTable() {
    const { capabilities, filters, onFiltersChange } = this.props;
    const { servicesState, groupServicesBy, visibleColumns } = this.state;

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

    const services = servicesState.data || [];
    return (
      <ReactTable
        data={services}
        loading={servicesState.loading}
        noDataText={
          servicesState.isEmpty() ? 'No historicals' : servicesState.getErrorMessage() || ''
        }
        filterable
        filtered={filters}
        onFilteredChange={onFiltersChange}
        pivotBy={groupServicesBy ? [groupServicesBy] : []}
        defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
        pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={services.length > STANDARD_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: 'Service',
            show: visibleColumns.shown('Service'),
            accessor: 'service',
            width: 300,
            Cell: this.renderFilterableCell('service'),
            Aggregated: () => '',
          },
          {
            Header: 'Type',
            show: visibleColumns.shown('Type'),
            accessor: 'service_type',
            width: 150,
            Cell: this.renderFilterableCell('service_type'),
          },
          {
            Header: 'Tier',
            show: visibleColumns.shown('Tier'),
            id: 'tier',
            width: 180,
            accessor: row => {
              if (row.tier) return row.tier;
              return deepGet(row, 'workerInfo.worker.category');
            },
            Cell: this.renderFilterableCell('tier'),
          },
          {
            Header: 'Host',
            show: visibleColumns.shown('Host'),
            accessor: 'host',
            width: 200,
            Cell: this.renderFilterableCell('host'),
            Aggregated: () => '',
          },
          {
            Header: 'Port',
            show: visibleColumns.shown('Port'),
            id: 'port',
            width: 100,
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
            Cell: this.renderFilterableCell('port'),
            Aggregated: () => '',
          },
          {
            Header: 'Current size',
            show: visibleColumns.shown('Current size'),
            id: 'curr_size',
            width: 100,
            filterable: false,
            accessor: 'curr_size',
            className: 'padded',
            Aggregated: ({ subRows }) => {
              const originalRows = subRows.map(r => r._original);
              if (!originalRows.some(r => r.service_type === 'historical')) return '';
              const totalCurr = sum(originalRows, s => s.curr_size);
              return formatBytes(totalCurr);
            },
            Cell: ({ value, aggregated, original }) => {
              if (aggregated || original.service_type !== 'historical') return '';
              if (value === null) return '';
              return formatBytes(value);
            },
          },
          {
            Header: 'Max size',
            show: visibleColumns.shown('Max size'),
            id: 'max_size',
            width: 100,
            filterable: false,
            accessor: 'max_size',
            className: 'padded',
            Aggregated: ({ subRows }) => {
              const originalRows = subRows.map(r => r._original);
              if (!originalRows.some(r => r.service_type === 'historical')) return '';
              const totalMax = sum(originalRows, s => s.max_size);
              return formatBytes(totalMax);
            },
            Cell: ({ value, aggregated, original }) => {
              if (aggregated || original.service_type !== 'historical') return '';
              if (value === null) return '';
              return formatBytes(value);
            },
          },
          {
            Header: 'Usage',
            show: visibleColumns.shown('Usage'),
            id: 'usage',
            width: 140,
            filterable: false,
            className: 'padded',
            accessor: row => {
              if (oneOf(row.service_type, 'middle_manager', 'indexer')) {
                const { workerInfo } = row;
                if (!workerInfo) return 0;
                return (
                  (Number(workerInfo.currCapacityUsed) || 0) / Number(workerInfo.worker?.capacity)
                );
              } else {
                return row.max_size ? Number(row.curr_size) / Number(row.max_size) : null;
              }
            },
            Aggregated: ({ subRows }) => {
              const originalRows = subRows.map(r => r._original);

              if (originalRows.some(r => r.service_type === 'historical')) {
                const totalCurr = sum(originalRows, s => Number(s.curr_size));
                const totalMax = sum(originalRows, s => Number(s.max_size));
                return fillIndicator(totalCurr / totalMax);
              } else if (
                originalRows.some(
                  r => r.service_type === 'indexer' || r.service_type === 'middle_manager',
                )
              ) {
                const workerInfos: WorkerInfo[] = filterMap(originalRows, r => r.workerInfo);

                if (!workerInfos.length) {
                  return 'Could not get worker infos';
                }

                const totalCurrCapacityUsed = sum(
                  workerInfos,
                  w => Number(w.currCapacityUsed) || 0,
                );
                const totalWorkerCapacity = sum(
                  workerInfos,
                  s => deepGet(s, 'worker.capacity') || 0,
                );
                return `Slots used: ${totalCurrCapacityUsed} of ${totalWorkerCapacity}`;
              } else {
                return '';
              }
            },
            Cell: ({ value, aggregated, original }) => {
              if (aggregated) return '';
              const { service_type } = original;
              switch (service_type) {
                case 'historical':
                  return fillIndicator(value);

                case 'indexer':
                case 'middle_manager': {
                  if (!deepGet(original, 'workerInfo')) {
                    return 'Could not get capacity info';
                  }
                  const currCapacityUsed = deepGet(original, 'workerInfo.currCapacityUsed') || 0;
                  const capacity = deepGet(original, 'workerInfo.worker.capacity');
                  if (typeof capacity === 'number') {
                    return `Slots used: ${currCapacityUsed} of ${capacity}`;
                  } else {
                    return 'Slots used: -';
                  }
                }

                default:
                  return '';
              }
            },
          },
          {
            Header: 'Start time',
            show: visibleColumns.shown('Start time'),
            accessor: 'start_time',
            width: 200,
            Cell: this.renderFilterableCell('start_time'),
            Aggregated: () => '',
          },
          {
            Header: 'Detail',
            show: visibleColumns.shown('Detail'),
            id: 'queue',
            width: 400,
            filterable: false,
            className: 'padded',
            accessor: row => {
              switch (row.service_type) {
                case 'middle_manager':
                case 'indexer': {
                  const { workerInfo } = row;
                  if (!workerInfo) {
                    return 'Could not get detail info';
                  }

                  if (workerInfo.worker.version === '') return 'Disabled';

                  const details: string[] = [];
                  if (workerInfo.lastCompletedTaskTime) {
                    details.push(`Last completed task: ${workerInfo.lastCompletedTaskTime}`);
                  }
                  if (workerInfo.blacklistedUntil) {
                    details.push(`Blacklisted until: ${workerInfo.blacklistedUntil}`);
                  }
                  return details.join(' ');
                }

                case 'coordinator':
                case 'overlord':
                  return row.is_leader === 1 ? 'Leader' : '';

                case 'historical': {
                  const { loadQueueInfo } = row;
                  if (!loadQueueInfo) return 0;
                  return (
                    (Number(loadQueueInfo.segmentsToLoad) || 0) +
                    (Number(loadQueueInfo.segmentsToDrop) || 0)
                  );
                }

                default:
                  return 0;
              }
            },
            Cell: ({ value, aggregated, original }) => {
              if (aggregated) return '';
              const { service_type } = original;
              switch (service_type) {
                case 'middle_manager':
                case 'indexer':
                case 'coordinator':
                case 'overlord':
                  return value;

                case 'historical': {
                  const { loadQueueInfo } = original;
                  if (!loadQueueInfo) return 'Could not get load queue info';

                  const { segmentsToLoad, segmentsToLoadSize, segmentsToDrop, segmentsToDropSize } =
                    loadQueueInfo;
                  return formatQueues(
                    segmentsToLoad,
                    segmentsToLoadSize,
                    segmentsToDrop,
                    segmentsToDropSize,
                  );
                }

                default:
                  return '';
              }
            },
            Aggregated: ({ subRows }) => {
              const originalRows = subRows.map(r => r._original);
              if (!originalRows.some(r => r.service_type === 'historical')) return '';

              const loadQueueInfos: LoadQueueInfo[] = filterMap(originalRows, r => r.loadQueueInfo);

              if (!loadQueueInfos.length) {
                return 'Could not get load queue infos';
              }

              const segmentsToLoad = sum(loadQueueInfos, s => Number(s.segmentsToLoad) || 0);
              const segmentsToLoadSize = sum(
                loadQueueInfos,
                s => Number(s.segmentsToLoadSize) || 0,
              );
              const segmentsToDrop = sum(loadQueueInfos, s => Number(s.segmentsToDrop) || 0);
              const segmentsToDropSize = sum(
                loadQueueInfos,
                s => Number(s.segmentsToDropSize) || 0,
              );
              return formatQueues(
                segmentsToLoad,
                segmentsToLoadSize,
                segmentsToDrop,
                segmentsToDropSize,
              );
            },
          },
          {
            Header: ACTION_COLUMN_LABEL,
            show: capabilities.hasOverlordAccess() && visibleColumns.shown(ACTION_COLUMN_LABEL),
            id: ACTION_COLUMN_ID,
            width: ACTION_COLUMN_WIDTH,
            accessor: row => row.workerInfo,
            filterable: false,
            Cell: ({ value, aggregated }) => {
              if (aggregated) return '';
              if (!value) return null;
              const { worker } = value;
              const disabled = worker.version === '';
              const workerActions = this.getWorkerActions(worker.host, disabled);
              return <ActionCell actions={workerActions} />;
            },
            Aggregated: () => '',
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
          const resp = await Api.instance.post(
            `/druid/indexer/v1/worker/${Api.encodePath(middleManagerDisableWorkerHost)}/disable`,
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
          const resp = await Api.instance.post(
            `/druid/indexer/v1/worker/${Api.encodePath(middleManagerEnableWorkerHost)}/enable`,
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
            onClick={() => goToQuery({ queryString: ServicesView.SERVICE_SQL })}
          />
        )}
      </MoreButton>
    );
  }

  render() {
    const { capabilities } = this.props;
    const { groupServicesBy, visibleColumns } = this.state;

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
            onRefresh={auto => {
              if (auto && hasPopoverOpen()) return;
              this.serviceQueryManager.rerunLastQuery(auto);
            }}
            localStorageKey={LocalStorageKeys.SERVICES_REFRESH_RATE}
          />
          {this.renderBulkServicesActions()}
          <TableColumnSelector
            columns={tableColumns[capabilities.getMode()]}
            onChange={column =>
              this.setState(prevState => ({
                visibleColumns: prevState.visibleColumns.toggle(column),
              }))
            }
            tableColumnsHidden={visibleColumns.getHiddenColumns()}
          />
        </ViewControlBar>
        {this.renderServicesTable()}
        {this.renderDisableWorkerAction()}
        {this.renderEnableWorkerAction()}
      </div>
    );
  }
}
