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
import ReactTable, { Filter } from 'react-table';

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
import { QueryWithContext } from '../../druid-models';
import { STANDARD_TABLE_PAGE_SIZE, STANDARD_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import { Api } from '../../singletons';
import {
  Capabilities,
  CapabilitiesMode,
  deepGet,
  formatBytes,
  formatBytesCompact,
  hasPopoverOpen,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  lookupBy,
  NumberLike,
  oneOf,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
  QueryState,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';

import './services-view.scss';

const allColumns: string[] = [
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
];

const tableColumns: Record<CapabilitiesMode, string[]> = {
  'full': allColumns,
  'no-sql': allColumns,
  'no-proxy': ['Service', 'Type', 'Tier', 'Host', 'Port', 'Current size', 'Max size', 'Usage'],
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
  goToQuery(queryWithContext: QueryWithContext): void;
  capabilities: Capabilities;
}

export interface ServicesViewState {
  servicesState: QueryState<ServiceResultRow[]>;
  serviceFilter: Filter[];
  groupServicesBy?: 'service_type' | 'tier';

  middleManagerDisableWorkerHost?: string;
  middleManagerEnableWorkerHost?: string;

  visibleColumns: LocalStorageBackedVisibility;
}

interface ServiceQueryResultRow {
  readonly service: string;
  readonly service_type: string;
  readonly tier: string;
  readonly is_leader: number;
  readonly host: string;
  readonly curr_size: NumberLike;
  readonly max_size: NumberLike;
  readonly plaintext_port: number;
  readonly tls_port: number;
}

interface LoadQueueStatus {
  readonly segmentsToDrop: NumberLike;
  readonly segmentsToDropSize: NumberLike;
  readonly segmentsToLoad: NumberLike;
  readonly segmentsToLoadSize: NumberLike;
}

interface MiddleManagerQueryResultRow {
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

interface ServiceResultRow
  extends ServiceQueryResultRow,
    Partial<LoadQueueStatus>,
    Partial<MiddleManagerQueryResultRow> {}

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
  "is_leader"
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

  static async getServices(): Promise<ServiceQueryResultRow[]> {
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

  constructor(props: ServicesViewProps, context: any) {
    super(props, context);
    this.state = {
      servicesState: QueryState.INIT,
      serviceFilter: [],

      visibleColumns: new LocalStorageBackedVisibility(
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
          const loadQueueResponse = await Api.instance.get(
            '/druid/coordinator/v1/loadqueue?simple',
          );
          const loadQueues: Record<string, LoadQueueStatus> = loadQueueResponse.data;
          services = services.map(s => {
            const loadQueueInfo = loadQueues[s.service];
            if (loadQueueInfo) {
              s = { ...s, ...loadQueueInfo };
            }
            return s;
          });
        }

        if (capabilities.hasOverlordAccess()) {
          let middleManagers: MiddleManagerQueryResultRow[];
          try {
            const middleManagerResponse = await Api.instance.get('/druid/indexer/v1/workers');
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

          const middleManagersLookup: Record<string, MiddleManagerQueryResultRow> = lookupBy(
            middleManagers,
            m => m.worker.host,
          );

          services = services.map(s => {
            const middleManagerInfo = middleManagersLookup[s.service];
            if (middleManagerInfo) {
              s = { ...s, ...middleManagerInfo };
            }
            return s;
          });
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
    const { serviceFilter } = this.state;

    return (row: { value: any }) => (
      <TableFilterableCell
        field={field}
        value={row.value}
        filters={serviceFilter}
        onFiltersChange={filters => this.setState({ serviceFilter: filters })}
      >
        {row.value}
      </TableFilterableCell>
    );
  }

  renderServicesTable() {
    const { capabilities } = this.props;
    const { servicesState, serviceFilter, groupServicesBy, visibleColumns } = this.state;

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
        filtered={serviceFilter}
        onFilteredChange={filtered => {
          this.setState({ serviceFilter: filtered });
        }}
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
              return row.tier ? row.tier : row.worker ? row.worker.category : null;
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
          },
          {
            Header: 'Max size',
            show: visibleColumns.shown('Max size'),
            id: 'max_size',
            width: 100,
            filterable: false,
            accessor: 'max_size',
            className: 'padded',
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
                return row.worker
                  ? (Number(row.currCapacityUsed) || 0) / Number(row.worker.capacity)
                  : null;
              } else {
                return row.max_size ? Number(row.curr_size) / Number(row.max_size) : null;
              }
            },
            Aggregated: row => {
              switch (row.row._pivotVal) {
                case 'historical': {
                  const originalHistoricals: ServiceResultRow[] = row.subRows.map(r => r._original);
                  const totalCurr = sum(originalHistoricals, s => Number(s.curr_size));
                  const totalMax = sum(originalHistoricals, s => Number(s.max_size));
                  return fillIndicator(totalCurr / totalMax);
                }

                case 'indexer':
                case 'middle_manager': {
                  const originalMiddleManagers: ServiceResultRow[] = row.subRows.map(
                    r => r._original,
                  );
                  const totalCurrCapacityUsed = sum(
                    originalMiddleManagers,
                    s => Number(s.currCapacityUsed) || 0,
                  );
                  const totalWorkerCapacity = sum(
                    originalMiddleManagers,
                    s => deepGet(s, 'worker.capacity') || 0,
                  );
                  return `${totalCurrCapacityUsed} / ${totalWorkerCapacity} (total slots)`;
                }

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
                case 'middle_manager': {
                  const currCapacityUsed = deepGet(row, 'original.currCapacityUsed') || 0;
                  const capacity = deepGet(row, 'original.worker.capacity');
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
            Header: 'Detail',
            show: visibleColumns.shown('Detail'),
            id: 'queue',
            width: 400,
            filterable: false,
            className: 'padded',
            accessor: row => {
              if (oneOf(row.service_type, 'middle_manager', 'indexer')) {
                if (deepGet(row, 'worker.version') === '') return 'Disabled';

                const details: string[] = [];
                if (row.lastCompletedTaskTime) {
                  details.push(`Last completed task: ${row.lastCompletedTaskTime}`);
                }
                if (row.blacklistedUntil) {
                  details.push(`Blacklisted until: ${row.blacklistedUntil}`);
                }
                return details.join(' ');
              } else if (oneOf(row.service_type, 'coordinator', 'overlord')) {
                return row.is_leader === 1 ? 'Leader' : '';
              } else {
                return (Number(row.segmentsToLoad) || 0) + (Number(row.segmentsToDrop) || 0);
              }
            },
            Cell: row => {
              if (row.aggregated) return '';
              const { service_type } = row.original;
              switch (service_type) {
                case 'historical': {
                  const { segmentsToLoad, segmentsToLoadSize, segmentsToDrop, segmentsToDropSize } =
                    row.original;
                  return formatQueues(
                    segmentsToLoad,
                    segmentsToLoadSize,
                    segmentsToDrop,
                    segmentsToDropSize,
                  );
                }

                case 'indexer':
                case 'middle_manager':
                case 'coordinator':
                case 'overlord':
                  return row.value;

                default:
                  return '';
              }
            },
            Aggregated: row => {
              if (row.row._pivotVal !== 'historical') return '';
              const originals: ServiceResultRow[] = row.subRows.map(r => r._original);
              const segmentsToLoad = sum(originals, s => Number(s.segmentsToLoad) || 0);
              const segmentsToLoadSize = sum(originals, s => Number(s.segmentsToLoadSize) || 0);
              const segmentsToDrop = sum(originals, s => Number(s.segmentsToDrop) || 0);
              const segmentsToDropSize = sum(originals, s => Number(s.segmentsToDropSize) || 0);
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
            accessor: row => row.worker,
            filterable: false,
            Cell: ({ value, aggregated }) => {
              if (aggregated) return '';
              if (!value) return null;
              const disabled = value.version === '';
              const workerActions = this.getWorkerActions(value.host, disabled);
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

  render(): JSX.Element {
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
