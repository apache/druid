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

import { Icon, Intent, Menu, MenuItem, Position, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import * as JSONBig from 'json-bigint-native';
import type { JSX } from 'react';
import React from 'react';
import type { Filter } from 'react-table';
import ReactTable from 'react-table';

import type { TableColumnSelectorColumn } from '../../components';
import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  MoreButton,
  RefreshButton,
  TableClickableCell,
  TableColumnSelector,
  TableFilterableCell,
  ViewControlBar,
} from '../../components';
import {
  AlertDialog,
  AsyncActionDialog,
  SpecDialog,
  SupervisorTableActionDialog,
} from '../../dialogs';
import { SupervisorResetOffsetsDialog } from '../../dialogs/supervisor-reset-offsets-dialog/supervisor-reset-offsets-dialog';
import type {
  IngestionSpec,
  QueryWithContext,
  RowStatsKey,
  SupervisorStatus,
  SupervisorStatusTask,
} from '../../druid-models';
import { getTotalSupervisorStats } from '../../druid-models';
import type { Capabilities } from '../../helpers';
import {
  SMALL_TABLE_PAGE_SIZE,
  SMALL_TABLE_PAGE_SIZE_OPTIONS,
  sqlQueryCustomTableFilter,
} from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import type { TableState } from '../../utils';
import {
  assemble,
  checkedCircleIcon,
  deepGet,
  formatByteRate,
  formatBytes,
  formatInteger,
  formatRate,
  getDruidErrorMessage,
  hasPopoverOpen,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  nonEmptyArray,
  oneOf,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
  QueryState,
  sortedToOrderByClause,
  twoLines,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';

import './supervisors-view.scss';

const SUPERVISOR_TABLE_COLUMNS: TableColumnSelectorColumn[] = [
  'Supervisor ID',
  'Type',
  'Topic/Stream',
  'Status',
  'Configured tasks',
  { text: 'Running tasks', label: 'status API' },
  { text: 'Aggregate lag', label: 'status API' },
  { text: 'Recent errors', label: 'status API' },
  { text: 'Stats', label: 'stats API' },
];

const ROW_STATS_KEYS: RowStatsKey[] = ['1m', '5m', '15m'];
const STATUS_API_TIMEOUT = 5000;
const STATS_API_TIMEOUT = 5000;

function getRowStatsKeyTitle(key: RowStatsKey) {
  return `Rate over past ${pluralIfNeeded(parseInt(key, 10), 'minute')}`;
}

function getRowStatsKeySeconds(key: RowStatsKey): number {
  return parseInt(key, 10) * 60;
}

interface SupervisorQuery extends TableState {
  capabilities: Capabilities;
  visibleColumns: LocalStorageBackedVisibility;
}

interface SupervisorQueryResultRow {
  supervisor_id: string;
  type: string;
  source: string;
  detailed_state: string;
  spec?: IngestionSpec;
  suspended: boolean;
  status?: SupervisorStatus;
  stats?: any;
}

export interface SupervisorsViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
  openSupervisorDialog: boolean | undefined;
  goToDatasource(datasource: string): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  goToStreamingDataLoader(supervisorId: string): void;
  goToTasks(supervisorId: string, type: string): void;
  capabilities: Capabilities;
}

export interface SupervisorsViewState {
  supervisorsState: QueryState<SupervisorQueryResultRow[]>;
  statsKey: RowStatsKey;

  resumeSupervisorId?: string;
  suspendSupervisorId?: string;
  resetOffsetsSupervisorInfo?: { id: string; type: string };
  resetSupervisorId?: string;
  terminateSupervisorId?: string;

  showResumeAllSupervisors: boolean;
  showSuspendAllSupervisors: boolean;
  showTerminateAllSupervisors: boolean;

  supervisorSpecDialogOpen: boolean;
  alertErrorMsg?: string;

  supervisorTableActionDialogId?: string;
  supervisorTableActionDialogActions: BasicAction[];
  visibleColumns: LocalStorageBackedVisibility;
}

function detailedStateToColor(detailedState: string): string {
  switch (detailedState) {
    case 'UNHEALTHY_SUPERVISOR':
    case 'UNHEALTHY_TASKS':
    case 'UNABLE_TO_CONNECT_TO_STREAM':
    case 'LOST_CONTACT_WITH_STREAM':
      return '#d5100a';

    case 'PENDING':
      return '#00eaff';

    case 'DISCOVERING_INITIAL_TASKS':
    case 'CREATING_TASKS':
    case 'CONNECTING_TO_STREAM':
    case 'RUNNING':
      return '#2167d5';

    case 'IDLE':
      return '#44659d';

    case 'STOPPING':
      return '#e75c06';

    case `SUSPENDED`:
      return '#ffbf00';

    default:
      return '#0a1500';
  }
}

export class SupervisorsView extends React.PureComponent<
  SupervisorsViewProps,
  SupervisorsViewState
> {
  private readonly supervisorQueryManager: QueryManager<
    SupervisorQuery,
    SupervisorQueryResultRow[]
  >;

  constructor(props: SupervisorsViewProps) {
    super(props);

    this.state = {
      supervisorsState: QueryState.INIT,
      statsKey: '5m',

      showResumeAllSupervisors: false,
      showSuspendAllSupervisors: false,
      showTerminateAllSupervisors: false,

      supervisorSpecDialogOpen: Boolean(props.openSupervisorDialog),

      supervisorTableActionDialogActions: [],

      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.SUPERVISOR_TABLE_COLUMN_SELECTION,
      ),
    };

    this.supervisorQueryManager = new QueryManager({
      processQuery: async (
        { capabilities, visibleColumns, filtered, sorted, page, pageSize },
        cancelToken,
        setIntermediateQuery,
      ) => {
        let supervisors: SupervisorQueryResultRow[];
        if (capabilities.hasSql()) {
          const sqlQuery = assemble(
            'WITH s AS (SELECT',
            '  "supervisor_id",',
            '  "type",',
            '  "source",',
            `  CASE WHEN "suspended" = 0 THEN "detailed_state" ELSE 'SUSPENDED' END AS "detailed_state",`,
            visibleColumns.shown('Configured tasks') ? '  "spec",' : undefined,
            '  "suspended" = 1 AS "suspended"',
            'FROM "sys"."supervisors")',
            'SELECT *',
            'FROM s',
            filtered.length
              ? `WHERE ${filtered.map(sqlQueryCustomTableFilter).join(' AND ')}`
              : undefined,
            sortedToOrderByClause(sorted),
            `LIMIT ${pageSize}`,
            page ? `OFFSET ${page * pageSize}` : undefined,
          ).join('\n');
          setIntermediateQuery(sqlQuery);
          supervisors = await queryDruidSql<SupervisorQueryResultRow>(
            {
              query: sqlQuery,
            },
            cancelToken,
          );

          for (const supervisor of supervisors) {
            const spec: any = supervisor.spec;
            if (typeof spec === 'string') {
              supervisor.spec = JSONBig.parse(spec);
            }
          }
        } else if (capabilities.hasOverlordAccess()) {
          const supervisorList = (
            await Api.instance.get('/druid/indexer/v1/supervisor?full', { cancelToken })
          ).data;
          if (!Array.isArray(supervisorList)) {
            throw new Error(`Unexpected result from /druid/indexer/v1/supervisor?full`);
          }
          supervisors = supervisorList.map((sup: any) => {
            return {
              supervisor_id: deepGet(sup, 'id'),
              type: deepGet(sup, 'spec.tuningConfig.type'),
              source:
                deepGet(sup, 'spec.ioConfig.topic') ||
                deepGet(sup, 'spec.ioConfig.stream') ||
                'n/a',
              state: deepGet(sup, 'state'),
              detailed_state: deepGet(sup, 'detailedState'),
              spec: sup.spec,
              suspended: Boolean(deepGet(sup, 'suspended')),
            };
          });

          const firstSorted = sorted[0];
          if (firstSorted) {
            const { id, desc } = firstSorted;
            supervisors.sort((s1: any, s2: any) => {
              return (
                String(s1[id]).localeCompare(String(s2[id]), undefined, { numeric: true }) *
                (desc ? -1 : 1)
              );
            });
          }
        } else {
          throw new Error(`must have SQL or overlord access`);
        }

        if (capabilities.hasOverlordAccess()) {
          let showIssue = (message: string) => {
            showIssue = () => {}; // Only show once
            AppToaster.show({
              icon: IconNames.ERROR,
              intent: Intent.DANGER,
              message,
            });
          };

          if (visibleColumns.shown('Running tasks', 'Aggregate lag', 'Recent errors')) {
            try {
              for (const supervisor of supervisors) {
                cancelToken.throwIfRequested();
                supervisor.status = (
                  await Api.instance.get(
                    `/druid/indexer/v1/supervisor/${Api.encodePath(
                      supervisor.supervisor_id,
                    )}/status`,
                    { cancelToken, timeout: STATUS_API_TIMEOUT },
                  )
                ).data;
              }
            } catch (e) {
              showIssue('Could not get status');
            }
          }

          if (visibleColumns.shown('Stats')) {
            try {
              for (const supervisor of supervisors) {
                cancelToken.throwIfRequested();
                supervisor.stats = (
                  await Api.instance.get(
                    `/druid/indexer/v1/supervisor/${Api.encodePath(
                      supervisor.supervisor_id,
                    )}/stats`,
                    { cancelToken, timeout: STATS_API_TIMEOUT },
                  )
                ).data;
              }
            } catch (e) {
              showIssue('Could not get stats');
            }
          }
        }

        return supervisors;
      },
      onStateChange: supervisorsState => {
        this.setState({
          supervisorsState,
        });
      },
    });
  }

  private lastTableState: TableState | undefined;

  componentWillUnmount(): void {
    this.supervisorQueryManager.terminate();
  }

  private readonly fetchData = (tableState?: TableState) => {
    const { capabilities } = this.props;
    const { visibleColumns } = this.state;
    if (tableState) this.lastTableState = tableState;
    if (!this.lastTableState) return;
    const { page, pageSize, filtered, sorted } = this.lastTableState;
    this.supervisorQueryManager.runQuery({
      page,
      pageSize,
      filtered,
      sorted,
      visibleColumns,
      capabilities,
    });
  };

  private readonly closeSpecDialogs = () => {
    this.setState({
      supervisorSpecDialogOpen: false,
    });
  };

  private readonly submitSupervisor = async (spec: JSON) => {
    try {
      await Api.instance.post('/druid/indexer/v1/supervisor', spec);
    } catch (e) {
      AppToaster.show({
        message: `Failed to submit supervisor: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
      return;
    }

    AppToaster.show({
      message: 'Supervisor submitted successfully',
      intent: Intent.SUCCESS,
    });
    this.supervisorQueryManager.rerunLastQuery();
  };

  private getSupervisorActions(
    id: string,
    supervisorSuspended: boolean,
    type: string,
  ): BasicAction[] {
    const { goToDatasource, goToStreamingDataLoader } = this.props;

    const actions: BasicAction[] = [];
    if (oneOf(type, 'kafka', 'kinesis')) {
      actions.push(
        {
          icon: IconNames.MULTI_SELECT,
          title: 'Go to datasource',
          onAction: () => goToDatasource(id),
        },
        {
          icon: IconNames.CLOUD_UPLOAD,
          title: 'Open in data loader',
          onAction: () => goToStreamingDataLoader(id),
        },
      );
    }
    actions.push(
      {
        icon: supervisorSuspended ? IconNames.PLAY : IconNames.PAUSE,
        title: supervisorSuspended ? 'Resume' : 'Suspend',
        onAction: () =>
          supervisorSuspended
            ? this.setState({ resumeSupervisorId: id })
            : this.setState({ suspendSupervisorId: id }),
      },
      {
        icon: IconNames.STEP_BACKWARD,
        title: `Set ${type === 'kinesis' ? 'sequence numbers' : 'offsets'}`,
        onAction: () => this.setState({ resetOffsetsSupervisorInfo: { id, type } }),
        disabledReason: supervisorSuspended ? undefined : `Supervisor must be suspended`,
      },
      {
        icon: IconNames.STEP_BACKWARD,
        title: 'Hard reset',
        intent: Intent.DANGER,
        onAction: () => this.setState({ resetSupervisorId: id }),
        disabledReason: supervisorSuspended ? undefined : `Supervisor must be suspended`,
      },
      {
        icon: IconNames.CROSS,
        title: 'Terminate',
        intent: Intent.DANGER,
        onAction: () => this.setState({ terminateSupervisorId: id }),
      },
    );
    return actions;
  }

  renderResumeSupervisorAction() {
    const { resumeSupervisorId } = this.state;
    if (!resumeSupervisorId) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(
            `/druid/indexer/v1/supervisor/${Api.encodePath(resumeSupervisorId)}/resume`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Resume supervisor"
        successText="Supervisor has been resumed"
        failText="Could not resume supervisor"
        intent={Intent.PRIMARY}
        onClose={() => {
          this.setState({ resumeSupervisorId: undefined });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      >
        <p>
          Are you sure you want to resume supervisor <Tag minimal>{resumeSupervisorId}</Tag>?
        </p>
      </AsyncActionDialog>
    );
  }

  renderSuspendSupervisorAction() {
    const { suspendSupervisorId } = this.state;
    if (!suspendSupervisorId) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(
            `/druid/indexer/v1/supervisor/${Api.encodePath(suspendSupervisorId)}/suspend`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Suspend supervisor"
        successText="Supervisor has been suspended"
        failText="Could not suspend supervisor"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ suspendSupervisorId: undefined });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      >
        <p>
          Are you sure you want to suspend supervisor <Tag minimal>{suspendSupervisorId}</Tag>?
        </p>
      </AsyncActionDialog>
    );
  }

  renderResetOffsetsSupervisorAction() {
    const { resetOffsetsSupervisorInfo } = this.state;
    if (!resetOffsetsSupervisorInfo) return;

    return (
      <SupervisorResetOffsetsDialog
        supervisorId={resetOffsetsSupervisorInfo.id}
        supervisorType={resetOffsetsSupervisorInfo.type}
        onClose={() => {
          this.setState({ resetOffsetsSupervisorInfo: undefined });
        }}
      />
    );
  }

  renderResetSupervisorAction() {
    const { resetSupervisorId } = this.state;
    if (!resetSupervisorId) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(
            `/druid/indexer/v1/supervisor/${Api.encodePath(resetSupervisorId)}/reset`,
            '',
          );
          return resp.data;
        }}
        confirmButtonText="Hard reset supervisor"
        successText="Supervisor has been hard reset"
        failText="Could not hard reset supervisor"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ resetSupervisorId: undefined });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
        warningChecks={[
          <>
            I understand that resetting <Tag minimal>{resetSupervisorId}</Tag> will clear
            checkpoints and may lead to data loss or duplication.
          </>,
          'I understand that this operation cannot be undone.',
        ]}
      >
        <p>
          Are you sure you want to hard reset supervisor <Tag minimal>{resetSupervisorId}</Tag>?
        </p>
        <p>Hard resetting a supervisor may lead to data loss or data duplication.</p>
        <p>
          Use this operation to restore functionality when the supervisor stops operating due to
          missing offsets.
        </p>
      </AsyncActionDialog>
    );
  }

  renderTerminateSupervisorAction() {
    const { terminateSupervisorId } = this.state;
    if (!terminateSupervisorId) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(
            `/druid/indexer/v1/supervisor/${Api.encodePath(terminateSupervisorId)}/terminate`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Terminate supervisor"
        successText="Supervisor has been terminated"
        failText="Could not terminate supervisor"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ terminateSupervisorId: undefined });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      >
        <p>
          Are you sure you want to terminate supervisor <Tag minimal>{terminateSupervisorId}</Tag>?
        </p>
        <p>This action is not reversible.</p>
      </AsyncActionDialog>
    );
  }

  private renderSupervisorFilterableCell(field: string) {
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

  private onSupervisorDetail(supervisor: SupervisorQueryResultRow) {
    this.setState({
      supervisorTableActionDialogId: supervisor.supervisor_id,
      supervisorTableActionDialogActions: this.getSupervisorActions(
        supervisor.supervisor_id,
        supervisor.suspended,
        supervisor.type,
      ),
    });
  }

  private renderSupervisorTable() {
    const { goToTasks, filters, onFiltersChange } = this.props;
    const { supervisorsState, statsKey, visibleColumns } = this.state;

    const supervisors = supervisorsState.data || [];
    return (
      <ReactTable
        data={supervisors}
        loading={supervisorsState.loading}
        noDataText={
          supervisorsState.isEmpty() ? 'No supervisors' : supervisorsState.getErrorMessage() || ''
        }
        filtered={filters}
        onFilteredChange={onFiltersChange}
        filterable
        onFetchData={tableState => {
          this.fetchData(tableState);
        }}
        defaultPageSize={SMALL_TABLE_PAGE_SIZE}
        pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={supervisors.length > SMALL_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: twoLines('Supervisor ID', <i>(datasource)</i>),
            id: 'supervisor_id',
            accessor: 'supervisor_id',
            width: 280,
            show: visibleColumns.shown('Supervisor ID'),
            Cell: ({ value, original }) => (
              <TableClickableCell
                onClick={() => this.onSupervisorDetail(original)}
                hoverIcon={IconNames.SEARCH_TEMPLATE}
              >
                {value}
              </TableClickableCell>
            ),
          },
          {
            Header: 'Type',
            accessor: 'type',
            width: 80,
            Cell: this.renderSupervisorFilterableCell('type'),
            show: visibleColumns.shown('Type'),
          },
          {
            Header: 'Topic/Stream',
            accessor: 'source',
            width: 200,
            Cell: this.renderSupervisorFilterableCell('source'),
            show: visibleColumns.shown('Topic/Stream'),
          },
          {
            Header: 'Status',
            id: 'detailed_state',
            width: 130,
            accessor: 'detailed_state',
            Cell: ({ value }) => (
              <TableFilterableCell
                field="status"
                value={value}
                filters={filters}
                onFiltersChange={onFiltersChange}
              >
                <span>
                  <span style={{ color: detailedStateToColor(value) }}>&#x25cf;&nbsp;</span>
                  {value}
                </span>
              </TableFilterableCell>
            ),
            show: visibleColumns.shown('Status'),
          },
          {
            Header: 'Configured tasks',
            id: 'configured_tasks',
            width: 130,
            accessor: 'spec',
            filterable: false,
            sortable: false,
            className: 'padded',
            Cell: ({ value }) => {
              if (!value) return null;
              const taskCount = deepGet(value, 'spec.ioConfig.taskCount');
              const replicas = deepGet(value, 'spec.ioConfig.replicas');
              if (typeof taskCount !== 'number' || typeof replicas !== 'number') return null;
              return (
                <div>
                  <div>{formatInteger(taskCount * replicas)}</div>
                  <div className="detail-line">
                    {replicas === 1
                      ? '(no replication)'
                      : `(${pluralIfNeeded(taskCount, 'task')} × ${pluralIfNeeded(
                          replicas,
                          'replica',
                        )})`}
                  </div>
                </div>
              );
            },
            show: visibleColumns.shown('Configured tasks'),
          },
          {
            Header: 'Running tasks',
            id: 'running_tasks',
            width: 150,
            accessor: 'status.payload',
            filterable: false,
            sortable: false,
            Cell: ({ value, original }) => {
              if (original.suspended) return;
              let label: string | JSX.Element;
              const { activeTasks, publishingTasks } = value || {};
              if (Array.isArray(activeTasks)) {
                label = pluralIfNeeded(activeTasks.length, 'active task');
                if (nonEmptyArray(publishingTasks)) {
                  label = (
                    <>
                      <div>{label}</div>
                      <div>{pluralIfNeeded(publishingTasks.length, 'publishing task')}</div>
                    </>
                  );
                }
              } else {
                label = 'n/a';
              }
              return (
                <TableClickableCell
                  onClick={() => goToTasks(original.supervisor_id, `index_${original.type}`)}
                  hoverIcon={IconNames.ARROW_TOP_RIGHT}
                  title="Go to tasks"
                >
                  {label}
                </TableClickableCell>
              );
            },
            show: visibleColumns.shown('Running tasks'),
          },
          {
            Header: 'Aggregate lag',
            accessor: 'status.payload.aggregateLag',
            width: 200,
            filterable: false,
            sortable: false,
            className: 'padded',
            show: visibleColumns.shown('Aggregate lag'),
            Cell: ({ value }) => formatInteger(value),
          },
          {
            Header: twoLines(
              'Stats',
              <Popover2
                position={Position.BOTTOM}
                content={
                  <Menu>
                    {ROW_STATS_KEYS.map(k => (
                      <MenuItem
                        key={k}
                        icon={checkedCircleIcon(k === statsKey)}
                        text={getRowStatsKeyTitle(k)}
                        onClick={() => {
                          this.setState({ statsKey: k });
                        }}
                      />
                    ))}
                  </Menu>
                }
              >
                <i className="title-button">
                  {getRowStatsKeyTitle(statsKey)} <Icon icon={IconNames.CARET_DOWN} />
                </i>
              </Popover2>,
            ),
            id: 'stats',
            width: 300,
            filterable: false,
            sortable: false,
            className: 'padded',
            accessor: 'stats',
            Cell: ({ value, original }) => {
              if (!value) return;
              const activeTaskIds: string[] | undefined = deepGet(
                original,
                'status.payload.activeTasks',
              )?.map((t: SupervisorStatusTask) => t.id);
              const c = getTotalSupervisorStats(value, statsKey, activeTaskIds);
              const seconds = getRowStatsKeySeconds(statsKey);
              const totalLabel = `Total over ${statsKey}: `;
              const bytes = c.processedBytes ? ` (${formatByteRate(c.processedBytes)})` : '';
              return (
                <div>
                  <div
                    title={`${totalLabel}${formatInteger(c.processed * seconds)} (${formatBytes(
                      c.processedBytes * seconds,
                    )})`}
                  >{`Processed: ${formatRate(c.processed)}${bytes}`}</div>
                  {Boolean(c.processedWithError) && (
                    <div
                      className="warning-line"
                      title={`${totalLabel}${formatInteger(c.processedWithError * seconds)}`}
                    >
                      Processed with error: {formatRate(c.processedWithError)}
                    </div>
                  )}
                  {Boolean(c.thrownAway) && (
                    <div
                      className="warning-line"
                      title={`${totalLabel}${formatInteger(c.thrownAway * seconds)}`}
                    >
                      Thrown away: {formatRate(c.thrownAway)}
                    </div>
                  )}
                  {Boolean(c.unparseable) && (
                    <div
                      className="warning-line"
                      title={`${totalLabel}${formatInteger(c.unparseable * seconds)}`}
                    >
                      Unparseable: {formatRate(c.unparseable)}
                    </div>
                  )}
                </div>
              );
            },
            show: visibleColumns.shown('Stats'),
          },
          {
            Header: 'Recent errors',
            accessor: 'status.payload.recentErrors',
            width: 150,
            filterable: false,
            sortable: false,
            show: visibleColumns.shown('Recent errors'),
            Cell: ({ value, original }) => {
              return (
                <TableClickableCell
                  onClick={() => this.onSupervisorDetail(original)}
                  hoverIcon={IconNames.SEARCH_TEMPLATE}
                  title="See errors"
                >
                  {pluralIfNeeded(value?.length, 'error')}
                </TableClickableCell>
              );
            },
          },
          {
            Header: ACTION_COLUMN_LABEL,
            id: ACTION_COLUMN_ID,
            accessor: 'supervisor_id',
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
            sortable: false,
            Cell: row => {
              const id = row.value;
              const type = row.original.type;
              const supervisorSuspended = row.original.suspended;
              const supervisorActions = this.getSupervisorActions(id, supervisorSuspended, type);
              return (
                <ActionCell
                  onDetail={() => this.onSupervisorDetail(row.original)}
                  actions={supervisorActions}
                />
              );
            },
          },
        ]}
      />
    );
  }

  renderBulkSupervisorActions() {
    const { capabilities, goToQuery } = this.props;
    const lastSupervisorQuery = this.supervisorQueryManager.getLastIntermediateQuery();

    return (
      <>
        <MoreButton>
          {capabilities.hasSql() && (
            <MenuItem
              icon={IconNames.APPLICATION}
              text="View SQL query for table"
              onClick={() => goToQuery({ queryString: lastSupervisorQuery })}
            />
          )}
          <MenuItem
            icon={IconNames.MANUALLY_ENTERED_DATA}
            text="Submit JSON supervisor"
            onClick={() => this.setState({ supervisorSpecDialogOpen: true })}
          />
          <MenuItem
            icon={IconNames.PLAY}
            text="Resume all supervisors"
            onClick={() => this.setState({ showResumeAllSupervisors: true })}
          />
          <MenuItem
            icon={IconNames.PAUSE}
            text="Suspend all supervisors"
            onClick={() => this.setState({ showSuspendAllSupervisors: true })}
          />
          <MenuItem
            icon={IconNames.CROSS}
            text="Terminate all supervisors"
            intent={Intent.DANGER}
            onClick={() => this.setState({ showTerminateAllSupervisors: true })}
          />
        </MoreButton>
        {this.renderResumeAllSupervisorAction()}
        {this.renderSuspendAllSupervisorAction()}
        {this.renderTerminateAllSupervisorAction()}
      </>
    );
  }

  renderResumeAllSupervisorAction() {
    const { showResumeAllSupervisors } = this.state;
    if (!showResumeAllSupervisors) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(`/druid/indexer/v1/supervisor/resumeAll`, {});
          return resp.data;
        }}
        confirmButtonText="Resume all supervisors"
        successText="All supervisors have been resumed"
        failText="Could not resume all supervisors"
        intent={Intent.PRIMARY}
        onClose={() => {
          this.setState({ showResumeAllSupervisors: false });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      >
        <p>Are you sure you want to resume all the supervisors?</p>
      </AsyncActionDialog>
    );
  }

  renderSuspendAllSupervisorAction() {
    const { showSuspendAllSupervisors } = this.state;
    if (!showSuspendAllSupervisors) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(`/druid/indexer/v1/supervisor/suspendAll`, {});
          return resp.data;
        }}
        confirmButtonText="Suspend all supervisors"
        successText="All supervisors have been suspended"
        failText="Could not suspend all supervisors"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ showSuspendAllSupervisors: false });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      >
        <p>Are you sure you want to suspend all the supervisors?</p>
      </AsyncActionDialog>
    );
  }

  renderTerminateAllSupervisorAction() {
    const { showTerminateAllSupervisors } = this.state;
    if (!showTerminateAllSupervisors) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(`/druid/indexer/v1/supervisor/terminateAll`, {});
          return resp.data;
        }}
        confirmButtonText="Terminate all supervisors"
        successText="All supervisors have been terminated"
        failText="Could not terminate all supervisors"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ showTerminateAllSupervisors: false });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      >
        <p>Are you sure you want to terminate all the supervisors?</p>
      </AsyncActionDialog>
    );
  }

  render() {
    const {
      supervisorSpecDialogOpen,
      alertErrorMsg,
      supervisorTableActionDialogId,
      supervisorTableActionDialogActions,
      visibleColumns,
    } = this.state;

    return (
      <div className="supervisors-view app-view">
        <ViewControlBar label="Supervisors">
          <RefreshButton
            localStorageKey={LocalStorageKeys.SUPERVISORS_REFRESH_RATE}
            onRefresh={auto => {
              if (auto && hasPopoverOpen()) return;
              this.supervisorQueryManager.rerunLastQuery(auto);
            }}
          />
          {this.renderBulkSupervisorActions()}
          <TableColumnSelector
            columns={SUPERVISOR_TABLE_COLUMNS}
            onChange={column =>
              this.setState(prevState => ({
                visibleColumns: prevState.visibleColumns.toggle(column),
              }))
            }
            tableColumnsHidden={visibleColumns.getHiddenColumns()}
          />
        </ViewControlBar>
        {this.renderSupervisorTable()}
        {this.renderResumeSupervisorAction()}
        {this.renderSuspendSupervisorAction()}
        {this.renderResetOffsetsSupervisorAction()}
        {this.renderResetSupervisorAction()}
        {this.renderTerminateSupervisorAction()}
        {supervisorSpecDialogOpen && (
          <SpecDialog
            onClose={this.closeSpecDialogs}
            onSubmit={this.submitSupervisor}
            title="Submit supervisor"
          />
        )}
        <AlertDialog
          icon={IconNames.ERROR}
          intent={Intent.PRIMARY}
          isOpen={Boolean(alertErrorMsg)}
          confirmButtonText="OK"
          onConfirm={() => this.setState({ alertErrorMsg: undefined })}
        >
          <p>{alertErrorMsg}</p>
        </AlertDialog>
        {supervisorTableActionDialogId && (
          <SupervisorTableActionDialog
            supervisorId={supervisorTableActionDialogId}
            actions={supervisorTableActionDialogActions}
            onClose={() => this.setState({ supervisorTableActionDialogId: undefined })}
          />
        )}
      </div>
    );
  }
}
