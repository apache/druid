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

import { Icon, Intent, Menu, MenuItem, Popover, Position, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import * as JSONBig from 'json-bigint-native';
import memoize from 'memoize-one';
import type { JSX } from 'react';
import React, { createContext, useContext } from 'react';
import type { Column, Filter, SortingRule } from 'react-table';
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
  SupervisorResetOffsetsDialog,
  SupervisorTableActionDialog,
  TaskGroupHandoffDialog,
} from '../../dialogs';
import type {
  IngestionSpec,
  QueryWithContext,
  RowStatsKey,
  SupervisorStats,
  SupervisorStatus,
  SupervisorStatusTask,
} from '../../druid-models';
import { getTotalSupervisorStats } from '../../druid-models';
import type { Capabilities } from '../../helpers';
import {
  SMALL_TABLE_PAGE_SIZE,
  SMALL_TABLE_PAGE_SIZE_OPTIONS,
  sqlQueryCustomTableFilters,
  suggestibleFilterInput,
} from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import type { AuxiliaryQueryFn, TableState } from '../../utils';
import {
  assemble,
  checkedCircleIcon,
  deepGet,
  filterMap,
  formatByteRate,
  formatBytes,
  formatInteger,
  formatRate,
  getApiArray,
  getDruidErrorMessage,
  hasOverlayOpen,
  isNumberLike,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  nonEmptyArray,
  oneOf,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
  QueryState,
  ResultWithAuxiliaryWork,
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
  { text: 'Stats', label: 'stats API' },
  { text: 'Recent errors', label: 'status API' },
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
  readonly supervisor_id: string;
  readonly type: string;
  readonly source: string;
  readonly detailed_state: string;
  readonly spec?: IngestionSpec;
  readonly suspended: boolean;
}

interface SupervisorsWithAuxiliaryInfo {
  readonly supervisors: SupervisorQueryResultRow[];
  readonly count: number;
  readonly status: Record<string, SupervisorStatus>;
  readonly stats: Record<string, SupervisorStats>;
}

export const StatusContext = createContext<Record<string, SupervisorStatus>>({});
export const StatsContext = createContext<{
  stats: Record<string, SupervisorStats>;
  statsKey: RowStatsKey;
}>({ stats: {}, statsKey: '5m' });

interface HeaderStatsKeySelectorProps {
  changeStatsKey(statsKey: RowStatsKey): void;
}

function HeaderStatsKeySelector({ changeStatsKey }: HeaderStatsKeySelectorProps) {
  const { statsKey } = useContext(StatsContext);
  return (
    <Popover
      position={Position.BOTTOM}
      content={
        <Menu>
          {ROW_STATS_KEYS.map(k => (
            <MenuItem
              key={k}
              icon={checkedCircleIcon(k === statsKey)}
              text={getRowStatsKeyTitle(k)}
              onClick={() => changeStatsKey(k)}
            />
          ))}
        </Menu>
      }
    >
      <i className="title-button">
        {getRowStatsKeyTitle(statsKey)} <Icon icon={IconNames.CARET_DOWN} />
      </i>
    </Popover>
  );
}

export interface SupervisorsViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
  openSupervisorDialog: boolean | undefined;
  goToDatasource(datasource: string): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  goToStreamingDataLoader(supervisorId: string): void;
  goToTasks(supervisorId: string, type: string | undefined): void;
  capabilities: Capabilities;
}

export interface SupervisorsViewState {
  supervisorsState: QueryState<SupervisorsWithAuxiliaryInfo>;
  statsKey: RowStatsKey;

  resumeSupervisorId?: string;
  suspendSupervisorId?: string;
  handoffSupervisorId?: string;
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
  page: number;
  pageSize: number;
  sorted: SortingRule[];
}

function detailedStateToColor(detailedState: string): string {
  switch (detailedState) {
    case 'UNHEALTHY_SUPERVISOR':
    case 'UNHEALTHY_TASKS':
    case 'UNABLE_TO_CONNECT_TO_STREAM':
    case 'LOST_CONTACT_WITH_STREAM':
    case 'INVALID_SPEC':
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

    case 'SUSPENDED':
    case 'SCHEDULER_STOPPED':
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
    SupervisorsWithAuxiliaryInfo
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
      page: 0,
      pageSize: SMALL_TABLE_PAGE_SIZE,
      sorted: [],
    };

    this.supervisorQueryManager = new QueryManager({
      processQuery: async (
        { capabilities, visibleColumns, filtered, sorted, page, pageSize },
        cancelToken,
        setIntermediateQuery,
      ) => {
        let supervisors: SupervisorQueryResultRow[];
        let count = -1;
        const auxiliaryQueries: AuxiliaryQueryFn<SupervisorsWithAuxiliaryInfo>[] = [];
        if (capabilities.hasSql()) {
          const whereExpression = sqlQueryCustomTableFilters(filtered);

          let filterClause = '';
          if (whereExpression.toString() !== 'TRUE') {
            filterClause = whereExpression.toString();
          }

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
            filterClause ? `WHERE ${filterClause}` : undefined,
            sortedToOrderByClause(sorted),
            `LIMIT ${pageSize}`,
            page ? `OFFSET ${page * pageSize}` : undefined,
          ).join('\n');
          setIntermediateQuery(sqlQuery);
          supervisors = (
            await queryDruidSql<SupervisorQueryResultRow>(
              {
                query: sqlQuery,
              },
              cancelToken,
            )
          ).map(supervisor => {
            const spec: any = supervisor.spec;
            if (typeof spec !== 'string') return supervisor;
            return { ...supervisor, spec: JSONBig.parse(spec) };
          });

          auxiliaryQueries.push(async (supervisorsWithAuxiliaryInfo, cancelToken) => {
            const sqlQuery = assemble(
              'SELECT COUNT(*) AS "cnt"',
              'FROM "sys"."supervisors"',
              filterClause ? `WHERE ${filterClause}` : undefined,
            ).join('\n');
            const cnt: any = (
              await queryDruidSql<{ cnt: number }>(
                {
                  query: sqlQuery,
                },
                cancelToken,
              )
            )[0].cnt;
            return {
              ...supervisorsWithAuxiliaryInfo,
              count: typeof cnt === 'number' ? cnt : -1,
            };
          });
        } else if (capabilities.hasOverlordAccess()) {
          supervisors = (await getApiArray('/druid/indexer/v1/supervisor?full', cancelToken)).map(
            (sup: any) => {
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
            },
          );
          count = supervisors.length;

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
          if (visibleColumns.shown('Running tasks', 'Aggregate lag', 'Recent errors')) {
            auxiliaryQueries.push(
              ...supervisors.map(
                (supervisor): AuxiliaryQueryFn<SupervisorsWithAuxiliaryInfo> =>
                  async (supervisorsWithAuxiliaryInfo, cancelToken) => {
                    const status = (
                      await Api.instance.get(
                        `/druid/indexer/v1/supervisor/${Api.encodePath(
                          supervisor.supervisor_id,
                        )}/status`,
                        { cancelToken, timeout: STATUS_API_TIMEOUT },
                      )
                    ).data;
                    return {
                      ...supervisorsWithAuxiliaryInfo,
                      status: {
                        ...supervisorsWithAuxiliaryInfo.status,
                        [supervisor.supervisor_id]: status,
                      },
                    };
                  },
              ),
            );
          }

          if (visibleColumns.shown('Stats')) {
            auxiliaryQueries.push(
              ...filterMap(
                supervisors,
                (supervisor): AuxiliaryQueryFn<SupervisorsWithAuxiliaryInfo> | undefined => {
                  if (oneOf(supervisor.type, 'autocompact', 'scheduled_batch')) return; // These supervisors do not report stats
                  return async (supervisorsWithAuxiliaryInfo, cancelToken) => {
                    const stats = (
                      await Api.instance.get(
                        `/druid/indexer/v1/supervisor/${Api.encodePath(
                          supervisor.supervisor_id,
                        )}/stats`,
                        { cancelToken, timeout: STATS_API_TIMEOUT },
                      )
                    ).data;
                    return {
                      ...supervisorsWithAuxiliaryInfo,
                      stats: {
                        ...supervisorsWithAuxiliaryInfo.stats,
                        [supervisor.supervisor_id]: stats,
                      },
                    };
                  };
                },
              ),
            );
          }
        }

        return new ResultWithAuxiliaryWork<SupervisorsWithAuxiliaryInfo>(
          { supervisors, count, status: {}, stats: {} },
          auxiliaryQueries,
        );
      },
      onStateChange: supervisorsState => {
        this.setState({
          supervisorsState,
        });
      },
    });
  }

  componentDidMount() {
    this.fetchData();
  }

  componentWillUnmount(): void {
    this.supervisorQueryManager.terminate();
  }

  componentDidUpdate(
    prevProps: Readonly<SupervisorsViewProps>,
    prevState: Readonly<SupervisorsViewState>,
  ) {
    const { filters } = this.props;
    const { page, pageSize, sorted } = this.state;
    if (
      !sqlQueryCustomTableFilters(filters).equals(sqlQueryCustomTableFilters(prevProps.filters)) ||
      page !== prevState.page ||
      pageSize !== prevState.pageSize ||
      sortedToOrderByClause(sorted) !== sortedToOrderByClause(prevState.sorted)
    ) {
      this.fetchData();
    }
  }

  private readonly fetchData = () => {
    const { capabilities, filters } = this.props;
    const { visibleColumns, page, pageSize, sorted } = this.state;
    this.supervisorQueryManager.runQuery({
      page,
      pageSize,
      filtered: filters,
      sorted,
      visibleColumns,
      capabilities,
    });
  };

  private readonly handleFilterChange = (filters: Filter[]) => {
    this.goToFirstPage();
    this.props.onFiltersChange(filters);
  };

  private goToFirstPage() {
    if (this.state.page) {
      this.setState({ page: 0 });
    }
  }

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

  private getSupervisorActions(supervisor: SupervisorQueryResultRow): BasicAction[] {
    const { supervisor_id, suspended, type } = supervisor;
    const { goToDatasource, goToStreamingDataLoader } = this.props;

    const actions: BasicAction[] = [];
    if (oneOf(type, 'kafka', 'kinesis')) {
      actions.push(
        {
          icon: IconNames.MULTI_SELECT,
          title: 'Go to datasource',
          onAction: () => goToDatasource(supervisor_id),
        },
        {
          icon: IconNames.CLOUD_UPLOAD,
          title: 'Open in data loader',
          onAction: () => goToStreamingDataLoader(supervisor_id),
        },
      );
    }

    actions.push(
      {
        icon: suspended ? IconNames.PLAY : IconNames.PAUSE,
        title: suspended ? 'Resume' : 'Suspend',
        onAction: () =>
          suspended
            ? this.setState({ resumeSupervisorId: supervisor_id })
            : this.setState({ suspendSupervisorId: supervisor_id }),
      },
      {
        icon: IconNames.GANTT_CHART,
        title: 'Go to tasks',
        onAction: () => this.goToTasksForSupervisor(supervisor),
      },
    );

    if (!suspended) {
      actions.push({
        icon: IconNames.AUTOMATIC_UPDATES,
        title: 'Handoff early',
        onAction: () => this.setState({ handoffSupervisorId: supervisor_id }),
      });
    }

    actions.push(
      {
        icon: IconNames.STEP_BACKWARD,
        title: `Set ${type === 'kinesis' ? 'sequence numbers' : 'offsets'}`,
        onAction: () => this.setState({ resetOffsetsSupervisorInfo: { id: supervisor_id, type } }),
      },
      {
        icon: IconNames.STEP_BACKWARD,
        title: 'Hard reset',
        intent: Intent.DANGER,
        onAction: () => this.setState({ resetSupervisorId: supervisor_id }),
      },
      {
        icon: IconNames.CROSS,
        title: 'Terminate',
        intent: Intent.DANGER,
        onAction: () => this.setState({ terminateSupervisorId: supervisor_id }),
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

  renderTaskGroupHandoffAction() {
    const { handoffSupervisorId } = this.state;
    if (!handoffSupervisorId) return;

    return (
      <TaskGroupHandoffDialog
        supervisorId={handoffSupervisorId}
        onClose={() => {
          this.setState({ handoffSupervisorId: undefined });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      />
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
    const { filters } = this.props;
    const { handleFilterChange } = this;

    return function SupervisorFilterableCell(row: { value: any }) {
      return (
        <TableFilterableCell
          field={field}
          value={row.value}
          filters={filters}
          onFiltersChange={handleFilterChange}
        />
      );
    };
  }

  private onSupervisorDetail(supervisor: SupervisorQueryResultRow) {
    this.setState({
      supervisorTableActionDialogId: supervisor.supervisor_id,
      supervisorTableActionDialogActions: this.getSupervisorActions(supervisor),
    });
  }

  private goToTasksForSupervisor(supervisor: SupervisorQueryResultRow) {
    const { goToTasks } = this.props;
    switch (supervisor.type) {
      case 'kafka':
      case 'kinesis':
        goToTasks(supervisor.supervisor_id, `index_${supervisor.type}`);
        return;

      case 'autocompact':
        goToTasks(supervisor.supervisor_id.replace(/^autocompact__/, ''), 'compact');
        return;

      case 'scheduled_batch':
        goToTasks(
          supervisor.supervisor_id.replace(/^scheduled_batch__/, '').replace(/__[0-9a-f-]+$/, ''),
          'query_controller',
        );
        return;

      default:
        goToTasks(supervisor.supervisor_id, undefined);
        return;
    }
  }

  private renderSupervisorTable() {
    const { filters } = this.props;
    const { supervisorsState, page, pageSize, sorted, statsKey, visibleColumns } = this.state;

    const { supervisors, count, status, stats } = supervisorsState.data || {
      supervisors: [],
      count: -1,
      status: {},
      stats: {},
    };
    return (
      <StatusContext.Provider value={status}>
        <StatsContext.Provider value={{ stats, statsKey }}>
          <ReactTable
            data={supervisors}
            pages={count >= 0 ? Math.ceil(count / pageSize) : 10000000} // We are hiding the page selector
            loading={supervisorsState.loading}
            noDataText={
              supervisorsState.isEmpty()
                ? 'No supervisors'
                : supervisorsState.getErrorMessage() || ''
            }
            manual
            filterable
            filtered={filters}
            onFilteredChange={this.handleFilterChange}
            sorted={sorted}
            onSortedChange={sorted => this.setState({ sorted })}
            page={page}
            onPageChange={page => this.setState({ page })}
            pageSize={pageSize}
            onPageSizeChange={pageSize => this.setState({ pageSize })}
            pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
            showPagination
            showPageJump={false}
            ofText={count >= 0 ? `of ${formatInteger(count)}` : ''}
            columns={this.getTableColumns(visibleColumns, filters)}
          />
        </StatsContext.Provider>
      </StatusContext.Provider>
    );
  }

  private readonly getTableColumns = memoize(
    (
      visibleColumns: LocalStorageBackedVisibility,
      filters: Filter[],
    ): Column<SupervisorQueryResultRow>[] => {
      return [
        {
          Header: twoLines('Supervisor ID', <i>(datasource)</i>),
          id: 'supervisor_id',
          accessor: 'supervisor_id',
          width: 280,
          show: visibleColumns.shown('Supervisor ID'),
          Cell: ({ value, original }) => (
            <TableClickableCell
              tooltip="Show detail"
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
          width: 120,
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
          width: 170,
          Filter: suggestibleFilterInput([
            'CONNECTING_TO_STREAM',
            'CREATING_TASKS',
            'DISCOVERING_INITIAL_TASKS',
            'IDLE',
            'INVALID_SPEC',
            'LOST_CONTACT_WITH_STREAM',
            'PENDING',
            'RUNNING',
            'SCHEDULER_STOPPED',
            'STOPPING',
            'SUSPENDED',
            'UNABLE_TO_CONNECT_TO_STREAM',
            'UNHEALTHY_SUPERVISOR',
            'UNHEALTHY_TASKS',
          ]),
          accessor: 'detailed_state',
          Cell: ({ value }) => (
            <TableFilterableCell
              field="detailed_state"
              value={value}
              filters={filters}
              onFiltersChange={this.handleFilterChange}
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
          accessor: 'supervisor_id',
          width: 150,
          filterable: false,
          sortable: false,
          Cell: ({ value, original }) => {
            const status = useContext(StatusContext);
            if (original.suspended) return;
            const { activeTasks, publishingTasks } = status[value]?.payload || {};
            let label: string | JSX.Element;
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
              label = '';
            }

            return (
              <TableClickableCell
                tooltip="Go to tasks"
                onClick={() => this.goToTasksForSupervisor(original)}
                hoverIcon={IconNames.ARROW_TOP_RIGHT}
              >
                {label}
              </TableClickableCell>
            );
          },
          show: visibleColumns.shown('Running tasks'),
        },
        {
          Header: 'Aggregate lag',
          accessor: 'supervisor_id',
          width: 200,
          filterable: false,
          sortable: false,
          className: 'padded',
          show: visibleColumns.shown('Aggregate lag'),
          Cell: ({ value }) => {
            const status = useContext(StatusContext);
            const aggregateLag = status[value]?.payload?.aggregateLag;
            return isNumberLike(aggregateLag) ? formatInteger(aggregateLag) : null;
          },
        },
        {
          Header: twoLines(
            'Stats',
            <HeaderStatsKeySelector changeStatsKey={statsKey => this.setState({ statsKey })} />,
          ),
          id: 'stats',
          accessor: 'supervisor_id',
          width: 300,
          filterable: false,
          sortable: false,
          className: 'padded',
          show: visibleColumns.shown('Stats'),
          Cell: ({ value, original }) => {
            const { stats, statsKey } = useContext(StatsContext);
            const s = stats[value];
            if (!s) return;
            const activeTasks: SupervisorStatusTask[] | undefined = deepGet(
              original,
              'status.payload.activeTasks',
            );
            const activeTaskIds: string[] | undefined = Array.isArray(activeTasks)
              ? activeTasks.map((t: SupervisorStatusTask) => t.id)
              : undefined;

            const c = getTotalSupervisorStats(s, statsKey, activeTaskIds);
            const seconds = getRowStatsKeySeconds(statsKey);
            const issues = (c.processedWithError || 0) + (c.thrownAway || 0) + (c.unparseable || 0);
            const totalLabel = `Total (past ${statsKey}): `;
            return issues ? (
              <div>
                <div
                  data-tooltip={`${totalLabel}${formatBytes(c.processedBytes * seconds)}`}
                >{`Input: ${formatByteRate(c.processedBytes)}`}</div>
                {Boolean(c.processed) && (
                  <div
                    data-tooltip={`${totalLabel}${formatInteger(c.processed * seconds)} events`}
                  >{`Processed: ${formatRate(c.processed)}`}</div>
                )}
                {Boolean(c.processedWithError) && (
                  <div
                    className="warning-line"
                    data-tooltip={`${totalLabel}${formatInteger(
                      c.processedWithError * seconds,
                    )} events`}
                  >
                    Processed with error: {formatRate(c.processedWithError)}
                  </div>
                )}
                {Boolean(c.thrownAway) && (
                  <div
                    className="warning-line"
                    data-tooltip={`${totalLabel}${formatInteger(c.thrownAway * seconds)} events`}
                  >
                    Thrown away: {formatRate(c.thrownAway)}
                  </div>
                )}
                {Boolean(c.unparseable) && (
                  <div
                    className="warning-line"
                    data-tooltip={`${totalLabel}${formatInteger(c.unparseable * seconds)} events`}
                  >
                    Unparseable: {formatRate(c.unparseable)}
                  </div>
                )}
              </div>
            ) : c.processedBytes ? (
              <div
                data-tooltip={`${totalLabel}${formatInteger(
                  c.processed * seconds,
                )} events, ${formatBytes(c.processedBytes * seconds)}`}
              >{`Processed: ${formatRate(c.processed)} (${formatByteRate(c.processedBytes)})`}</div>
            ) : (
              <div>No activity</div>
            );
          },
        },
        {
          Header: 'Recent errors',
          accessor: 'supervisor_id',
          width: 150,
          filterable: false,
          sortable: false,
          show: visibleColumns.shown('Recent errors'),
          Cell: ({ value, original }) => {
            const status = useContext(StatusContext);
            const recentErrors = status[value]?.payload?.recentErrors;
            if (!recentErrors) return null;
            return (
              <TableClickableCell
                tooltip="Show errors"
                onClick={() => this.onSupervisorDetail(original)}
                hoverIcon={IconNames.SEARCH_TEMPLATE}
              >
                {pluralIfNeeded(recentErrors.length, 'error')}
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
          Cell: ({ value: id, original }) => {
            const supervisorActions = this.getSupervisorActions(original);
            return (
              <ActionCell
                onDetail={() => this.onSupervisorDetail(original)}
                actions={supervisorActions}
                menuTitle={id}
              />
            );
          },
        },
      ];
    },
  );

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
              disabled={typeof lastSupervisorQuery !== 'string'}
              onClick={() => {
                if (typeof lastSupervisorQuery !== 'string') return;
                goToQuery({ queryString: lastSupervisorQuery });
              }}
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
              if (auto && hasOverlayOpen()) return;
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
            onClose={this.fetchData}
            tableColumnsHidden={visibleColumns.getHiddenColumns()}
          />
        </ViewControlBar>
        {this.renderSupervisorTable()}
        {this.renderResumeSupervisorAction()}
        {this.renderSuspendSupervisorAction()}
        {this.renderTaskGroupHandoffAction()}
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
