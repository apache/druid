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

import { Alert, Button, ButtonGroup, Intent, Label, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';
import SplitterLayout from 'react-splitter-layout';
import ReactTable, { Filter } from 'react-table';

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
  AsyncActionDialog,
  SpecDialog,
  SupervisorTableActionDialog,
  TaskTableActionDialog,
} from '../../dialogs';
import { QueryWithContext } from '../../druid-models';
import {
  SMALL_TABLE_PAGE_SIZE,
  SMALL_TABLE_PAGE_SIZE_OPTIONS,
  syncFilterClauseById,
} from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import {
  Capabilities,
  deepGet,
  formatDuration,
  getDruidErrorMessage,
  hasPopoverOpen,
  LocalStorageBackedVisibility,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
  oneOf,
  queryDruidSql,
  QueryManager,
  QueryState,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';

import './ingestion-view.scss';

const supervisorTableColumns: string[] = [
  'Datasource',
  'Type',
  'Topic/Stream',
  'Status',
  ACTION_COLUMN_LABEL,
];

const taskTableColumns: string[] = [
  'Task ID',
  'Group ID',
  'Type',
  'Datasource',
  'Status',
  'Created time',
  'Duration',
  'Location',
  ACTION_COLUMN_LABEL,
];

const CANCELED_ERROR_MSG = 'Shutdown request from user';

interface SupervisorQueryResultRow {
  supervisor_id: string;
  type: string;
  source: string;
  state: string;
  detailed_state: string;
  suspended: boolean;
}

interface TaskQueryResultRow {
  task_id: string;
  group_id: string;
  type: string;
  created_time: string;
  datasource: string;
  duration: number;
  error_msg: string | null;
  location: string | null;
  status: string;
}

export interface IngestionViewProps {
  taskId: string | undefined;
  taskGroupId: string | undefined;
  datasourceId: string | undefined;
  openDialog: string | undefined;
  goToDatasource(datasource: string): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  goToStreamingDataLoader(supervisorId?: string): void;
  goToClassicBatchDataLoader(taskId?: string): void;
  capabilities: Capabilities;
}

export interface IngestionViewState {
  supervisorsState: QueryState<SupervisorQueryResultRow[]>;

  resumeSupervisorId?: string;
  suspendSupervisorId?: string;
  resetSupervisorId?: string;
  terminateSupervisorId?: string;

  showResumeAllSupervisors: boolean;
  showSuspendAllSupervisors: boolean;
  showTerminateAllSupervisors: boolean;

  tasksState: QueryState<TaskQueryResultRow[]>;

  taskFilter: Filter[];
  supervisorFilter: Filter[];

  groupTasksBy?: 'group_id' | 'type' | 'datasource' | 'status';

  killTaskId?: string;

  supervisorSpecDialogOpen: boolean;
  taskSpecDialogOpen: boolean;
  alertErrorMsg?: string;

  taskTableActionDialogId?: string;
  taskTableActionDialogStatus?: string;
  taskTableActionDialogActions: BasicAction[];
  supervisorTableActionDialogId?: string;
  supervisorTableActionDialogActions: BasicAction[];
  hiddenTaskColumns: LocalStorageBackedVisibility;
  hiddenSupervisorColumns: LocalStorageBackedVisibility;
}

function statusToColor(status: string): string {
  switch (status) {
    case 'RUNNING':
      return '#2167d5';
    case 'WAITING':
      return '#d5631a';
    case 'PENDING':
      return '#ffbf00';
    case 'SUCCESS':
      return '#57d500';
    case 'FAILED':
      return '#d5100a';
    case 'CANCELED':
      return '#858585';
    default:
      return '#0a1500';
  }
}

function stateToColor(status: string): string {
  switch (status) {
    case 'UNHEALTHY_SUPERVISOR':
      return '#d5100a';
    case 'UNHEALTHY_TASKS':
      return '#d5100a';
    case 'PENDING':
      return '#ffbf00';
    case `SUSPENDED`:
      return '#ffbf00';
    case 'STOPPING':
      return '#d5100a';
    case 'RUNNING':
      return '#2167d5';
    default:
      return '#0a1500';
  }
}

export class IngestionView extends React.PureComponent<IngestionViewProps, IngestionViewState> {
  private readonly supervisorQueryManager: QueryManager<Capabilities, SupervisorQueryResultRow[]>;
  private readonly taskQueryManager: QueryManager<Capabilities, TaskQueryResultRow[]>;
  static statusRanking: Record<string, number> = {
    RUNNING: 4,
    PENDING: 3,
    WAITING: 2,
    SUCCESS: 1,
    FAILED: 1,
  };

  static SUPERVISOR_SQL = `SELECT
  "supervisor_id", "type", "source", "state", "detailed_state", "suspended" = 1 AS "suspended"
FROM sys.supervisors
ORDER BY "supervisor_id"`;

  static TASK_SQL = `WITH tasks AS (SELECT
  "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg",
  CASE WHEN "error_msg" = '${CANCELED_ERROR_MSG}' THEN 'CANCELED' WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status"
  FROM sys.tasks
)
SELECT "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg", "status"
FROM tasks
ORDER BY
  (
    CASE "status"
    WHEN 'RUNNING' THEN 4
    WHEN 'PENDING' THEN 3
    WHEN 'WAITING' THEN 2
    ELSE 1
    END
  ) DESC,
  "created_time" DESC`;

  constructor(props: IngestionViewProps, context: any) {
    super(props, context);

    const taskFilter: Filter[] = [];
    if (props.taskId) taskFilter.push({ id: 'task_id', value: `=${props.taskId}` });
    if (props.taskGroupId) taskFilter.push({ id: 'group_id', value: `=${props.taskGroupId}` });
    if (props.datasourceId) taskFilter.push({ id: 'datasource', value: `=${props.datasourceId}` });

    const supervisorFilter: Filter[] = [];
    if (props.datasourceId)
      supervisorFilter.push({ id: 'datasource', value: `=${props.datasourceId}` });

    this.state = {
      supervisorsState: QueryState.INIT,

      showResumeAllSupervisors: false,
      showSuspendAllSupervisors: false,
      showTerminateAllSupervisors: false,

      tasksState: QueryState.INIT,
      taskFilter: taskFilter,
      supervisorFilter: supervisorFilter,

      supervisorSpecDialogOpen: props.openDialog === 'supervisor',
      taskSpecDialogOpen: props.openDialog === 'task',

      taskTableActionDialogActions: [],
      supervisorTableActionDialogActions: [],

      hiddenTaskColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.TASK_TABLE_COLUMN_SELECTION,
      ),
      hiddenSupervisorColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.SUPERVISOR_TABLE_COLUMN_SELECTION,
      ),
    };

    this.supervisorQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasSql()) {
          return await queryDruidSql({
            query: IngestionView.SUPERVISOR_SQL,
          });
        } else if (capabilities.hasOverlordAccess()) {
          const supervisors = (await Api.instance.get('/druid/indexer/v1/supervisor?full')).data;
          if (!Array.isArray(supervisors)) throw new Error(`Unexpected results`);
          return supervisors.map((sup: any) => {
            return {
              supervisor_id: deepGet(sup, 'id'),
              type: deepGet(sup, 'spec.tuningConfig.type'),
              source:
                deepGet(sup, 'spec.ioConfig.topic') ||
                deepGet(sup, 'spec.ioConfig.stream') ||
                'n/a',
              state: deepGet(sup, 'state'),
              detailed_state: deepGet(sup, 'detailedState'),
              suspended: Boolean(deepGet(sup, 'suspended')),
            };
          });
        } else {
          throw new Error(`must have SQL or overlord access`);
        }
      },
      onStateChange: supervisorsState => {
        this.setState({
          supervisorsState,
        });
      },
    });

    this.taskQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasSql()) {
          return await queryDruidSql({
            query: IngestionView.TASK_SQL,
          });
        } else if (capabilities.hasOverlordAccess()) {
          const resp = await Api.instance.get(`/druid/indexer/v1/tasks`);
          return IngestionView.parseTasks(resp.data);
        } else {
          throw new Error(`must have SQL or overlord access`);
        }
      },
      onStateChange: tasksState => {
        this.setState({
          tasksState,
        });
      },
    });
  }

  static parseTasks = (data: any[]): TaskQueryResultRow[] => {
    return data.map(d => {
      return {
        task_id: d.id,
        group_id: d.groupId,
        type: d.type,
        created_time: d.createdTime,
        datasource: d.dataSource,
        duration: d.duration ? d.duration : 0,
        error_msg: d.errorMsg,
        location: d.location.host ? `${d.location.host}:${d.location.port}` : null,
        status: d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode,
      };
    });
  };

  private static onSecondaryPaneSizeChange(secondaryPaneSize: number) {
    localStorageSet(LocalStorageKeys.INGESTION_VIEW_PANE_SIZE, String(secondaryPaneSize));
  }

  componentDidMount(): void {
    const { capabilities } = this.props;

    this.supervisorQueryManager.runQuery(capabilities);
    this.taskQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.supervisorQueryManager.terminate();
    this.taskQueryManager.terminate();
  }

  private readonly closeSpecDialogs = () => {
    this.setState({
      supervisorSpecDialogOpen: false,
      taskSpecDialogOpen: false,
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

  private readonly submitTask = async (spec: JSON) => {
    try {
      await Api.instance.post('/druid/indexer/v1/task', spec);
    } catch (e) {
      AppToaster.show({
        message: `Failed to submit task: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
      return;
    }

    AppToaster.show({
      message: 'Task submitted successfully',
      intent: Intent.SUCCESS,
    });
    this.taskQueryManager.rerunLastQuery();
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
        title: 'Hard reset',
        intent: Intent.DANGER,
        onAction: () => this.setState({ resetSupervisorId: id }),
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
        <p>{`Are you sure you want to resume supervisor '${resumeSupervisorId}'?`}</p>
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
        <p>{`Are you sure you want to suspend supervisor '${suspendSupervisorId}'?`}</p>
      </AsyncActionDialog>
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
            {},
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
          `I understand that resetting ${resetSupervisorId} will clear checkpoints and therefore lead to data loss or duplication.`,
          'I understand that this operation cannot be undone.',
        ]}
      >
        <p>{`Are you sure you want to hard reset supervisor '${resetSupervisorId}'?`}</p>
        <p>Hard resetting a supervisor will lead to data loss or data duplication.</p>
        <p>
          The reason for using this operation is to recover from a state in which the supervisor
          ceases operating due to missing offsets.
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
        <p>{`Are you sure you want to terminate supervisor '${terminateSupervisorId}'?`}</p>
        <p>This action is not reversible.</p>
      </AsyncActionDialog>
    );
  }

  private renderSupervisorFilterableCell(field: string) {
    const { supervisorFilter } = this.state;

    return (row: { value: any }) => (
      <TableFilterableCell
        field={field}
        value={row.value}
        filters={supervisorFilter}
        onFiltersChange={filters => this.setState({ supervisorFilter: filters })}
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
    const { supervisorsState, hiddenSupervisorColumns, taskFilter, supervisorFilter } = this.state;

    const supervisors = supervisorsState.data || [];
    return (
      <ReactTable
        data={supervisors}
        loading={supervisorsState.loading}
        noDataText={
          supervisorsState.isEmpty() ? 'No supervisors' : supervisorsState.getErrorMessage() || ''
        }
        filtered={supervisorFilter}
        onFilteredChange={(filtered, column) => {
          this.setState({
            supervisorFilter: filtered,
            taskFilter:
              column.id === 'datasource'
                ? syncFilterClauseById(taskFilter, filtered, 'datasource')
                : taskFilter,
          });
        }}
        filterable
        defaultPageSize={SMALL_TABLE_PAGE_SIZE}
        pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={supervisors.length > SMALL_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: 'Datasource',
            id: 'datasource',
            accessor: 'supervisor_id',
            width: 300,
            show: hiddenSupervisorColumns.shown('Datasource'),
            Cell: ({ value, original }) => (
              <TableClickableCell
                onClick={() => this.onSupervisorDetail(original)}
                hoverIcon={IconNames.EDIT}
              >
                {value}
              </TableClickableCell>
            ),
          },
          {
            Header: 'Type',
            accessor: 'type',
            width: 100,
            Cell: this.renderSupervisorFilterableCell('type'),
            show: hiddenSupervisorColumns.shown('Type'),
          },
          {
            Header: 'Topic/Stream',
            accessor: 'source',
            width: 300,
            Cell: this.renderSupervisorFilterableCell('source'),
            show: hiddenSupervisorColumns.shown('Topic/Stream'),
          },
          {
            Header: 'Status',
            id: 'status',
            width: 300,
            accessor: 'detailed_state',
            Cell: row => (
              <TableFilterableCell
                field="status"
                value={row.value}
                filters={supervisorFilter}
                onFiltersChange={filters => this.setState({ supervisorFilter: filters })}
              >
                <span>
                  <span style={{ color: stateToColor(row.original.state) }}>&#x25cf;&nbsp;</span>
                  {row.value}
                </span>
              </TableFilterableCell>
            ),
            show: hiddenSupervisorColumns.shown('Status'),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            id: ACTION_COLUMN_ID,
            accessor: 'supervisor_id',
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
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
            show: hiddenSupervisorColumns.shown(ACTION_COLUMN_LABEL),
          },
        ]}
      />
    );
  }

  private getTaskActions(
    id: string,
    datasource: string,
    status: string,
    type: string,
  ): BasicAction[] {
    const { goToDatasource, goToClassicBatchDataLoader } = this.props;

    const actions: BasicAction[] = [];
    if (datasource && status === 'SUCCESS') {
      actions.push({
        icon: IconNames.MULTI_SELECT,
        title: 'Go to datasource',
        onAction: () => goToDatasource(datasource),
      });
    }
    if (oneOf(type, 'index', 'index_parallel')) {
      actions.push({
        icon: IconNames.CLOUD_UPLOAD,
        title: 'Open in data loader',
        onAction: () => goToClassicBatchDataLoader(id),
      });
    }
    if (oneOf(status, 'RUNNING', 'WAITING', 'PENDING')) {
      actions.push({
        icon: IconNames.CROSS,
        title: 'Kill',
        intent: Intent.DANGER,
        onAction: () => this.setState({ killTaskId: id }),
      });
    }
    return actions;
  }

  renderKillTaskAction() {
    const { killTaskId } = this.state;
    if (!killTaskId) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(
            `/druid/indexer/v1/task/${Api.encodePath(killTaskId)}/shutdown`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Kill task"
        successText="Task was killed"
        failText="Could not kill task"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ killTaskId: undefined });
        }}
        onSuccess={() => {
          this.taskQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to kill task '${killTaskId}'?`}</p>
      </AsyncActionDialog>
    );
  }

  private renderTaskFilterableCell(field: string) {
    const { taskFilter } = this.state;

    return (row: { value: any }) => (
      <TableFilterableCell
        field={field}
        value={row.value}
        filters={taskFilter}
        onFiltersChange={filters => this.setState({ taskFilter: filters })}
      >
        {row.value}
      </TableFilterableCell>
    );
  }

  private onTaskDetail(task: TaskQueryResultRow) {
    this.setState({
      taskTableActionDialogId: task.task_id,
      taskTableActionDialogStatus: task.status,
      taskTableActionDialogActions: this.getTaskActions(
        task.task_id,
        task.datasource,
        task.status,
        task.type,
      ),
    });
  }

  private renderTaskTable() {
    const { tasksState, taskFilter, groupTasksBy, hiddenTaskColumns, supervisorFilter } =
      this.state;

    const tasks = tasksState.data || [];
    return (
      <ReactTable
        data={tasks}
        loading={tasksState.loading}
        noDataText={tasksState.isEmpty() ? 'No tasks' : tasksState.getErrorMessage() || ''}
        filterable
        filtered={taskFilter}
        onFilteredChange={(filtered, column) => {
          this.setState({
            supervisorFilter:
              column.id === 'datasource'
                ? syncFilterClauseById(supervisorFilter, filtered, 'datasource')
                : supervisorFilter,
            taskFilter: filtered,
          });
        }}
        defaultSorted={[{ id: 'status', desc: true }]}
        pivotBy={groupTasksBy ? [groupTasksBy] : []}
        defaultPageSize={SMALL_TABLE_PAGE_SIZE}
        pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={tasks.length > SMALL_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: 'Task ID',
            accessor: 'task_id',
            width: 440,
            Cell: ({ value, original }) => (
              <TableClickableCell
                onClick={() => this.onTaskDetail(original)}
                hoverIcon={IconNames.EDIT}
              >
                {value}
              </TableClickableCell>
            ),
            Aggregated: () => '',
            show: hiddenTaskColumns.shown('Task ID'),
          },
          {
            Header: 'Group ID',
            accessor: 'group_id',
            width: 300,
            Cell: this.renderTaskFilterableCell('group_id'),
            Aggregated: () => '',
            show: hiddenTaskColumns.shown('Group ID'),
          },
          {
            Header: 'Type',
            accessor: 'type',
            width: 140,
            Cell: this.renderTaskFilterableCell('type'),
            show: hiddenTaskColumns.shown('Type'),
          },
          {
            Header: 'Datasource',
            accessor: 'datasource',
            width: 200,
            Cell: this.renderTaskFilterableCell('datasource'),
            show: hiddenTaskColumns.shown('Datasource'),
          },
          {
            Header: 'Status',
            id: 'status',
            width: 110,
            accessor: row => ({
              status: row.status,
              created_time: row.created_time,
              toString: () => row.status,
            }),
            Cell: row => {
              if (row.aggregated) return '';
              const { status } = row.original;
              const errorMsg = row.original.error_msg;
              return (
                <TableFilterableCell
                  field="status"
                  value={status}
                  filters={taskFilter}
                  onFiltersChange={filters => this.setState({ taskFilter: filters })}
                >
                  <span>
                    <span style={{ color: statusToColor(status) }}>&#x25cf;&nbsp;</span>
                    {status}
                    {errorMsg && errorMsg !== CANCELED_ERROR_MSG && (
                      <a
                        onClick={() => this.setState({ alertErrorMsg: errorMsg })}
                        title={errorMsg}
                      >
                        &nbsp;?
                      </a>
                    )}
                  </span>
                </TableFilterableCell>
              );
            },
            sortMethod: (d1, d2) => {
              const typeofD1 = typeof d1;
              const typeofD2 = typeof d2;
              if (typeofD1 !== typeofD2) return 0;
              switch (typeofD1) {
                case 'string':
                  return IngestionView.statusRanking[d1] - IngestionView.statusRanking[d2];

                case 'object':
                  return (
                    IngestionView.statusRanking[d1.status] -
                      IngestionView.statusRanking[d2.status] ||
                    d1.created_time.localeCompare(d2.created_time)
                  );

                default:
                  return 0;
              }
            },
            show: hiddenTaskColumns.shown('Status'),
          },
          {
            Header: 'Created time',
            accessor: 'created_time',
            width: 190,
            Cell: this.renderTaskFilterableCell('created_time'),
            Aggregated: () => '',
            show: hiddenTaskColumns.shown('Created time'),
          },
          {
            Header: 'Duration',
            accessor: 'duration',
            width: 80,
            filterable: false,
            className: 'padded',
            Cell({ value, original }) {
              if (value > 0) {
                return formatDuration(value);
              }
              if (oneOf(original.status, 'RUNNING', 'PENDING') && original.created_time) {
                // Compute running duration from the created time if it exists
                return formatDuration(Date.now() - Date.parse(original.created_time));
              }
              return '';
            },
            Aggregated: () => '',
            show: hiddenTaskColumns.shown('Duration'),
          },
          {
            Header: 'Location',
            accessor: 'location',
            width: 200,
            Cell: this.renderTaskFilterableCell('location'),
            Aggregated: () => '',
            show: hiddenTaskColumns.shown('Location'),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            id: ACTION_COLUMN_ID,
            accessor: 'task_id',
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
            Cell: row => {
              if (row.aggregated) return '';
              const id = row.value;
              const type = row.row.type;
              const { datasource, status } = row.original;
              const taskActions = this.getTaskActions(id, datasource, status, type);
              return (
                <ActionCell
                  onDetail={() => this.onTaskDetail(row.original)}
                  actions={taskActions}
                />
              );
            },
            Aggregated: () => '',
            show: hiddenTaskColumns.shown(ACTION_COLUMN_LABEL),
          },
        ]}
      />
    );
  }

  renderBulkSupervisorActions() {
    const { capabilities, goToQuery } = this.props;

    return (
      <>
        <MoreButton>
          {capabilities.hasSql() && (
            <MenuItem
              icon={IconNames.APPLICATION}
              text="View SQL query for table"
              onClick={() => goToQuery({ queryString: IngestionView.SUPERVISOR_SQL })}
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

  renderBulkTasksActions() {
    const { goToQuery, capabilities } = this.props;

    return (
      <MoreButton>
        {capabilities.hasSql() && (
          <MenuItem
            icon={IconNames.APPLICATION}
            text="View SQL query for table"
            onClick={() => goToQuery({ queryString: IngestionView.TASK_SQL })}
          />
        )}
        <MenuItem
          icon={IconNames.MANUALLY_ENTERED_DATA}
          text="Submit JSON task"
          onClick={() => this.setState({ taskSpecDialogOpen: true })}
        />
      </MoreButton>
    );
  }

  render(): JSX.Element {
    const {
      groupTasksBy,
      supervisorSpecDialogOpen,
      taskSpecDialogOpen,
      alertErrorMsg,
      taskTableActionDialogId,
      taskTableActionDialogActions,
      supervisorTableActionDialogId,
      supervisorTableActionDialogActions,
      taskTableActionDialogStatus,
      hiddenSupervisorColumns,
      hiddenTaskColumns,
    } = this.state;

    return (
      <>
        <SplitterLayout
          customClassName="ingestion-view app-view"
          vertical
          percentage
          secondaryInitialSize={
            Number(localStorageGet(LocalStorageKeys.INGESTION_VIEW_PANE_SIZE)!) || 60
          }
          primaryMinSize={30}
          secondaryMinSize={30}
          onSecondaryPaneSizeChange={IngestionView.onSecondaryPaneSizeChange}
        >
          <div className="top-pane">
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
                columns={supervisorTableColumns}
                onChange={column =>
                  this.setState(prevState => ({
                    hiddenSupervisorColumns: prevState.hiddenSupervisorColumns.toggle(column),
                  }))
                }
                tableColumnsHidden={hiddenSupervisorColumns.getHiddenColumns()}
              />
            </ViewControlBar>
            {this.renderSupervisorTable()}
          </div>
          <div className="bottom-pane">
            <ViewControlBar label="Tasks">
              <Label>Group by</Label>
              <ButtonGroup>
                <Button
                  active={!groupTasksBy}
                  onClick={() => this.setState({ groupTasksBy: undefined })}
                >
                  None
                </Button>
                <Button
                  active={groupTasksBy === 'group_id'}
                  onClick={() => this.setState({ groupTasksBy: 'group_id' })}
                >
                  Group ID
                </Button>
                <Button
                  active={groupTasksBy === 'type'}
                  onClick={() => this.setState({ groupTasksBy: 'type' })}
                >
                  Type
                </Button>
                <Button
                  active={groupTasksBy === 'datasource'}
                  onClick={() => this.setState({ groupTasksBy: 'datasource' })}
                >
                  Datasource
                </Button>
                <Button
                  active={groupTasksBy === 'status'}
                  onClick={() => this.setState({ groupTasksBy: 'status' })}
                >
                  Status
                </Button>
              </ButtonGroup>
              <RefreshButton
                localStorageKey={LocalStorageKeys.TASKS_REFRESH_RATE}
                onRefresh={auto => {
                  if (auto && hasPopoverOpen()) return;
                  this.taskQueryManager.rerunLastQuery(auto);
                }}
              />
              {this.renderBulkTasksActions()}
              <TableColumnSelector
                columns={taskTableColumns}
                onChange={column =>
                  this.setState(prevState => ({
                    hiddenTaskColumns: prevState.hiddenTaskColumns.toggle(column),
                  }))
                }
                tableColumnsHidden={hiddenTaskColumns.getHiddenColumns()}
              />
            </ViewControlBar>
            {this.renderTaskTable()}
          </div>
        </SplitterLayout>
        {this.renderResumeSupervisorAction()}
        {this.renderSuspendSupervisorAction()}
        {this.renderResetSupervisorAction()}
        {this.renderTerminateSupervisorAction()}
        {this.renderKillTaskAction()}
        {supervisorSpecDialogOpen && (
          <SpecDialog
            onClose={this.closeSpecDialogs}
            onSubmit={this.submitSupervisor}
            title="Submit supervisor"
          />
        )}
        {taskSpecDialogOpen && (
          <SpecDialog
            onClose={this.closeSpecDialogs}
            onSubmit={this.submitTask}
            title="Submit task"
          />
        )}
        <Alert
          icon={IconNames.ERROR}
          intent={Intent.PRIMARY}
          isOpen={Boolean(alertErrorMsg)}
          confirmButtonText="OK"
          onConfirm={() => this.setState({ alertErrorMsg: undefined })}
        >
          <p>{alertErrorMsg}</p>
        </Alert>
        {supervisorTableActionDialogId && (
          <SupervisorTableActionDialog
            supervisorId={supervisorTableActionDialogId}
            actions={supervisorTableActionDialogActions}
            onClose={() => this.setState({ supervisorTableActionDialogId: undefined })}
          />
        )}
        {taskTableActionDialogId && taskTableActionDialogStatus && (
          <TaskTableActionDialog
            status={taskTableActionDialogStatus}
            taskId={taskTableActionDialogId}
            actions={taskTableActionDialogActions}
            onClose={() => this.setState({ taskTableActionDialogId: undefined })}
          />
        )}
      </>
    );
  }
}
