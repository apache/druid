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
  Alert,
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
import React from 'react';
import SplitterLayout from 'react-splitter-layout';
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
import {
  AsyncActionDialog,
  SpecDialog,
  SupervisorTableActionDialog,
  TaskTableActionDialog,
} from '../../dialogs';
import { AppToaster } from '../../singletons/toaster';
import {
  addFilter,
  addFilterRaw,
  booleanCustomTableFilter,
  formatDuration,
  getDruidErrorMessage,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
  queryDruidSql,
  QueryManager,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';

import './tasks-view.scss';

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
  'Location',
  'Created time',
  'Status',
  'Duration',
  ACTION_COLUMN_LABEL,
];

export interface TasksViewProps {
  taskId: string | undefined;
  datasourceId: string | undefined;
  openDialog: string | undefined;
  goToDatasource: (datasource: string) => void;
  goToQuery: (initSql: string) => void;
  goToMiddleManager: (middleManager: string) => void;
  goToLoadData: (supervisorId?: string, taskId?: string) => void;
  noSqlMode: boolean;
}

export interface TasksViewState {
  supervisorsLoading: boolean;
  supervisors: any[];
  supervisorsError?: string;

  resumeSupervisorId?: string;
  suspendSupervisorId?: string;
  resetSupervisorId?: string;
  terminateSupervisorId?: string;

  showResumeAllSupervisors: boolean;
  showSuspendAllSupervisors: boolean;
  showTerminateAllSupervisors: boolean;

  tasksLoading: boolean;
  tasks?: any[];
  tasksError?: string;

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
  hiddenTaskColumns: LocalStorageBackedArray<string>;
  hiddenSupervisorColumns: LocalStorageBackedArray<string>;
}

interface TaskQueryResultRow {
  created_time: string;
  datasource: string;
  duration: number;
  error_msg: string | null;
  location: string | null;
  status: string;
  task_id: string;
  type: string;
  rank: number;
}

interface SupervisorQueryResultRow {
  id: string;
  spec: any;
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

export class TasksView extends React.PureComponent<TasksViewProps, TasksViewState> {
  private supervisorQueryManager: QueryManager<null, SupervisorQueryResultRow[]>;
  private taskQueryManager: QueryManager<boolean, TaskQueryResultRow[]>;
  static statusRanking: Record<string, number> = {
    RUNNING: 4,
    PENDING: 3,
    WAITING: 2,
    SUCCESS: 1,
    FAILED: 1,
  };

  static TASK_SQL = `SELECT
  "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg",
  CASE WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status",
  (
    CASE WHEN "status" = 'RUNNING' THEN
     (CASE "runner_status" WHEN 'RUNNING' THEN 4 WHEN 'PENDING' THEN 3 ELSE 2 END)
    ELSE 1
    END
  ) AS "rank"
FROM sys.tasks
ORDER BY "rank" DESC, "created_time" DESC`;

  constructor(props: TasksViewProps, context: any) {
    super(props, context);

    const taskFilter: Filter[] = [];
    if (props.taskId) taskFilter.push({ id: 'task_id', value: props.taskId });
    if (props.datasourceId) taskFilter.push({ id: 'datasource', value: props.datasourceId });

    const supervisorFilter: Filter[] = [];
    if (props.datasourceId) supervisorFilter.push({ id: 'datasource', value: props.datasourceId });

    this.state = {
      supervisorsLoading: true,
      supervisors: [],

      showResumeAllSupervisors: false,
      showSuspendAllSupervisors: false,
      showTerminateAllSupervisors: false,

      tasksLoading: true,
      taskFilter: taskFilter,
      supervisorFilter: supervisorFilter,

      supervisorSpecDialogOpen: props.openDialog === 'supervisor',
      taskSpecDialogOpen: props.openDialog === 'task',

      taskTableActionDialogActions: [],
      supervisorTableActionDialogActions: [],

      hiddenTaskColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.TASK_TABLE_COLUMN_SELECTION,
      ),
      hiddenSupervisorColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.SUPERVISOR_TABLE_COLUMN_SELECTION,
      ),
    };

    this.supervisorQueryManager = new QueryManager({
      processQuery: async () => {
        const resp = await axios.get('/druid/indexer/v1/supervisor?full');
        return resp.data;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          supervisors: result,
          supervisorsLoading: loading,
          supervisorsError: error,
        });
      },
    });

    this.taskQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        if (!noSqlMode) {
          return await queryDruidSql({
            query: TasksView.TASK_SQL,
          });
        } else {
          const taskEndpoints: string[] = [
            'completeTasks',
            'runningTasks',
            'waitingTasks',
            'pendingTasks',
          ];
          const result: TaskQueryResultRow[][] = await Promise.all(
            taskEndpoints.map(async (endpoint: string) => {
              const resp = await axios.get(`/druid/indexer/v1/${endpoint}`);
              return TasksView.parseTasks(resp.data);
            }),
          );
          return ([] as TaskQueryResultRow[]).concat.apply([], result);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          tasks: result,
          tasksLoading: loading,
          tasksError: error,
        });
      },
    });
  }

  static parseTasks = (data: any[]): TaskQueryResultRow[] => {
    return data.map((d: any) => {
      return {
        created_time: d.createdTime,
        datasource: d.dataSource,
        duration: d.duration ? d.duration : 0,
        error_msg: d.errorMsg,
        location: d.location.host ? `${d.location.host}:${d.location.port}` : null,
        status: d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode,
        task_id: d.id,
        type: d.typTasksView,
        rank:
          TasksView.statusRanking[d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode],
      };
    });
  };

  private onSecondaryPaneSizeChange(secondaryPaneSize: number) {
    localStorageSet(LocalStorageKeys.TASKS_VIEW_PANE_SIZE, String(secondaryPaneSize));
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;

    this.supervisorQueryManager.runQuery(null);
    this.taskQueryManager.runQuery(noSqlMode);
  }

  componentWillUnmount(): void {
    this.supervisorQueryManager.terminate();
    this.taskQueryManager.terminate();
  }

  private closeSpecDialogs = () => {
    this.setState({
      supervisorSpecDialogOpen: false,
      taskSpecDialogOpen: false,
    });
  };

  private submitSupervisor = async (spec: JSON) => {
    try {
      await axios.post('/druid/indexer/v1/supervisor', spec);
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

  private submitTask = async (spec: JSON) => {
    try {
      await axios.post('/druid/indexer/v1/task', spec);
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
    const { goToDatasource, goToLoadData } = this.props;

    const actions: BasicAction[] = [];
    if (type === 'kafka' || type === 'kinesis') {
      actions.push(
        {
          icon: IconNames.MULTI_SELECT,
          title: 'Go to datasource',
          onAction: () => goToDatasource(id),
        },
        {
          icon: IconNames.CLOUD_UPLOAD,
          title: 'Open in data loader',
          onAction: () => goToLoadData(id),
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
        title: 'Reset',
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
          const resp = await axios.post(
            `/druid/indexer/v1/supervisor/${resumeSupervisorId}/resume`,
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
          const resp = await axios.post(
            `/druid/indexer/v1/supervisor/${suspendSupervisorId}/suspend`,
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
          const resp = await axios.post(
            `/druid/indexer/v1/supervisor/${resetSupervisorId}/reset`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Reset supervisor"
        successText="Supervisor has been reset"
        failText="Could not reset supervisor"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ resetSupervisorId: undefined });
        }}
        onSuccess={() => {
          this.supervisorQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to reset supervisor '${resetSupervisorId}'?`}</p>
        <p>Resetting a supervisor could lead data loss or data duplication</p>
      </AsyncActionDialog>
    );
  }

  renderTerminateSupervisorAction() {
    const { terminateSupervisorId } = this.state;
    if (!terminateSupervisorId) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await axios.post(
            `/druid/indexer/v1/supervisor/${terminateSupervisorId}/terminate`,
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

  renderSupervisorTable() {
    const {
      supervisors,
      supervisorsLoading,
      supervisorsError,
      hiddenSupervisorColumns,
      taskFilter,
      supervisorFilter,
    } = this.state;
    return (
      <>
        <ReactTable
          data={supervisors || []}
          loading={supervisorsLoading}
          noDataText={
            !supervisorsLoading && supervisors && !supervisors.length
              ? 'No supervisors'
              : supervisorsError || ''
          }
          filtered={supervisorFilter}
          onFilteredChange={filtered => {
            const datasourceFilter = filtered.find(filter => filter.id === 'datasource');
            let newTaskFilter = taskFilter.filter(filter => filter.id !== 'datasource');
            if (datasourceFilter) {
              newTaskFilter = addFilterRaw(
                newTaskFilter,
                datasourceFilter.id,
                datasourceFilter.value,
              );
            }
            this.setState({ supervisorFilter: filtered, taskFilter: newTaskFilter });
          }}
          filterable
          columns={[
            {
              Header: 'Datasource',
              id: 'datasource',
              accessor: 'id',
              width: 300,
              show: hiddenSupervisorColumns.exists('Datasource'),
            },
            {
              Header: 'Type',
              id: 'type',
              accessor: row => {
                const { spec } = row;
                if (!spec) return '';
                const { tuningConfig } = spec;
                if (!tuningConfig) return '';
                return tuningConfig.type;
              },
              show: hiddenSupervisorColumns.exists('Type'),
            },
            {
              Header: 'Topic/Stream',
              id: 'topic',
              accessor: row => {
                const { spec } = row;
                if (!spec) return '';
                const { ioConfig } = spec;
                if (!ioConfig) return '';
                return ioConfig.topic || ioConfig.stream || '';
              },
              show: hiddenSupervisorColumns.exists('Topic/Stream'),
            },
            {
              Header: 'Status',
              id: 'status',
              width: 300,
              accessor: row => {
                return row.detailedState;
              },
              Cell: row => {
                const value = row.original.detailedState;
                return (
                  <span>
                    <span style={{ color: stateToColor(row.original.state) }}>&#x25cf;&nbsp;</span>
                    {value}
                  </span>
                );
              },
              show: hiddenSupervisorColumns.exists('Status'),
            },
            {
              Header: ACTION_COLUMN_LABEL,
              id: ACTION_COLUMN_ID,
              accessor: 'id',
              width: ACTION_COLUMN_WIDTH,
              filterable: false,
              Cell: row => {
                const id = row.value;
                const type = row.row.type;
                const supervisorSuspended = row.original.spec.suspended;
                const supervisorActions = this.getSupervisorActions(id, supervisorSuspended, type);
                return (
                  <ActionCell
                    onDetail={() =>
                      this.setState({
                        supervisorTableActionDialogId: id,
                        supervisorTableActionDialogActions: supervisorActions,
                      })
                    }
                    actions={supervisorActions}
                  />
                );
              },
              show: hiddenSupervisorColumns.exists(ACTION_COLUMN_LABEL),
            },
          ]}
        />
        {this.renderResumeSupervisorAction()}
        {this.renderSuspendSupervisorAction()}
        {this.renderResetSupervisorAction()}
        {this.renderTerminateSupervisorAction()}
      </>
    );
  }

  private getTaskActions(
    id: string,
    datasource: string,
    status: string,
    type: string,
  ): BasicAction[] {
    const { goToDatasource, goToLoadData } = this.props;

    const actions: BasicAction[] = [];
    if (datasource && status === 'SUCCESS') {
      actions.push({
        icon: IconNames.MULTI_SELECT,
        title: 'Go to datasource',
        onAction: () => goToDatasource(datasource),
      });
    }
    if (type === 'index' || type === 'index_parallel') {
      actions.push({
        icon: IconNames.CLOUD_UPLOAD,
        title: 'Open in data loader',
        onAction: () => goToLoadData(undefined, id),
      });
    }
    if (status === 'RUNNING' || status === 'WAITING' || status === 'PENDING') {
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
          const resp = await axios.post(`/druid/indexer/v1/task/${killTaskId}/shutdown`, {});
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

  renderTaskTable() {
    const { goToMiddleManager } = this.props;
    const {
      tasks,
      tasksLoading,
      tasksError,
      taskFilter,
      groupTasksBy,
      hiddenTaskColumns,
      supervisorFilter,
    } = this.state;
    return (
      <>
        <ReactTable
          data={tasks || []}
          loading={tasksLoading}
          noDataText={!tasksLoading && tasks && !tasks.length ? 'No tasks' : tasksError || ''}
          filterable
          filtered={taskFilter}
          onFilteredChange={filtered => {
            const datasourceFilter = filtered.find(filter => filter.id === 'datasource');
            let newSupervisorFilter = supervisorFilter.filter(filter => filter.id !== 'datasource');
            if (datasourceFilter) {
              newSupervisorFilter = addFilterRaw(
                newSupervisorFilter,
                datasourceFilter.id,
                datasourceFilter.value,
              );
            }
            this.setState({ supervisorFilter: newSupervisorFilter, taskFilter: filtered });
          }}
          defaultSorted={[{ id: 'status', desc: true }]}
          pivotBy={groupTasksBy ? [groupTasksBy] : []}
          columns={[
            {
              Header: 'Task ID',
              accessor: 'task_id',
              width: 500,
              Aggregated: () => '',
              show: hiddenTaskColumns.exists('Task ID'),
            },
            {
              Header: 'Group ID',
              accessor: 'group_id',
              width: 300,
              Aggregated: () => '',
              show: hiddenTaskColumns.exists('Group ID'),
            },
            {
              Header: 'Type',
              accessor: 'type',
              width: 140,
              Cell: row => {
                const value = row.value;
                return (
                  <a
                    onClick={() => {
                      this.setState({ taskFilter: addFilter(taskFilter, 'type', value) });
                    }}
                  >
                    {value}
                  </a>
                );
              },
              show: hiddenTaskColumns.exists('Type'),
            },
            {
              Header: 'Datasource',
              accessor: 'datasource',
              Cell: row => {
                const value = row.value;
                return (
                  <a
                    onClick={() => {
                      this.setState({ taskFilter: addFilter(taskFilter, 'datasource', value) });
                    }}
                  >
                    {value}
                  </a>
                );
              },
              show: hiddenTaskColumns.exists('Datasource'),
            },

            {
              Header: 'Location',
              accessor: 'location',
              Aggregated: () => '',
              filterMethod: (filter: Filter, row: any) => {
                return booleanCustomTableFilter(filter, row.location);
              },
              show: hiddenTaskColumns.exists('Location'),
            },
            {
              Header: 'Created time',
              accessor: 'created_time',
              width: 190,
              Aggregated: () => '',
              show: hiddenTaskColumns.exists('Created time'),
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
                const { status, location } = row.original;
                const locationHostname = location ? location.split(':')[0] : null;
                const errorMsg = row.original.error_msg;
                return (
                  <span>
                    <span style={{ color: statusToColor(status) }}>&#x25cf;&nbsp;</span>
                    {status}
                    {location && (
                      <a
                        onClick={() => goToMiddleManager(locationHostname)}
                        title={`Go to: ${locationHostname}`}
                      >
                        &nbsp;&#x279A;
                      </a>
                    )}
                    {errorMsg && (
                      <a
                        onClick={() => this.setState({ alertErrorMsg: errorMsg })}
                        title={errorMsg}
                      >
                        &nbsp;?
                      </a>
                    )}
                  </span>
                );
              },
              sortMethod: (d1, d2) => {
                const typeofD1 = typeof d1;
                const typeofD2 = typeof d2;
                if (typeofD1 !== typeofD2) return 0;
                switch (typeofD1) {
                  case 'string':
                    return TasksView.statusRanking[d1] - TasksView.statusRanking[d2];

                  case 'object':
                    return (
                      TasksView.statusRanking[d1.status] - TasksView.statusRanking[d2.status] ||
                      d1.created_time.localeCompare(d2.created_time)
                    );

                  default:
                    return 0;
                }
              },
              filterMethod: (filter: Filter, row: any) => {
                return booleanCustomTableFilter(filter, row.status.status);
              },
              show: hiddenTaskColumns.exists('Status'),
            },
            {
              Header: 'Duration',
              accessor: 'duration',
              width: 70,
              filterable: false,
              Cell: row => (row.value > 0 ? formatDuration(row.value) : ''),
              Aggregated: () => '',
              show: hiddenTaskColumns.exists('Duration'),
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
                    onDetail={() =>
                      this.setState({
                        taskTableActionDialogId: id,
                        taskTableActionDialogStatus: status,
                        taskTableActionDialogActions: taskActions,
                      })
                    }
                    actions={taskActions}
                  />
                );
              },
              Aggregated: () => '',
              show: hiddenTaskColumns.exists(ACTION_COLUMN_LABEL),
            },
          ]}
        />
        {this.renderKillTaskAction()}
      </>
    );
  }

  renderBulkSupervisorActions() {
    const bulkSupervisorActionsMenu = (
      <Menu>
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
      </Menu>
    );

    return (
      <>
        <Popover content={bulkSupervisorActionsMenu} position={Position.BOTTOM_LEFT}>
          <Button icon={IconNames.MORE} />
        </Popover>
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
          const resp = await axios.post(`/druid/indexer/v1/supervisor/resumeAll`, {});
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
          const resp = await axios.post(`/druid/indexer/v1/supervisor/suspendAll`, {});
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
          const resp = await axios.post(`/druid/indexer/v1/supervisor/terminateAll`, {});
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
    const { goToQuery, noSqlMode } = this.props;

    const bulkTaskActionsMenu = (
      <Menu>
        {!noSqlMode && (
          <MenuItem
            icon={IconNames.APPLICATION}
            text="View SQL query for table"
            onClick={() => goToQuery(TasksView.TASK_SQL)}
          />
        )}
      </Menu>
    );

    return (
      <>
        <Popover content={bulkTaskActionsMenu} position={Position.BOTTOM_LEFT}>
          <Button icon={IconNames.MORE} />
        </Popover>
      </>
    );
  }

  render(): JSX.Element {
    const { goToLoadData } = this.props;
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

    const submitSupervisorMenu = (
      <Menu>
        <MenuItem
          icon={IconNames.CLOUD_UPLOAD}
          text="Go to data loader"
          onClick={() => goToLoadData()}
        />
        <MenuItem
          icon={IconNames.MANUALLY_ENTERED_DATA}
          text="Submit JSON supervisor"
          onClick={() => this.setState({ supervisorSpecDialogOpen: true })}
        />
      </Menu>
    );

    const submitTaskMenu = (
      <Menu>
        <MenuItem
          icon={IconNames.CLOUD_UPLOAD}
          text="Go to data loader"
          onClick={() => goToLoadData()}
        />
        <MenuItem
          icon={IconNames.MANUALLY_ENTERED_DATA}
          text="Submit JSON task"
          onClick={() => this.setState({ taskSpecDialogOpen: true })}
        />
      </Menu>
    );

    return (
      <>
        <SplitterLayout
          customClassName={'tasks-view app-view'}
          vertical
          percentage
          secondaryInitialSize={
            Number(localStorageGet(LocalStorageKeys.TASKS_VIEW_PANE_SIZE) as string) || 60
          }
          primaryMinSize={30}
          secondaryMinSize={30}
          onSecondaryPaneSizeChange={this.onSecondaryPaneSizeChange}
        >
          <div className={'top-pane'}>
            <ViewControlBar label="Supervisors">
              <RefreshButton
                localStorageKey={LocalStorageKeys.SUPERVISORS_REFRESH_RATE}
                onRefresh={auto => this.supervisorQueryManager.rerunLastQuery(auto)}
              />
              <Popover content={submitSupervisorMenu} position={Position.BOTTOM_LEFT}>
                <Button icon={IconNames.PLUS} text="Submit supervisor" />
              </Popover>
              {this.renderBulkSupervisorActions()}
              <TableColumnSelector
                columns={supervisorTableColumns}
                onChange={column =>
                  this.setState(prevState => ({
                    hiddenSupervisorColumns: prevState.hiddenSupervisorColumns.toggle(column),
                  }))
                }
                tableColumnsHidden={hiddenSupervisorColumns.storedArray}
              />
            </ViewControlBar>
            {this.renderSupervisorTable()}
          </div>
          <div className={'bottom-pane'}>
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
                onRefresh={auto => this.taskQueryManager.rerunLastQuery(auto)}
              />
              <Popover content={submitTaskMenu} position={Position.BOTTOM_LEFT}>
                <Button icon={IconNames.PLUS} text="Submit task" />
              </Popover>
              {this.renderBulkTasksActions()}
              <TableColumnSelector
                columns={taskTableColumns}
                onChange={column =>
                  this.setState(prevState => ({
                    hiddenTaskColumns: prevState.hiddenTaskColumns.toggle(column),
                  }))
                }
                tableColumnsHidden={hiddenTaskColumns.storedArray}
              />
            </ViewControlBar>
            {this.renderTaskTable()}
          </div>
        </SplitterLayout>
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
