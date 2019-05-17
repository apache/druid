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

import { Alert, Button, ButtonGroup, Icon, Intent, Label, Menu, MenuDivider, MenuItem, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import * as React from 'react';
import ReactTable from 'react-table';
import { Filter } from 'react-table';

import { ActionCell } from '../components/action-cell';
import { TableColumnSelection } from '../components/table-column-selection';
import { ViewControlBar } from '../components/view-control-bar';
import { AsyncActionDialog } from '../dialogs/async-action-dialog';
import { SpecDialog } from '../dialogs/spec-dialog';
import { SupervisorTableActionDialog } from '../dialogs/supervisor-table-action-dialog';
import { TaskTableActionDialog } from '../dialogs/task-table-action-dialog';
import { AppToaster } from '../singletons/toaster';
import {
  addFilter,
  booleanCustomTableFilter,
  countBy,
  formatDuration,
  getDruidErrorMessage, LocalStorageKeys,
  queryDruidSql,
  QueryManager, TableColumnSelectionHandler
} from '../utils';
import { BasicAction, basicActionsToMenu } from '../utils/basic-action';

import './tasks-view.scss';

const supervisorTableColumns: string[] = ['Datasource', 'Type', 'Topic/Stream', 'Status', 'Actions'];
const taskTableColumns: string[] = ['Task ID', 'Type', 'Datasource', 'Created time', 'Status', 'Duration', 'Actions'];

export interface TasksViewProps extends React.Props<any> {
  taskId: string | null;
  openDialog: string | null;
  goToSql: (initSql: string) => void;
  goToMiddleManager: (middleManager: string) => void;
  goToLoadDataView: () => void;
  noSqlMode: boolean;
}

export interface TasksViewState {
  supervisorsLoading: boolean;
  supervisors: any[];
  supervisorsError: string | null;

  resumeSupervisorId: string | null;
  suspendSupervisorId: string | null;
  resetSupervisorId: string | null;
  terminateSupervisorId: string | null;

  tasksLoading: boolean;
  tasks: any[] | null;
  tasksError: string | null;
  taskFilter: Filter[];
  groupTasksBy: null | 'type' | 'datasource' | 'status';

  killTaskId: string | null;

  supervisorSpecDialogOpen: boolean;
  taskSpecDialogOpen: boolean;
  initSpec: any;
  alertErrorMsg: string | null;

  taskTableActionDialogId: string | null;
  taskTableActionDialogActions: BasicAction[];
  supervisorTableActionDialogId: string | null;
  supervisorTableActionDialogActions: BasicAction[];
}

interface TaskQueryResultRow {
  created_time: string;
  datasource: string;
  duration: number;
  error_msg: string | null;
  location: string | null;
  rank: number;
  status: string;
  task_id: string;
  type: string;
}

interface SupervisorQueryResultRow {
  id: string;
  spec: any;
}

function statusToColor(status: string): string {
  switch (status) {
    case 'RUNNING': return '#2167d5';
    case 'WAITING': return '#d5631a';
    case 'PENDING': return '#ffbf00';
    case 'SUCCESS': return '#57d500';
    case 'FAILED': return '#d5100a';
    default: return '#0a1500';
  }
}

export class TasksView extends React.Component<TasksViewProps, TasksViewState> {
  private supervisorQueryManager: QueryManager<string, SupervisorQueryResultRow[]>;
  private taskQueryManager: QueryManager<string, TaskQueryResultRow[]>;
  private supervisorTableColumnSelectionHandler: TableColumnSelectionHandler;
  private taskTableColumnSelectionHandler: TableColumnSelectionHandler;
  static statusRanking: Record<string, number> = {RUNNING: 4, PENDING: 3, WAITING: 2, SUCCESS: 1, FAILED: 1};

  constructor(props: TasksViewProps, context: any) {
    super(props, context);
    this.state = {
      supervisorsLoading: true,
      supervisors: [],
      supervisorsError: null,

      resumeSupervisorId: null,
      suspendSupervisorId: null,
      resetSupervisorId: null,
      terminateSupervisorId: null,

      tasksLoading: true,
      tasks: null,
      tasksError: null,
      taskFilter: props.taskId ? [{ id: 'task_id', value: props.taskId }] : [],
      groupTasksBy: null,

      killTaskId: null,

      supervisorSpecDialogOpen: props.openDialog === 'supervisor',
      taskSpecDialogOpen: props.openDialog === 'task',
      initSpec: null,
      alertErrorMsg: null,

      taskTableActionDialogId: null,
      taskTableActionDialogActions: [],
      supervisorTableActionDialogId: null,
      supervisorTableActionDialogActions: []
    };

    this.supervisorTableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.SUPERVISOR_TABLE_COLUMN_SELECTION, () => this.setState({})
    );

    this.taskTableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.TASK_TABLE_COLUMN_SELECTION, () => this.setState({})
    );
  }

  static parseTasks = (data: any[]): TaskQueryResultRow[] => {
    return data.map((d: any) => {
      return {
        created_time: d.createdTime,
        datasource: d.dataSource,
        duration: d.duration ? d.duration : 0,
        error_msg: d.errorMsg,
        location: d.location.host ? `${d.location.host}:${d.location.port}` : null,
        rank: (TasksView.statusRanking as any)[d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode],
        status: d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode,
        task_id: d.id,
        type: d.typTasksView
      };
    });
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;
    this.supervisorQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const resp = await axios.get('/druid/indexer/v1/supervisor?full');
        return resp.data;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          supervisors: result,
          supervisorsLoading: loading,
          supervisorsError: error
        });
      }
    });

    this.supervisorQueryManager.runQuery('dummy');

    this.taskQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        if (!noSqlMode) {
          return await queryDruidSql({ query });
        } else {
          const taskEndpoints: string[] = ['completeTasks', 'runningTasks', 'waitingTasks', 'pendingTasks'];
          const result: TaskQueryResultRow[][] = await Promise.all(taskEndpoints.map(async (endpoint: string) => {
            const resp = await axios.get(`/druid/indexer/v1/${endpoint}`);
            return TasksView.parseTasks(resp.data);
          }));
          return [].concat.apply([], result);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          tasks: result,
          tasksLoading: loading,
          tasksError: error
        });
      }
    });

    // Ranking
    //   RUNNING => 4
    //   PENDING => 3
    //   WAITING => 2
    //   SUCCESS => 1
    //   FAILED => 1

    this.taskQueryManager.runQuery(`SELECT
  "task_id", "type", "datasource", "created_time",
  CASE WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status",
  CASE WHEN "status" = 'RUNNING' THEN
   (CASE WHEN "runner_status" = 'RUNNING' THEN 4 WHEN "runner_status" = 'PENDING' THEN 3 ELSE 2 END)
  ELSE 1 END AS "rank",
  "location", "duration", "error_msg"
FROM sys.tasks
ORDER BY "rank" DESC, "created_time" DESC`);

  }

  componentWillUnmount(): void {
    this.supervisorQueryManager.terminate();
    this.taskQueryManager.terminate();
  }

  private closeSpecDialogs = () => {
    this.setState({
      supervisorSpecDialogOpen: false,
      taskSpecDialogOpen: false,
      initSpec: null
    });
  }

  private submitSupervisor = async (spec: JSON) => {
    try {
      await axios.post('/druid/indexer/v1/supervisor', spec);
    } catch (e) {
      AppToaster.show({
        message: `Failed to submit supervisor: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER
      });
      return;
    }

    AppToaster.show({
      message: 'Supervisor submitted successfully',
      intent: Intent.SUCCESS
    });
    this.supervisorQueryManager.rerunLastQuery();
  }

  private submitTask = async (spec: JSON) => {
    try {
      await axios.post('/druid/indexer/v1/task', spec);
    } catch (e) {
      AppToaster.show({
        message: `Failed to submit task: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER
      });
      return;
    }

    AppToaster.show({
      message: 'Task submitted successfully',
      intent: Intent.SUCCESS
    });
    this.taskQueryManager.rerunLastQuery();
  }

  private getSupervisorActions(id: string, supervisorSuspended: boolean): BasicAction[] {
    return [
      {
        icon: IconNames.STEP_BACKWARD,
        title: 'Reset',
        onAction: () => this.setState({ resetSupervisorId: id })
      },
      {
        icon: supervisorSuspended ? IconNames.PLAY : IconNames.PAUSE,
        title: supervisorSuspended ? 'Resume' : 'Suspend',
        onAction: () => supervisorSuspended ? this.setState({ resumeSupervisorId: id }) : this.setState({ suspendSupervisorId: id })
      },
      {
        icon: IconNames.CROSS,
        title: 'Terminate',
        intent: Intent.DANGER,
        onAction: () => this.setState({ terminateSupervisorId: id })
      }
    ];
  }

  renderResumeSupervisorAction() {
    const { resumeSupervisorId } = this.state;

    return <AsyncActionDialog
      action={
        resumeSupervisorId ? async () => {
          const resp = await axios.post(`/druid/indexer/v1/supervisor/${resumeSupervisorId}/suspend`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Resume supervisor"
      successText="Supervisor has been resumed"
      failText="Could not resume supervisor"
      intent={Intent.PRIMARY}
      onClose={(success) => {
        this.setState({ resumeSupervisorId: null });
        if (success) this.supervisorQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to resume supervisor '${resumeSupervisorId}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderSuspendSupervisorAction() {
    const { suspendSupervisorId } = this.state;

    return <AsyncActionDialog
      action={
        suspendSupervisorId ? async () => {
          const resp = await axios.post(`/druid/indexer/v1/supervisor/${suspendSupervisorId}/suspend`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Suspend supervisor"
      successText="Supervisor has been suspended"
      failText="Could not suspend supervisor"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ suspendSupervisorId: null });
        if (success) this.supervisorQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to suspend supervisor '${suspendSupervisorId}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderResetSupervisorAction() {
    const { resetSupervisorId } = this.state;

    return <AsyncActionDialog
      action={
        resetSupervisorId ? async () => {
          const resp = await axios.post(`/druid/indexer/v1/supervisor/${resetSupervisorId}/reset`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Reset supervisor"
      successText="Supervisor has been reset"
      failText="Could not reset supervisor"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ resetSupervisorId: null });
        if (success) this.supervisorQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to reset supervisor '${resetSupervisorId}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderTerminateSupervisorAction() {
    const { terminateSupervisorId } = this.state;

    return <AsyncActionDialog
      action={
        terminateSupervisorId ? async () => {
          const resp = await axios.post(`/druid/indexer/v1/supervisor/${terminateSupervisorId}/terminate`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Terminate supervisor"
      successText="Supervisor has been terminated"
      failText="Could not terminate supervisor"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ terminateSupervisorId: null });
        if (success) this.supervisorQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to terminate supervisor '${terminateSupervisorId}'?`}
      </p>
      <p>
        This action is not reversible.
      </p>
    </AsyncActionDialog>;
  }

  renderSupervisorTable() {
    const { supervisors, supervisorsLoading, supervisorsError } = this.state;
    const { supervisorTableColumnSelectionHandler } = this;

    return <>
      <ReactTable
        data={supervisors || []}
        loading={supervisorsLoading}
        noDataText={!supervisorsLoading && supervisors && !supervisors.length ? 'No supervisors' : (supervisorsError || '')}
        filterable
        columns={[
          {
            Header: 'Datasource',
            id: 'datasource',
            accessor: 'id',
            width: 300,
            show: supervisorTableColumnSelectionHandler.showColumn('Datasource')
          },
          {
            Header: 'Type',
            id: 'type',
            accessor: (row) => {
              const { spec } = row;
              if (!spec) return '';
              const { tuningConfig } = spec;
              if (!tuningConfig) return '';
              return tuningConfig.type;
            },
            show: supervisorTableColumnSelectionHandler.showColumn('Type')
          },
          {
            Header: 'Topic/Stream',
            id: 'topic',
            accessor: (row) => {
              const { spec } = row;
              if (!spec) return '';
              const { ioConfig } = spec;
              if (!ioConfig) return '';
              return ioConfig.topic || ioConfig.stream || '';
            },
            show: supervisorTableColumnSelectionHandler.showColumn('Topic/Stream')
          },
          {
            Header: 'Status',
            id: 'status',
            accessor: (row) => row.spec.suspended ? 'Suspended' : 'Running',
            Cell: row => {
              const value = row.value;
              return <span>
                <span
                  style={{ color: value === 'Suspended' ? '#d58512' : '#2167d5' }}
                >
                  &#x25cf;&nbsp;
                </span>
                {value}
              </span>;
            },
            show: supervisorTableColumnSelectionHandler.showColumn('Status')
          },
          {
            Header: 'Actions',
            id: 'actions',
            accessor: 'id',
            width: 70,
            filterable: false,
            Cell: row => {
              const id = row.value;
              const supervisorSuspended = row.original.spec.suspended;
              const supervisorActions = this.getSupervisorActions(id, supervisorSuspended);
              const supervisorMenu = basicActionsToMenu(supervisorActions);

              return <ActionCell>
                <Icon
                  icon={IconNames.SEARCH_TEMPLATE}
                  onClick={() => this.setState({
                    supervisorTableActionDialogId: id,
                    supervisorTableActionDialogActions: supervisorActions
                  })}
                />
                {
                  supervisorMenu &&
                  <Popover content={supervisorMenu} position={Position.BOTTOM_RIGHT}>
                    <Icon icon={IconNames.WRENCH}/>
                  </Popover>
                }
              </ActionCell>;
            },
            show: supervisorTableColumnSelectionHandler.showColumn('Actions')
          }
        ]}
        defaultPageSize={5}
        className="-striped -highlight"
      />
      {this.renderResumeSupervisorAction()}
      {this.renderSuspendSupervisorAction()}
      {this.renderResetSupervisorAction()}
      {this.renderTerminateSupervisorAction()}
    </>;
  }

  // --------------------------------------

  private getTaskActions(id: string, status: string): BasicAction[] {
    if (status !== 'RUNNING' && status !== 'WAITING' && status !== 'PENDING') return [];
    return [
      {
        icon: IconNames.CROSS,
        title: 'Kill',
        intent: Intent.DANGER,
        onAction: () => this.setState({ killTaskId: id })
      }
    ];
  }

  renderKillTaskAction() {
    const { killTaskId } = this.state;

    return <AsyncActionDialog
      action={
        killTaskId ? async () => {
          const resp = await axios.post(`/druid/indexer/v1/task/${killTaskId}/shutdown`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Kill task"
      successText="Task was killed"
      failText="Could not kill task"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ killTaskId: null });
        if (success) this.taskQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to kill task '${killTaskId}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderTaskTable() {
    const { goToMiddleManager } = this.props;
    const { tasks, tasksLoading, tasksError, taskFilter, groupTasksBy } = this.state;
    const { taskTableColumnSelectionHandler } = this;

    return <>
      <ReactTable
        data={tasks || []}
        loading={tasksLoading}
        noDataText={!tasksLoading && tasks && !tasks.length ? 'No tasks' : (tasksError || '')}
        filterable
        filtered={taskFilter}
        onFilteredChange={(filtered, column) => {
          this.setState({ taskFilter: filtered });
        }}
        defaultSorted={[{id: 'status', desc: true}]}
        pivotBy={groupTasksBy ? [groupTasksBy] : []}
        columns={[
          {
            Header: 'Task ID',
            accessor: 'task_id',
            width: 300,
            Aggregated: row => '',
            show: taskTableColumnSelectionHandler.showColumn('Task ID')
          },
          {
            Header: 'Type',
            accessor: 'type',
            Cell: row => {
              const value = row.value;
              return <a onClick={() => { this.setState({ taskFilter: addFilter(taskFilter, 'type', value) }); }}>{value}</a>;
            },
            show: taskTableColumnSelectionHandler.showColumn('Type')
          },
          {
            Header: 'Datasource',
            accessor: 'datasource',
            Cell: row => {
              const value = row.value;
              return <a onClick={() => { this.setState({ taskFilter: addFilter(taskFilter, 'datasource', value) }); }}>{value}</a>;
            },
            show: taskTableColumnSelectionHandler.showColumn('Datasource')
          },
          {
            Header: 'Created time',
            accessor: 'created_time',
            width: 120,
            Aggregated: row => '',
            show: taskTableColumnSelectionHandler.showColumn('Created time')
          },
          {
            Header: 'Status',
            id: 'status',
            width: 110,
            accessor: (row) => ({ status: row.status, created_time: row.created_time, toString: () => row.status }),
            Cell: row => {
              if (row.aggregated) return '';
              const { status, location } = row.original;
              const locationHostname = location ? location.split(':')[0] : null;
              const errorMsg = row.original.error_msg;
              return <span>
                <span
                  style={{ color: statusToColor(status) }}
                >
                  &#x25cf;&nbsp;
                </span>
                {status}
                {location && <a onClick={() => goToMiddleManager(locationHostname)} title={`Go to: ${locationHostname}`}>&nbsp;&#x279A;</a>}
                {errorMsg && <a onClick={() => this.setState({ alertErrorMsg: errorMsg })} title={errorMsg}>&nbsp;?</a>}
              </span>;
            },
            sortMethod: (d1, d2) => {
              const typeofD1 = typeof d1;
              const typeofD2 = typeof d2;
              if (typeofD1 !== typeofD2) return 0;
              switch (typeofD1) {
                case 'string':
                  return TasksView.statusRanking[d1] - TasksView.statusRanking[d2];

                case 'object':
                  return TasksView.statusRanking[d1.status] - TasksView.statusRanking[d2.status] || d1.created_time.localeCompare(d2.created_time);

                default:
                  return 0;
              }
            },
            filterMethod: (filter: Filter, row: any) => {
              return booleanCustomTableFilter(filter, row.status.status);
            },
            show: taskTableColumnSelectionHandler.showColumn('Status')
          },
          {
            Header: 'Duration',
            accessor: 'duration',
            filterable: false,
            Cell: (row) => row.value > 0 ? formatDuration(row.value) : '',
            Aggregated: () => '',
            show: taskTableColumnSelectionHandler.showColumn('Duration')
          },
          {
            Header: 'Actions',
            id: 'actions',
            accessor: 'task_id',
            width: 70,
            filterable: false,
            Cell: row => {
              if (row.aggregated) return '';
              const id = row.value;
              const { status } = row.original;
              const taskActions = this.getTaskActions(id, status);
              const taskMenu = basicActionsToMenu(taskActions);

              return <ActionCell>
                <Icon
                  icon={IconNames.SEARCH_TEMPLATE}
                  onClick={() => this.setState({
                    taskTableActionDialogId: id,
                    taskTableActionDialogActions: taskActions
                  })}
                />
                {
                  taskMenu &&
                  <Popover content={taskMenu} position={Position.BOTTOM_RIGHT}>
                    <Icon icon={IconNames.WRENCH}/>
                  </Popover>
                }
              </ActionCell>;
            },
            Aggregated: row => '',
            show: taskTableColumnSelectionHandler.showColumn('Actions')
          }
        ]}
        defaultPageSize={20}
        className="-striped -highlight"
      />
      {this.renderKillTaskAction()}
    </>;
  }

  render() {
    const { goToSql, goToLoadDataView, noSqlMode } = this.props;
    const { groupTasksBy, supervisorSpecDialogOpen, taskSpecDialogOpen, alertErrorMsg, taskTableActionDialogId, taskTableActionDialogActions, supervisorTableActionDialogId, supervisorTableActionDialogActions } = this.state;
    const { supervisorTableColumnSelectionHandler, taskTableColumnSelectionHandler } = this;

    const submitTaskMenu = <Menu>
      <MenuItem
        text="Raw JSON task"
        onClick={() => this.setState({ taskSpecDialogOpen: true })}
      />
      <MenuItem
        text="Go to data loader"
        onClick={() => goToLoadDataView()}
      />
    </Menu>;

    return <div className="tasks-view app-view">
      <ViewControlBar label="Supervisors">
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.supervisorQueryManager.rerunLastQuery()}
        />
        <Button
          icon={IconNames.PLUS}
          text="Submit supervisor"
          onClick={() => this.setState({ supervisorSpecDialogOpen: true })}
        />
        <TableColumnSelection
          columns={supervisorTableColumns}
          onChange={(column) => supervisorTableColumnSelectionHandler.changeTableColumnSelection(column)}
          tableColumnsHidden={supervisorTableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderSupervisorTable()}

      <div className="control-separator"/>

      <ViewControlBar label="Tasks">
        <Label>Group by</Label>
        <ButtonGroup>
          <Button active={groupTasksBy === null} onClick={() => this.setState({ groupTasksBy: null })}>None</Button>
          <Button active={groupTasksBy === 'type'} onClick={() => this.setState({ groupTasksBy: 'type' })}>Type</Button>
          <Button active={groupTasksBy === 'datasource'} onClick={() => this.setState({ groupTasksBy: 'datasource' })}>Datasource</Button>
          <Button active={groupTasksBy === 'status'} onClick={() => this.setState({ groupTasksBy: 'status' })}>Status</Button>
        </ButtonGroup>
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.taskQueryManager.rerunLastQuery()}
        />
        {
          !noSqlMode &&
          <Button
            icon={IconNames.APPLICATION}
            text="Go to SQL"
            onClick={() => goToSql(this.taskQueryManager.getLastQuery())}
          />
        }
        <Popover content={submitTaskMenu} position={Position.BOTTOM_LEFT}>
          <Button icon={IconNames.PLUS} text="Submit task"/>
        </Popover>
        <TableColumnSelection
          columns={taskTableColumns}
          onChange={(column) => taskTableColumnSelectionHandler.changeTableColumnSelection(column)}
          tableColumnsHidden={taskTableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderTaskTable()}
      {
        supervisorSpecDialogOpen &&
        <SpecDialog
          onClose={this.closeSpecDialogs}
          onSubmit={this.submitSupervisor}
          title="Submit supervisor"
        />
      }
      {
        taskSpecDialogOpen &&
        <SpecDialog
          onClose={this.closeSpecDialogs}
          onSubmit={this.submitTask}
          title="Submit task"
        />
      }
      <Alert
        icon={IconNames.ERROR}
        intent={Intent.PRIMARY}
        isOpen={Boolean(alertErrorMsg)}
        confirmButtonText="OK"
        onConfirm={() => this.setState({ alertErrorMsg: null })}
      >
        <p>{alertErrorMsg}</p>
      </Alert>
      {
        supervisorTableActionDialogId &&
        <SupervisorTableActionDialog
          isOpen
          supervisorId={supervisorTableActionDialogId}
          actions={supervisorTableActionDialogActions}
          onClose={() => this.setState({supervisorTableActionDialogId: null})}
        />
      }
      {
        taskTableActionDialogId &&
        <TaskTableActionDialog
          isOpen
          taskId={taskTableActionDialogId}
          actions={taskTableActionDialogActions}
          onClose={() => this.setState({taskTableActionDialogId: null})}
        />
      }
    </div>;
  }
}
