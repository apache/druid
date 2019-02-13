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

import axios from 'axios';
import * as React from 'react';
import * as classNames from 'classnames';
import ReactTable from "react-table";
import { Filter } from "react-table";
import { Button, H1, ButtonGroup, Intent, Label, Alert } from "@blueprintjs/core";
import { IconNames } from "@blueprintjs/icons";
import { addFilter, QueryManager, getDruidErrorMessage, countBy, formatDuration, queryDruidSql } from "../utils";
import { AsyncActionDialog } from "../dialogs/async-action-dialog";
import { SpecDialog } from "../dialogs/spec-dialog";
import { AppToaster } from '../singletons/toaster';
import "./tasks-view.scss";

export interface TasksViewProps extends React.Props<any> {
  taskId: string | null;
  goToSql: (initSql: string) => void;
  goToMiddleManager: (middleManager: string) => void;
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
  alertErrorMsg: string | null;
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
  private supervisorQueryManager: QueryManager<string, any[]>;
  private taskQueryManager: QueryManager<string, any[]>;

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

      supervisorSpecDialogOpen: false,
      taskSpecDialogOpen: false,
      alertErrorMsg: null
    };
  }

  componentDidMount(): void {
    this.supervisorQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const resp = await axios.get("/druid/indexer/v1/supervisor?full")
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
        return await queryDruidSql({ query });
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

  private async submitSupervisor(spec: JSON) {
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

  private async submitTask(spec: JSON) {
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
          const resp = await axios.post(`/druid/indexer/v1/supervisor/${terminateSupervisorId}/reset`, {});
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

    return <>
      <ReactTable
        data={supervisors || []}
        loading={supervisorsLoading}
        noDataText={!supervisorsLoading && supervisors && !supervisors.length ? 'No supervisors' : (supervisorsError || '')}
        filterable={true}
        columns={[
          {
            Header: "Datasource",
            id: 'datasource',
            accessor: "id",
            width: 300
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
            }
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
            }
          },
          {
            Header: "Status",
            id: 'status',
            accessor: (row) => row.spec.suspended ? 'Suspended' : 'Running',
            Cell: row => {
              const value = row.value;
              return <span>
                <span
                  style={{ color: value === 'Suspended' ? '#d58512' : '#2167d5' }}
                >&#x25cf;&nbsp;</span>
                {value}
              </span>;
            }
          },
          {
            Header: 'Actions',
            id: 'actions',
            accessor: 'id',
            width: 420,
            filterable: false,
            Cell: row => {
              const id = row.value;
              const suspendResume = row.original.spec.suspended ?
                <a onClick={() => this.setState({ resumeSupervisorId: id })}>Resume</a> :
                <a onClick={() => this.setState({ suspendSupervisorId: id })}>Suspend</a>;

              return <div>
                <a href={`/druid/indexer/v1/supervisor/${id}`} target="_blank">Payload</a>&nbsp;&nbsp;&nbsp;
                <a href={`/druid/indexer/v1/supervisor/${id}/status`} target="_blank">Status</a>&nbsp;&nbsp;&nbsp;
                <a href={`/druid/indexer/v1/supervisor/${id}/stats`} target="_blank">Stats</a>&nbsp;&nbsp;&nbsp;
                <a href={`/druid/indexer/v1/supervisor/${id}/history`} target="_blank">History</a>&nbsp;&nbsp;&nbsp;
                {suspendResume}&nbsp;&nbsp;&nbsp;
                <a onClick={() => this.setState({ resetSupervisorId: id })}>Reset</a>&nbsp;&nbsp;&nbsp;
                <a onClick={() => this.setState({ terminateSupervisorId: id })}>Terminate</a>
              </div>
            }
          }
        ]}
        defaultPageSize={10}
        className="-striped -highlight"
      />
      {this.renderResumeSupervisorAction()}
      {this.renderSuspendSupervisorAction()}
      {this.renderResetSupervisorAction()}
      {this.renderTerminateSupervisorAction()}
    </>;
  }

  // --------------------------------------

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

    return <>
      <ReactTable
        data={tasks || []}
        loading={tasksLoading}
        noDataText={!tasksLoading && tasks && !tasks.length ? 'No tasks' : (tasksError || '')}
        filterable={true}
        filtered={taskFilter}
        onFilteredChange={(filtered, column) => {
          this.setState({ taskFilter: filtered });
        }}
        defaultSorted={[{id: "status", desc: true}]}
        pivotBy={groupTasksBy ? [groupTasksBy] : []}
        columns={[
          {
            Header: "Task ID",
            accessor: "task_id",
            width: 300,
            Aggregated: row => ''
          },
          {
            Header: "Type",
            accessor: "type",
            Cell: row => {
              const value = row.value;
              return <a onClick={() => { this.setState({ taskFilter: addFilter(taskFilter, 'type', value) }) }}>{value}</a>
            }
          },
          {
            Header: "Datasource",
            accessor: "datasource",
            Cell: row => {
              const value = row.value;
              return <a onClick={() => { this.setState({ taskFilter: addFilter(taskFilter, 'datasource', value) }) }}>{value}</a>
            }
          },
          {
            Header: "Created time",
            accessor: "created_time",
            width: 120,
            Aggregated: row => ''
          },
          {
            Header: "Status",
            id: "status",
            width: 110,
            accessor: (row) => `${row.rank}_${row.created_time}_${row.status}`,
            Cell: row => {
              if (row.aggregated) return '';
              const { status, location } = row.original;
              const locationHostname = location ? location.split(':')[0] : null;
              const errorMsg = row.original.error_msg;
              return <span>
                <span
                  style={{ color: statusToColor(status) }}
                >&#x25cf;&nbsp;</span>
                { status }
                { location && <a onClick={() => goToMiddleManager(locationHostname)} title={`Go to: ${locationHostname}`}>&nbsp;&#x279A;</a> }
                { errorMsg && <a onClick={() => this.setState({ alertErrorMsg: errorMsg })} title={errorMsg}>&nbsp;?</a> }
              </span>;
            },
            PivotValue: (opt) => {
              const { subRows, value } = opt;
              if (!subRows || !subRows.length) return '';
              return `${subRows[0]._original['status']} (${subRows.length})`;
            },
            Aggregated: (opt: any) => {
              const { subRows, column } = opt;
              const previewValues = subRows.filter((d: any) => typeof d[column.id] !== 'undefined').map((row: any) => row._original[column.id]);
              const previewCount = countBy(previewValues);
              return <span>{Object.keys(previewCount).sort().map(v => `${v} (${previewCount[v]})`).join(', ')}</span>;
            }
          },
          {
            Header: "Duration",
            accessor: "duration",
            filterable: false,
            Cell: (row) => row.value > 0 ? formatDuration(row.value) : '',
            Aggregated: () => ''
          },
          {
            Header: 'Actions',
            id: 'actions',
            accessor: 'task_id',
            width: 360,
            filterable: false,
            Cell: row => {
              if (row.aggregated) return '';
              const id = row.value;
              const { status } = row.original;
              return <div>
                <a href={`/druid/indexer/v1/task/${id}`} target="_blank">Payload</a>&nbsp;&nbsp;&nbsp;
                <a href={`/druid/indexer/v1/task/${id}/status`} target="_blank">Status</a>&nbsp;&nbsp;&nbsp;
                <a href={`/druid/indexer/v1/task/${id}/reports`} target="_blank">Reports</a>&nbsp;&nbsp;&nbsp;
                <a href={`/druid/indexer/v1/task/${id}/log`} target="_blank">Log (all)</a>&nbsp;&nbsp;&nbsp;
                <a href={`/druid/indexer/v1/task/${id}/log?offset=-8192`} target="_blank">Log (last 8kb)</a>&nbsp;&nbsp;&nbsp;
                { (status === 'RUNNING') && <a onClick={() => this.setState({ killTaskId: id })}>Kill</a> }
              </div>
            },
            Aggregated: row => ''
          }
        ]}
        defaultPageSize={20}
        className="-striped -highlight"
      />
      {this.renderKillTaskAction()}
    </>;
  }

  render() {
    const { goToSql } = this.props;
    const { groupTasksBy, supervisorSpecDialogOpen, taskSpecDialogOpen, alertErrorMsg } = this.state;

    return <div className="tasks-view app-view">
      <div className="control-bar">
        <div className="control-label">Supervisors</div>
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
      </div>
      {this.renderSupervisorTable()}

      <div className="control-separator"/>

      <div className="control-bar">
        <div className="control-label">Tasks</div>
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
        <Button
          icon={IconNames.CONSOLE}
          text="Go to SQL"
          onClick={() => goToSql(this.taskQueryManager.getLastQuery())}
        />
        <Button
          icon={IconNames.PLUS}
          text="Submit task"
          onClick={() => this.setState({ taskSpecDialogOpen: true })}
        />
      </div>
      {this.renderTaskTable()}

      <SpecDialog
        isOpen={ supervisorSpecDialogOpen }
        onClose={() => this.setState({ supervisorSpecDialogOpen: false })}
        onSubmit={this.submitSupervisor}
        title="Submit supervisor"
      />
      <SpecDialog
        isOpen={ taskSpecDialogOpen }
        onClose={() => this.setState({ taskSpecDialogOpen: false })}
        onSubmit={this.submitTask}
        title="Submit task"
      />
      <Alert
        icon={IconNames.ERROR}
        intent={Intent.PRIMARY}
        isOpen={Boolean(alertErrorMsg)}
        confirmButtonText="OK"
        onConfirm={() => this.setState({ alertErrorMsg: null })}
      >
        <p>{alertErrorMsg}</p>
      </Alert>
    </div>
  }
}

