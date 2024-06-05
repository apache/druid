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

import { Button, ButtonGroup, Intent, Label, MenuItem, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
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
  TableClickableCell,
  TableColumnSelector,
  TableFilterableCell,
  ViewControlBar,
} from '../../components';
import { AlertDialog, AsyncActionDialog, SpecDialog, TaskTableActionDialog } from '../../dialogs';
import type { QueryWithContext } from '../../druid-models';
import { TASK_CANCELED_ERROR_MESSAGES, TASK_CANCELED_PREDICATE } from '../../druid-models';
import type { Capabilities } from '../../helpers';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import {
  formatDuration,
  getDruidErrorMessage,
  hasPopoverOpen,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  oneOf,
  queryDruidSql,
  QueryManager,
  QueryState,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';
import { ExecutionDetailsDialog } from '../workbench-view/execution-details-dialog/execution-details-dialog';

import './tasks-view.scss';

const taskTableColumns: string[] = [
  'Task ID',
  'Group ID',
  'Type',
  'Datasource',
  'Status',
  'Created time',
  'Duration',
  'Location',
];

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

export interface TasksViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
  openTaskDialog: boolean | undefined;
  goToDatasource(datasource: string): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  goToClassicBatchDataLoader(taskId?: string): void;
  capabilities: Capabilities;
}

export interface TasksViewState {
  tasksState: QueryState<TaskQueryResultRow[]>;

  groupTasksBy?: 'group_id' | 'type' | 'datasource' | 'status';

  killTaskId?: string;

  taskSpecDialogOpen: boolean;
  alertErrorMsg?: string;

  taskTableActionDialogOpen?: { id: string; status: string; actions: BasicAction[] };
  executionDialogOpen?: string;
  visibleColumns: LocalStorageBackedVisibility;
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

export class TasksView extends React.PureComponent<TasksViewProps, TasksViewState> {
  private readonly taskQueryManager: QueryManager<Capabilities, TaskQueryResultRow[]>;
  static statusRanking: Record<string, number> = {
    RUNNING: 4,
    PENDING: 3,
    WAITING: 2,
    SUCCESS: 1,
    FAILED: 1,
  };

  static TASK_SQL = `WITH tasks AS (SELECT
  "task_id", "group_id", "type", "datasource", "created_time", "location", "duration", "error_msg",
  CASE WHEN ${TASK_CANCELED_PREDICATE} THEN 'CANCELED' WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status"
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

  constructor(props: TasksViewProps) {
    super(props);

    this.state = {
      tasksState: QueryState.INIT,

      taskSpecDialogOpen: Boolean(props.openTaskDialog),

      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.TASK_TABLE_COLUMN_SELECTION,
      ),
    };

    this.taskQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasSql()) {
          return await queryDruidSql({
            query: TasksView.TASK_SQL,
          });
        } else if (capabilities.hasOverlordAccess()) {
          const resp = await Api.instance.get(`/druid/indexer/v1/tasks`);
          return TasksView.parseTasks(resp.data);
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

  componentDidMount(): void {
    const { capabilities } = this.props;

    this.taskQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.taskQueryManager.terminate();
  }

  private readonly closeSpecDialogs = () => {
    this.setState({
      taskSpecDialogOpen: false,
    });
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

  private getTaskActions(
    id: string,
    datasource: string,
    status: string,
    type: string,
    fromTable?: boolean,
  ): BasicAction[] {
    const { goToDatasource, goToClassicBatchDataLoader } = this.props;

    const actions: BasicAction[] = [];
    if (fromTable) {
      actions.push({
        icon: IconNames.SEARCH_TEMPLATE,
        title: 'View raw details',
        onAction: () => {
          this.setState({
            taskTableActionDialogOpen: {
              id,
              status,
              actions: this.getTaskActions(id, datasource, status, type),
            },
          });
        },
      });
    }
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
        <p>
          Are you sure you want to kill task <Tag minimal>{killTaskId}</Tag>?
        </p>
      </AsyncActionDialog>
    );
  }

  private renderTaskFilterableCell(field: string) {
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

  private onTaskDetail(task: TaskQueryResultRow) {
    if (task.type === 'query_controller') {
      this.setState({
        executionDialogOpen: task.task_id,
      });
    } else {
      this.setState({
        taskTableActionDialogOpen: {
          id: task.task_id,
          status: task.status,
          actions: this.getTaskActions(task.task_id, task.datasource, task.status, task.type),
        },
      });
    }
  }

  private renderTaskTable() {
    const { filters, onFiltersChange } = this.props;
    const { tasksState, groupTasksBy, visibleColumns } = this.state;

    const tasks = tasksState.data || [];
    return (
      <ReactTable
        data={tasks}
        loading={tasksState.loading}
        noDataText={tasksState.isEmpty() ? 'No tasks' : tasksState.getErrorMessage() || ''}
        filterable
        filtered={filters}
        onFilteredChange={onFiltersChange}
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
                hoverIcon={IconNames.SEARCH_TEMPLATE}
              >
                {value}
              </TableClickableCell>
            ),
            Aggregated: () => '',
            show: visibleColumns.shown('Task ID'),
          },
          {
            Header: 'Group ID',
            accessor: 'group_id',
            width: 300,
            Cell: this.renderTaskFilterableCell('group_id'),
            Aggregated: () => '',
            show: visibleColumns.shown('Group ID'),
          },
          {
            Header: 'Type',
            accessor: 'type',
            width: 140,
            Cell: this.renderTaskFilterableCell('type'),
            show: visibleColumns.shown('Type'),
          },
          {
            Header: 'Datasource',
            accessor: 'datasource',
            width: 200,
            Cell: this.renderTaskFilterableCell('datasource'),
            show: visibleColumns.shown('Datasource'),
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
                  filters={filters}
                  onFiltersChange={onFiltersChange}
                >
                  <span title={errorMsg}>
                    <span style={{ color: statusToColor(status) }}>&#x25cf;&nbsp;</span>
                    {status}
                    {errorMsg && !TASK_CANCELED_ERROR_MESSAGES.includes(errorMsg) && (
                      <a onClick={() => this.setState({ alertErrorMsg: errorMsg })}>&nbsp;?</a>
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
            show: visibleColumns.shown('Status'),
          },
          {
            Header: 'Created time',
            accessor: 'created_time',
            width: 190,
            Cell: this.renderTaskFilterableCell('created_time'),
            Aggregated: () => '',
            show: visibleColumns.shown('Created time'),
          },
          {
            Header: 'Duration',
            accessor: 'duration',
            width: 80,
            filterable: false,
            className: 'padded',
            Cell({ value, original, aggregated }) {
              if (aggregated) return '';
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
            show: visibleColumns.shown('Duration'),
          },
          {
            Header: 'Location',
            accessor: 'location',
            width: 200,
            Cell: this.renderTaskFilterableCell('location'),
            Aggregated: () => '',
            show: visibleColumns.shown('Location'),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            id: ACTION_COLUMN_ID,
            accessor: 'task_id',
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
            sortable: false,
            Cell: row => {
              if (row.aggregated) return '';
              const id = row.value;
              const type = row.row.type;
              const { datasource, status } = row.original;
              return (
                <ActionCell
                  onDetail={() => this.onTaskDetail(row.original)}
                  actions={this.getTaskActions(id, datasource, status, type, true)}
                />
              );
            },
            Aggregated: () => '',
          },
        ]}
      />
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
            onClick={() => goToQuery({ queryString: TasksView.TASK_SQL })}
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

  render() {
    const { onFiltersChange } = this.props;
    const {
      groupTasksBy,
      taskSpecDialogOpen,
      executionDialogOpen,
      alertErrorMsg,
      taskTableActionDialogOpen,
      visibleColumns,
    } = this.state;

    return (
      <div className="tasks-view app-view">
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
                visibleColumns: prevState.visibleColumns.toggle(column),
              }))
            }
            tableColumnsHidden={visibleColumns.getHiddenColumns()}
          />
        </ViewControlBar>
        {this.renderTaskTable()}

        {this.renderKillTaskAction()}
        {taskSpecDialogOpen && (
          <SpecDialog
            onClose={this.closeSpecDialogs}
            onSubmit={this.submitTask}
            title="Submit task"
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
        {taskTableActionDialogOpen && (
          <TaskTableActionDialog
            taskId={taskTableActionDialogOpen.id}
            status={taskTableActionDialogOpen.status}
            actions={taskTableActionDialogOpen.actions}
            onClose={() => this.setState({ taskTableActionDialogOpen: undefined })}
          />
        )}
        {executionDialogOpen && (
          <ExecutionDetailsDialog
            id={executionDialogOpen}
            goToTask={taskId => {
              onFiltersChange([{ id: 'task_id', value: `=${taskId}` }]);
              this.setState({ executionDialogOpen: undefined });
            }}
            onClose={() => this.setState({ executionDialogOpen: undefined })}
          />
        )}
      </div>
    );
  }
}
