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

import { Code, Intent, MenuItem } from '@blueprintjs/core';
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
import {
  AlertDialog,
  AsyncActionDialog,
  SpecDialog,
  SupervisorTableActionDialog,
} from '../../dialogs';
import { SupervisorResetOffsetsDialog } from '../../dialogs/supervisor-reset-offsets-dialog/supervisor-reset-offsets-dialog';
import type { QueryWithContext } from '../../druid-models';
import type { Capabilities } from '../../helpers';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import {
  deepGet,
  getDruidErrorMessage,
  groupByAsMap,
  hasPopoverOpen,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  lookupBy,
  oneOf,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
  QueryState,
  twoLines,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';

import './supervisors-view.scss';

const supervisorTableColumns: string[] = [
  'Supervisor ID',
  'Type',
  'Topic/Stream',
  'Status',
  'Running tasks',
  ACTION_COLUMN_LABEL,
];

interface SupervisorQuery {
  capabilities: Capabilities;
  visibleColumns: LocalStorageBackedVisibility;
}

interface SupervisorQueryResultRow {
  supervisor_id: string;
  type: string;
  source: string;
  detailed_state: string;
  suspended: boolean;
  running_tasks?: number;
}

interface RunningTaskRow {
  datasource: string;
  type: string;
  num_running_tasks: number;
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

  static SUPERVISOR_SQL = `SELECT
  "supervisor_id",
  "type",
  "source",
  CASE WHEN "suspended" = 0 THEN "detailed_state" ELSE 'SUSPENDED' END AS "detailed_state",
  "suspended" = 1 AS "suspended"
FROM "sys"."supervisors"
ORDER BY "supervisor_id"`;

  static RUNNING_TASK_SQL = `SELECT
  "datasource", "type", COUNT(*) AS "num_running_tasks"
FROM "sys"."tasks" WHERE "status" = 'RUNNING' AND "runner_status" = 'RUNNING'
GROUP BY 1, 2`;

  constructor(props: SupervisorsViewProps) {
    super(props);

    this.state = {
      supervisorsState: QueryState.INIT,

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
      processQuery: async ({ capabilities, visibleColumns }) => {
        let supervisors: SupervisorQueryResultRow[];
        if (capabilities.hasSql()) {
          supervisors = await queryDruidSql<SupervisorQueryResultRow>({
            query: SupervisorsView.SUPERVISOR_SQL,
          });
        } else if (capabilities.hasOverlordAccess()) {
          const supervisorList = (await Api.instance.get('/druid/indexer/v1/supervisor?full')).data;
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
              suspended: Boolean(deepGet(sup, 'suspended')),
            };
          });
        } else {
          throw new Error(`must have SQL or overlord access`);
        }

        if (visibleColumns.shown('Running tasks')) {
          try {
            let runningTaskLookup: Record<string, number>;
            if (capabilities.hasSql()) {
              const runningTasks = await queryDruidSql<RunningTaskRow>({
                query: SupervisorsView.RUNNING_TASK_SQL,
              });

              runningTaskLookup = lookupBy(
                runningTasks,
                ({ datasource, type }) => `${datasource}_${type}`,
                ({ num_running_tasks }) => num_running_tasks,
              );
            } else if (capabilities.hasOverlordAccess()) {
              const taskList = (await Api.instance.get(`/druid/indexer/v1/tasks?state=running`))
                .data;
              runningTaskLookup = groupByAsMap(
                taskList,
                (t: any) => `${t.dataSource}_${t.type}`,
                xs => xs.length,
              );
            } else {
              throw new Error(`must have SQL or overlord access`);
            }

            supervisors.forEach(supervisor => {
              supervisor.running_tasks =
                runningTaskLookup[`${supervisor.supervisor_id}_index_${supervisor.type}`] || 0;
            });
          } catch (e) {
            AppToaster.show({
              icon: IconNames.ERROR,
              intent: Intent.DANGER,
              message: 'Could not get running task counts',
            });
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

  componentDidMount(): void {
    const { capabilities } = this.props;
    const { visibleColumns } = this.state;

    this.supervisorQueryManager.runQuery({ capabilities, visibleColumns: visibleColumns });
  }

  componentWillUnmount(): void {
    this.supervisorQueryManager.terminate();
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
        title: 'Set offsets',
        onAction: () => this.setState({ resetOffsetsSupervisorInfo: { id, type } }),
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
        <p>
          Are you sure you want to resume supervisor <Code>{resumeSupervisorId}</Code>?
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
          Are you sure you want to suspend supervisor <Code>{suspendSupervisorId}</Code>?
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
          `I understand that resetting ${resetSupervisorId} will clear checkpoints and therefore lead to data loss or duplication.`,
          'I understand that this operation cannot be undone.',
        ]}
      >
        <p>
          Are you sure you want to hard reset supervisor <Code>{resetSupervisorId}</Code>?
        </p>
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
        <p>
          Are you sure you want to terminate supervisor <Code>{terminateSupervisorId}</Code>?
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
    const { supervisorsState, visibleColumns } = this.state;

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
        defaultPageSize={SMALL_TABLE_PAGE_SIZE}
        pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={supervisors.length > SMALL_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: twoLines('Supervisor ID', <i>(datasource)</i>),
            id: 'supervisor_id',
            accessor: 'supervisor_id',
            width: 300,
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
            width: 100,
            Cell: this.renderSupervisorFilterableCell('type'),
            show: visibleColumns.shown('Type'),
          },
          {
            Header: 'Topic/Stream',
            accessor: 'source',
            width: 300,
            Cell: this.renderSupervisorFilterableCell('source'),
            show: visibleColumns.shown('Topic/Stream'),
          },
          {
            Header: 'Status',
            id: 'status',
            width: 250,
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
            Header: 'Running tasks',
            id: 'running_tasks',
            width: 150,
            accessor: 'running_tasks',
            filterable: false,
            Cell: ({ value, original }) => (
              <TableClickableCell
                onClick={() => goToTasks(original.supervisor_id, `index_${original.type}`)}
                hoverIcon={IconNames.ARROW_TOP_RIGHT}
                title="Go to tasks"
              >
                {typeof value === 'undefined'
                  ? 'n/a'
                  : value > 0
                  ? pluralIfNeeded(value, 'running task')
                  : original.suspended
                  ? ''
                  : `No running tasks`}
              </TableClickableCell>
            ),
            show: visibleColumns.shown('Running tasks'),
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
            show: visibleColumns.shown(ACTION_COLUMN_LABEL),
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
              onClick={() => goToQuery({ queryString: SupervisorsView.SUPERVISOR_SQL })}
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
            columns={supervisorTableColumns}
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
