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

import { Button, Icon, Intent, Menu, MenuDivider, MenuItem, Popover } from '@blueprintjs/core';
import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import { T } from 'druid-query-toolkit';
import React, { useState } from 'react';
import { useStore } from 'zustand';

import { Loader } from '../../../components';
import type { TaskStatusWithCanceled } from '../../../druid-models';
import { Execution, TASK_CANCELED_PREDICATE, WorkbenchQuery } from '../../../druid-models';
import { cancelTaskExecution, getTaskExecution } from '../../../helpers';
import { useClock, useInterval, useQueryManager } from '../../../hooks';
import { AppToaster } from '../../../singletons';
import {
  downloadQueryDetailArchive,
  formatDuration,
  prettyFormatIsoDate,
  queryDruidSql,
} from '../../../utils';
import { CancelQueryDialog } from '../cancel-query-dialog/cancel-query-dialog';
import { getMsqTaskVersion, WORK_STATE_STORE } from '../work-state-store';

import './recent-query-task-panel.scss';

function statusToIconAndColor(status: TaskStatusWithCanceled): [IconName, string] {
  switch (status) {
    case 'RUNNING':
      return [IconNames.REFRESH, '#2167d5'];
    case 'WAITING':
    case 'PENDING':
      return [IconNames.CIRCLE, '#d5631a'];
    case 'SUCCESS':
      return [IconNames.TICK_CIRCLE, '#57d500'];
    case 'FAILED':
      return [IconNames.DELETE, '#9f0d0a'];
    case 'CANCELED':
      return [IconNames.DISABLE, '#8d8d8d'];
    default:
      return [IconNames.CIRCLE, '#8d8d8d'];
  }
}

interface RecentQueryEntry {
  taskStatus: TaskStatusWithCanceled;
  taskId: string;
  datasource: string;
  createdTime: string;
  duration: number;
  errorMessage?: string;
}

export interface RecentQueryTaskPanelProps {
  onClose(): void;
  onExecutionDetails(id: string): void;
  onChangeQuery(queryString: string): void;
  onNewTab(query: WorkbenchQuery, tabName: string): void;
}

export const RecentQueryTaskPanel = React.memo(function RecentQueryTaskPanel(
  props: RecentQueryTaskPanelProps,
) {
  const { onClose, onExecutionDetails, onChangeQuery, onNewTab } = props;

  const [confirmCancelId, setConfirmCancelId] = useState<string | undefined>();

  const [queryTaskHistoryState, queryManager] = useQueryManager<number, RecentQueryEntry[]>({
    query: useStore(WORK_STATE_STORE, getMsqTaskVersion),
    processQuery: async (_, cancelToken) => {
      return await queryDruidSql<RecentQueryEntry>(
        {
          query: `SELECT
  CASE WHEN ${TASK_CANCELED_PREDICATE} THEN 'CANCELED' ELSE "status" END AS "taskStatus",
  "task_id" AS "taskId",
  "datasource",
  "created_time" AS "createdTime",
  "duration",
  "error_msg" AS "errorMessage"
FROM sys.tasks
WHERE "type" = 'query_controller'
ORDER BY "created_time" DESC
LIMIT 100`,
        },
        cancelToken,
      );
    },
  });

  useInterval(() => {
    queryManager.rerunLastQuery(true);
  }, 30000);

  const now = useClock();

  const queryTaskHistory = queryTaskHistoryState.getSomeData();
  return (
    <div className="recent-query-task-panel">
      <div className="title">
        Recent query tasks
        <Button className="close-button" icon={IconNames.CROSS} minimal onClick={onClose} />
      </div>
      {queryTaskHistory ? (
        <div className="work-entries">
          {queryTaskHistory.map(w => {
            const menu = (
              <Menu>
                <MenuItem
                  icon={IconNames.EYE_OPEN}
                  text="Show details"
                  onClick={() => {
                    onExecutionDetails(w.taskId);
                  }}
                />
                <MenuItem
                  icon={IconNames.DOCUMENT_OPEN}
                  text="Attach in new tab"
                  // eslint-disable-next-line @typescript-eslint/no-misused-promises
                  onClick={async () => {
                    let execution: Execution;
                    try {
                      execution = await getTaskExecution(w.taskId);
                    } catch {
                      AppToaster.show({
                        message: 'Could not get task report or payload',
                        intent: Intent.DANGER,
                      });
                      return;
                    }

                    if (!execution.sqlQuery || !execution.queryContext) {
                      AppToaster.show({
                        message: 'Could not get query',
                        intent: Intent.DANGER,
                      });
                      return;
                    }

                    onNewTab(
                      WorkbenchQuery.fromTaskQueryAndContext(
                        execution.sqlQuery,
                        execution.queryContext,
                      ).changeLastExecution({ engine: 'sql-msq-task', id: w.taskId }),
                      'Attached',
                    );
                  }}
                />
                <MenuItem
                  icon={IconNames.DUPLICATE}
                  text="Copy ID"
                  onClick={() => {
                    copy(w.taskId, { format: 'text/plain' });
                    AppToaster.show({
                      message: `${w.taskId} copied to clipboard`,
                      intent: Intent.SUCCESS,
                    });
                  }}
                />
                {w.taskStatus === 'SUCCESS' &&
                  w.datasource !== Execution.INLINE_DATASOURCE_MARKER && (
                    <MenuItem
                      icon={IconNames.APPLICATION}
                      text={`SELECT * FROM ${T(w.datasource)}`}
                      onClick={() => onChangeQuery(`SELECT * FROM ${T(w.datasource)}`)}
                    />
                  )}
                <MenuItem
                  icon={IconNames.ARCHIVE}
                  text="Get query detail archive"
                  onClick={() => void downloadQueryDetailArchive(w.taskId)}
                />
                {w.taskStatus === 'RUNNING' && (
                  <>
                    <MenuDivider />
                    <MenuItem
                      icon={IconNames.CROSS}
                      text="Cancel query"
                      intent={Intent.DANGER}
                      onClick={() => setConfirmCancelId(w.taskId)}
                    />
                  </>
                )}
              </Menu>
            );

            const duration =
              w.taskStatus === 'RUNNING'
                ? now.valueOf() - new Date(w.createdTime).valueOf()
                : w.duration;

            const [icon, color] = statusToIconAndColor(w.taskStatus);
            return (
              <Popover className="work-entry" key={w.taskId} position="left" content={menu}>
                <div
                  data-tooltip={
                    `ID: ${w.taskId}` + (w.errorMessage ? `\n\nError:\n${w.errorMessage}` : '')
                  }
                  onDoubleClick={() => onExecutionDetails(w.taskId)}
                >
                  <div className="line1">
                    <Icon
                      className={'status-icon ' + w.taskStatus.toLowerCase()}
                      icon={icon}
                      style={{ color }}
                      data-tooltip={`Task status: ${w.taskStatus}`}
                    />
                    <div className="timing">
                      {prettyFormatIsoDate(w.createdTime) +
                        (duration > 0 ? ` (${formatDuration(duration)})` : '')}
                    </div>
                  </div>
                  <div className="line2">
                    <Icon
                      className="output-icon"
                      icon={
                        w.datasource === Execution.INLINE_DATASOURCE_MARKER
                          ? IconNames.APPLICATION
                          : IconNames.CLOUD_UPLOAD
                      }
                    />
                    <div
                      className={classNames('output-datasource', {
                        query: w.datasource === Execution.INLINE_DATASOURCE_MARKER,
                      })}
                    >
                      {w.datasource === Execution.INLINE_DATASOURCE_MARKER
                        ? 'select query'
                        : w.datasource}
                    </div>
                  </div>
                </div>
              </Popover>
            );
          })}
        </div>
      ) : queryTaskHistoryState.isLoading() ? (
        <Loader />
      ) : undefined}
      {confirmCancelId && (
        <CancelQueryDialog
          // eslint-disable-next-line @typescript-eslint/no-misused-promises
          onCancel={async () => {
            if (!confirmCancelId) return;
            try {
              await cancelTaskExecution(confirmCancelId);
              AppToaster.show({
                message: 'Query canceled',
                intent: Intent.SUCCESS,
              });
              queryManager.rerunLastQuery();
            } catch {
              AppToaster.show({
                message: 'Could not cancel query',
                intent: Intent.DANGER,
              });
            }
          }}
          onDismiss={() => setConfirmCancelId(undefined)}
        />
      )}
    </div>
  );
});
