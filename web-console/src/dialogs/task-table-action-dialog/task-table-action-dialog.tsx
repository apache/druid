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

import React, { useState } from 'react';

import { ShowJson, ShowLog } from '../../components';
import { BasicAction } from '../../utils/basic-action';
import { deepGet } from '../../utils/object-change';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

interface TaskTableActionDialogProps {
  taskId: string;
  actions: BasicAction[];
  onClose: () => void;
  status?: string;
}

export const TaskTableActionDialog = React.memo(function TaskTableActionDialog(
  props: TaskTableActionDialogProps,
) {
  const { taskId, actions, onClose, status } = props;
  const [activeTab, setActiveTab] = useState('status');

  const taskTableSideButtonMetadata: SideButtonMetaData[] = [
    {
      icon: 'dashboard',
      text: 'Status',
      active: activeTab === 'status',
      onClick: () => setActiveTab('status'),
    },
    {
      icon: 'align-left',
      text: 'Payload',
      active: activeTab === 'payload',
      onClick: () => setActiveTab('payload'),
    },
    {
      icon: 'comparison',
      text: 'Reports',
      active: activeTab === 'reports',
      onClick: () => setActiveTab('reports'),
    },
    {
      icon: 'align-justify',
      text: 'Logs',
      active: activeTab === 'log',
      onClick: () => setActiveTab('log'),
    },
  ];

  return (
    <TableActionDialog
      sideButtonMetadata={taskTableSideButtonMetadata}
      onClose={onClose}
      title={`Task: ${taskId}`}
      actions={actions}
    >
      {activeTab === 'status' && (
        <ShowJson
          endpoint={`/druid/indexer/v1/task/${taskId}/status`}
          transform={x => deepGet(x, 'status')}
          downloadFilename={`task-status-${taskId}.json`}
        />
      )}
      {activeTab === 'payload' && (
        <ShowJson
          endpoint={`/druid/indexer/v1/task/${taskId}`}
          transform={x => deepGet(x, 'payload')}
          downloadFilename={`task-payload-${taskId}.json`}
        />
      )}
      {activeTab === 'reports' && (
        <ShowJson
          endpoint={`/druid/indexer/v1/task/${taskId}/reports`}
          transform={x => deepGet(x, 'ingestionStatsAndErrors.payload')}
          downloadFilename={`task-reports-${taskId}.json`}
        />
      )}
      {activeTab === 'log' && (
        <ShowLog
          status={status}
          endpoint={`/druid/indexer/v1/task/${taskId}/log`}
          downloadFilename={`task-log-${taskId}.log`}
          tailOffset={16000}
        />
      )}
    </TableActionDialog>
  );
});
