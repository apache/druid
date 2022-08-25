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

import { ShowJson } from '../../components';
import { ShowHistory } from '../../components/show-history/show-history';
import { cleanSpec } from '../../druid-models';
import { Api } from '../../singletons';
import { deepGet } from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

import { SupervisorStatisticsTable } from './supervisor-statistics-table/supervisor-statistics-table';

interface SupervisorTableActionDialogProps {
  supervisorId: string;
  actions: BasicAction[];
  onClose: () => void;
}

export const SupervisorTableActionDialog = React.memo(function SupervisorTableActionDialog(
  props: SupervisorTableActionDialogProps,
) {
  const { supervisorId, actions, onClose } = props;
  const [activeTab, setActiveTab] = useState('status');

  const supervisorTableSideButtonMetadata: SideButtonMetaData[] = [
    {
      icon: 'dashboard',
      text: 'Status',
      active: activeTab === 'status',
      onClick: () => setActiveTab('status'),
    },
    {
      icon: 'chart',
      text: 'Statistics',
      active: activeTab === 'stats',
      onClick: () => setActiveTab('stats'),
    },
    {
      icon: 'align-left',
      text: 'Payload',
      active: activeTab === 'payload',
      onClick: () => setActiveTab('payload'),
    },
    {
      icon: 'history',
      text: 'History',
      active: activeTab === 'history',
      onClick: () => setActiveTab('history'),
    },
  ];

  const supervisorEndpointBase = `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}`;
  return (
    <TableActionDialog
      sideButtonMetadata={supervisorTableSideButtonMetadata}
      onClose={onClose}
      title={`Supervisor: ${supervisorId}`}
      actions={actions}
    >
      {activeTab === 'status' && (
        <ShowJson
          endpoint={`${supervisorEndpointBase}/status`}
          transform={x => deepGet(x, 'payload')}
          downloadFilename={`supervisor-status-${supervisorId}.json`}
        />
      )}
      {activeTab === 'stats' && (
        <SupervisorStatisticsTable
          supervisorId={supervisorId}
          downloadFilename={`supervisor-stats-${supervisorId}.json`}
        />
      )}
      {activeTab === 'payload' && (
        <ShowJson
          endpoint={supervisorEndpointBase}
          transform={x => cleanSpec(x, true)}
          downloadFilename={`supervisor-payload-${supervisorId}.json`}
        />
      )}
      {activeTab === 'history' && (
        <ShowHistory
          endpoint={`${supervisorEndpointBase}/history`}
          downloadFilenamePrefix={`supervisor-${supervisorId}`}
        />
      )}
    </TableActionDialog>
  );
});
