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

import { ShowJson, SupervisorHistoryPanel } from '../../components';
import { cleanSpec } from '../../druid-models';
import { Api } from '../../singletons';
import { deepGet } from '../../utils';
import type { BasicAction } from '../../utils/basic-action';
import type { SideButtonMetaData } from '../table-action-dialog/table-action-dialog';
import { TableActionDialog } from '../table-action-dialog/table-action-dialog';

import { AutoScalerPanel } from './auto-scaler-panel/auto-scaler-panel';
import { SupervisorStatisticsTable } from './supervisor-statistics-table/supervisor-statistics-table';

type SupervisorTableActionDialogTab = 'status' | 'stats' | 'spec' | 'history' | 'auto-scaler';

interface SupervisorTableActionDialogProps {
  supervisorId: string;
  supervisorType?: string;
  actions: BasicAction[];
  onClose: () => void;
}

export const SupervisorTableActionDialog = React.memo(function SupervisorTableActionDialog(
  props: SupervisorTableActionDialogProps,
) {
  const { supervisorId, supervisorType, actions, onClose } = props;
  const [activeTab, setActiveTab] = useState<SupervisorTableActionDialogTab>('status');

  const isKafka = supervisorType === 'kafka';

  const supervisorTableSideButtonMetadata: SideButtonMetaData[] = [
    {
      icon: 'dashboard',
      text: 'Status',
      active: activeTab === 'status',
      onClick: () => setActiveTab('status'),
    },
    {
      icon: 'chart',
      text: 'Task stats',
      active: activeTab === 'stats',
      onClick: () => setActiveTab('stats'),
    },
    {
      icon: 'align-left',
      text: 'Spec',
      active: activeTab === 'spec',
      onClick: () => setActiveTab('spec'),
    },
    {
      icon: 'history',
      text: 'History',
      active: activeTab === 'history',
      onClick: () => setActiveTab('history'),
    },
    ...(isKafka
      ? [
          {
            icon: 'predictive-analysis' as any,
            text: 'Auto-scaler',
            active: activeTab === 'auto-scaler',
            onClick: () => setActiveTab('auto-scaler'),
          },
        ]
      : []),
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
      {activeTab === 'spec' && (
        <ShowJson
          endpoint={supervisorEndpointBase}
          transform={cleanSpec}
          downloadFilename={`supervisor-payload-${supervisorId}.json`}
        />
      )}
      {activeTab === 'history' && <SupervisorHistoryPanel supervisorId={supervisorId} />}
      {activeTab === 'auto-scaler' && isKafka && (
        <AutoScalerPanel supervisorId={supervisorId} />
      )}
    </TableActionDialog>
  );
});
