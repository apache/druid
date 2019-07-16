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

import { IDialogProps } from '@blueprintjs/core';
import React from 'react';

import { ShowJson } from '../../components';
import { BasicAction, basicActionsToButtons } from '../../utils/basic-action';
import { deepGet } from '../../utils/object-change';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

interface SupervisorTableActionDialogProps extends IDialogProps {
  supervisorId: string;
  actions: BasicAction[];
  onClose: () => void;
}

interface SupervisorTableActionDialogState {
  activeTab: 'status' | 'payload' | 'stats' | 'history';
}

export class SupervisorTableActionDialog extends React.PureComponent<
  SupervisorTableActionDialogProps,
  SupervisorTableActionDialogState
> {
  constructor(props: SupervisorTableActionDialogProps) {
    super(props);
    this.state = {
      activeTab: 'status',
    };
  }
  render(): React.ReactNode {
    const { supervisorId, actions, onClose } = this.props;
    const { activeTab } = this.state;

    const supervisorTableSideButtonMetadata: SideButtonMetaData[] = [
      {
        icon: 'dashboard',
        text: 'Status',
        active: activeTab === 'status',
        onClick: () => this.setState({ activeTab: 'status' }),
      },
      {
        icon: 'align-left',
        text: 'Payload',
        active: activeTab === 'payload',
        onClick: () => this.setState({ activeTab: 'payload' }),
      },
      {
        icon: 'chart',
        text: 'Statistics',
        active: activeTab === 'stats',
        onClick: () => this.setState({ activeTab: 'stats' }),
      },
      {
        icon: 'history',
        text: 'History',
        active: activeTab === 'history',
        onClick: () => this.setState({ activeTab: 'history' }),
      },
    ];

    return (
      <TableActionDialog
        isOpen
        sideButtonMetadata={supervisorTableSideButtonMetadata}
        onClose={onClose}
        title={`Supervisor: ${supervisorId}`}
        bottomButtons={basicActionsToButtons(actions)}
      >
        {activeTab === 'status' && (
          <ShowJson
            endpoint={`/druid/indexer/v1/supervisor/${supervisorId}/status`}
            transform={x => deepGet(x, 'payload')}
            downloadFilename={`supervisor-status-${supervisorId}.json`}
          />
        )}
        {activeTab === 'payload' && (
          <ShowJson
            endpoint={`/druid/indexer/v1/supervisor/${supervisorId}`}
            downloadFilename={`supervisor-payload-${supervisorId}.json`}
          />
        )}
        {activeTab === 'stats' && (
          <ShowJson
            endpoint={`/druid/indexer/v1/supervisor/${supervisorId}/stats`}
            downloadFilename={`supervisor-stats-${supervisorId}.json`}
          />
        )}
        {activeTab === 'history' && (
          <ShowJson
            endpoint={`/druid/indexer/v1/supervisor/${supervisorId}/history`}
            downloadFilename={`supervisor-history-${supervisorId}.json`}
          />
        )}
      </TableActionDialog>
    );
  }
}
