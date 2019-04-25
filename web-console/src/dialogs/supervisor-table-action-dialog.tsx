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

import { Button, ButtonGroup, Intent, TextArea } from '@blueprintjs/core';
import axios from 'axios';
import * as React from 'react';
import * as CopyToClipboard from 'react-copy-to-clipboard';

import { AppToaster } from '../singletons/toaster';
import { downloadFile } from '../utils';

import { SideButtonMetaData, TableActionDialog } from './table-action-dialog';

interface SupervisorTableActionDialogProps extends React.Props<any> {
  onClose: () => void;
  metaData: SupervisorTableActionDialogMetaData;
  terminateSupervisor: (id: string) => void;
  resetSupervisor: (id: string) => void;
  resumeSupervisor: (id: string) => void;
  suspendSupervisor: (id: string) => void;
}

interface SupervisorTableActionDialogState {
  jsonValue: string;
  activeTab: 'payload' | 'status' | 'stats' | 'history';
}

export interface SupervisorTableActionDialogMetaData {
  id: string | null;
  supervisorSuspended: boolean;
}

export class SupervisorTableActionDialog extends React.Component<SupervisorTableActionDialogProps, SupervisorTableActionDialogState> {
  constructor(props: SupervisorTableActionDialogProps) {
    super(props);
    this.state = {
      jsonValue: '',
      activeTab: 'payload'
    };
    this.getJsonInfo('payload');
  }

  private getJsonInfo = async (endpoint: string): Promise<void> => {
    const { id } = this.props.metaData;
    if (endpoint === 'payload') endpoint = '';
    try {
      const resp = await axios.get(`/druid/indexer/v1/supervisor/${id}/${endpoint}`);
      const data = resp.data;
      this.setState({
        jsonValue: JSON.stringify(data, undefined, 2)
      });
    } catch (e) {
      AppToaster.show({
        message: e.response.data,
        intent: Intent.WARNING
      });
      this.setState({
        jsonValue: ''
      });
    }
  }

  private onClickSideButton = (tab: 'payload' | 'status' | 'stats' | 'history') => {
    this.getJsonInfo(tab);
    this.setState({
      activeTab: tab
    });
  }

  render(): React.ReactNode {
    const { onClose, terminateSupervisor, resetSupervisor, resumeSupervisor, suspendSupervisor } = this.props;
    const { activeTab, jsonValue } = this.state;
    const { id, supervisorSuspended } = this.props.metaData;

    const supervisorTableSideButtonMetadata: SideButtonMetaData[] = [
      {icon: 'align-left', text: 'Payload', active: activeTab === 'payload',
        onClick: () => this.onClickSideButton('payload')},
      {icon: 'dashboard', text: 'Status', active: activeTab === 'status',
        onClick: () => this.onClickSideButton('status')},
      {icon: 'chart', text: 'Statistics', active: activeTab === 'stats',
        onClick: () => this.onClickSideButton('stats')},
      {icon: 'history', text: 'History', active: activeTab === 'history',
        onClick: () => this.onClickSideButton('history')}
    ];

    return <TableActionDialog
      isOpen
      sideButtonMetadata={supervisorTableSideButtonMetadata}
    >
      <div className={'top-actions'}>
        <div/>
        <ButtonGroup className={'right-buttons'}>
          <Button
            text={'Save'}
            onClick={() => downloadFile(jsonValue, 'json', `supervisor-${id}-${activeTab}.json`)}
            minimal
          />
          <CopyToClipboard
            text={jsonValue}
          >
            <Button
              text={'Copy'}
              onClick={() => {AppToaster.show({message: 'Copied to clipboard', intent: Intent.SUCCESS}); }}
              minimal
            />
          </CopyToClipboard>
          <Button
            text={'View raw data'}
            minimal
            onClick={() => window.open(`/druid/indexer/v1/supervisor/${id}/${activeTab === 'payload' ? '' : activeTab}`, '_blank')}
          />
        </ButtonGroup>
      </div>

      <div className={'textarea'}>
        <TextArea
          readOnly
          value={jsonValue}
        />
      </div>

      <div className={'bottom-buttons'}>
        <div>
          <Button
            key="terminate"
            text={'Terminate'}
            intent={Intent.DANGER}
            onClick={() => id ? terminateSupervisor(id) : null}
          />
          <Button
            key="resumeandsuspend"
            text={supervisorSuspended ? 'Resume' : 'Suspend'}
            onClick={() => id ? (supervisorSuspended ? resumeSupervisor(id) : suspendSupervisor(id)) : null}
          />
          <Button
            key="reset"
            text={'Reset'}
            onClick={() => id ? resetSupervisor(id) : null}
          />
        </div>
        <Button
          text={'Done'}
          intent={Intent.PRIMARY}
          onClick={onClose}
        />
      </div>

    </TableActionDialog>;
  }
}
