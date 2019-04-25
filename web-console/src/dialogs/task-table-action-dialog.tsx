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

import {Button, ButtonGroup, IDialogProps, Intent, TextArea} from '@blueprintjs/core';
import axios from 'axios';
import * as React from 'react';
import * as CopyToClipboard from 'react-copy-to-clipboard';

import { AppToaster } from '../singletons/toaster';
import { downloadFile } from '../utils';

import { SideButtonMetaData, TableActionDialog } from './table-action-dialog';

interface TaskTableActionDialogProps extends IDialogProps {
  onClose: () => void;
  metaData: TaskTableActionDialogMetaData;
  killTask: (id: string) => void;
}

interface TaskTableActionDialogState {
  jsonValue: string;
  activeTab: 'payload' | 'status' | 'reports' | 'log?offset=-8192' | 'log';
}

export interface TaskTableActionDialogMetaData {
  id: string | null;
  status: 'RUNNING' | 'WAITING' | 'PENDING' | 'SUCCESS' | 'FAILED';
}

export class TaskTableActionDialog extends React.Component<TaskTableActionDialogProps, TaskTableActionDialogState> {
  constructor(props: TaskTableActionDialogProps) {
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
      const resp = await axios.get(`/druid/indexer/v1/task/${id}/${endpoint}`);
      const data = resp.data;
      this.setState({
        jsonValue: typeof (data) === 'string' ? data : JSON.stringify(data, undefined, 2)
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

  private onClickSideButton = (tab: 'payload' | 'status' | 'reports' | 'log?offset=-8192' | 'log') => {
    this.getJsonInfo(tab);
    this.setState({
      activeTab: tab
    });
  }

  render(): React.ReactNode {
    const { onClose, killTask } = this.props;
    const { activeTab, jsonValue } = this.state;
    const { id, status } = this.props.metaData;

    const taskTableSideButtonMetadata: SideButtonMetaData[] = [
      {icon: 'align-left', text: 'Payload', active: activeTab === 'payload',
        onClick: () => this.onClickSideButton('payload')},
      {icon: 'dashboard', text: 'Status', active: activeTab === 'status',
        onClick: () => this.onClickSideButton('status')},
      {icon: 'comparison', text: 'Reports', active: activeTab === 'reports',
        onClick: () => this.onClickSideButton('reports')},
      {icon: 'align-justify', text: 'Logs', active: activeTab === 'log' || activeTab === 'log?offset=-8192',
        onClick: () => this.onClickSideButton('log?offset=-8192')}
    ];

    return <TableActionDialog
      isOpen
      sideButtonMetadata={taskTableSideButtonMetadata}
      onClose={onClose}
    >
      <div className={'top-actions'}>
        {
          (activeTab === 'log?offset=-8192' || activeTab === 'log' )
            ? <ButtonGroup>
              <Button
                className={'view-all-button'}
                text={activeTab === 'log' ? 'View last 8kb' : 'View all'}
                minimal
                onClick={() => {
                  const alternate =  activeTab === 'log' ? 'log?offset=-8192' : 'log';
                  this.setState({
                    activeTab: alternate
                  });
                  this.getJsonInfo(alternate);
                }}
              />
            </ButtonGroup>
            : <div/>
        }
        <ButtonGroup className={'right-buttons'}>
          <Button
            text={'Save'}
            onClick={() => downloadFile(jsonValue, 'json', `task-${id}-${activeTab}.json`)}
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
            onClick={() => window.open(`/druid/indexer/v1/task/${id}/${activeTab === 'payload' ? '' : activeTab}`, '_blank')}
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
        {
          (status === 'RUNNING' || status === 'WAITING' || status === 'PENDING')
          ?
            <Button
              text={'Kill'}
              intent={Intent.DANGER}
              onClick={() => id && killTask(id)}
            />
          : <div/>
        }
        <Button
          text={'Close'}
          intent={Intent.PRIMARY}
          onClick={onClose}
        />
      </div>
    </TableActionDialog>;
  }
}
