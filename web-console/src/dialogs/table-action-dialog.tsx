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

import {Button, ButtonGroup, Dialog, Divider, Icon, Intent, TextArea} from '@blueprintjs/core';
import axios from 'axios';
import * as FileSaver from 'file-saver';
import * as React from 'react';
import * as CopyToClipboard from 'react-copy-to-clipboard';

import {AppToaster} from '../singletons/toaster';
import { getDruidErrorMessage } from '../utils';

import './table-action-dialog.scss';

interface TableActionDialogProps extends React.Props<any> {
  onClose: () => void;
  metaData: TableActionDialogMetaData;
  terminateSupervisor: (id: string) => void;
  resetSupervisor: (id: string) => void;
  resumeSupervisor: (id: string) => void;
  suspendSupervisor: (id: string) => void;
  killTask: (id: string) => void;
}

interface TableActionDialogState {
  jsonValue: string;
  activeEndpoint: '' | 'status' | 'stats' | 'history' | 'reports' | 'log?offset=-8192' | 'log';
}

export interface TableActionDialogMetaData {
  id: string | null;
  mode: 'supervisor' | 'task' | null;
  status: 'RUNNING' | 'WAITING' | 'PENDING' | 'SUCCESS' | 'FAILED' | null;
  supervisorSuspended: boolean | null;
}

export class TableActionDialog extends React.Component<TableActionDialogProps, TableActionDialogState> {
  constructor(props: TableActionDialogProps) {
    super(props);
    this.state = {
      jsonValue: '',
      activeEndpoint: ''
    };
  }

  private getJsonInfo = async (endpoint: string) => {
    const { mode, id } = this.props.metaData;
    try {
      const resp = await axios.get(`/druid/indexer/v1/${mode}/${id}/${endpoint}`);
      const data = resp.data;
      this.setState({
        jsonValue: JSON.stringify(data, undefined, 2)
      });
    } catch (e) {
      AppToaster.show({
        message: getDruidErrorMessage(e),
        intent: Intent.WARNING
      });
    }
  }

  private downloadJson = () => {
    const { id, mode } = this.props.metaData;
    const { jsonValue, activeEndpoint } = this.state;

    const blob = new Blob([jsonValue], {
      type: 'text/json'
    });

    FileSaver.saveAs(blob, `${mode}-${id}-${activeEndpoint === '' ? 'payload' : activeEndpoint}.json`);
  }

  render() {
    const { onClose, terminateSupervisor, resetSupervisor, resumeSupervisor, suspendSupervisor } = this.props;
    const { jsonValue, activeEndpoint } = this.state;
    const { id, mode, status, supervisorSuspended } = this.props.metaData;

    const supervisorTableInfoButtons = [
      {icon: 'align-left', text: 'Payload', endpoint: ``},
      {icon: 'dashboard', text: 'Status', endpoint: `status`},
      {icon: 'chart', text: 'Statistics', endpoint: `stats`},
      {icon: 'history', text: 'History', endpoint: `history`}
    ];

    const taskTableInfoButtons = [
      {icon: 'align-left', text: 'Payload', endpoint: ``},
      {icon: 'dashboard', text: 'Status', endpoint: `status`},
      {icon: 'comparison', text: 'Reports', endpoint: `reports`},
      {icon: 'align-justify', text: 'Logs', endpoint: `log?offset=-8192`}
    ];

    const tableInfoButtons = mode === 'supervisor' ? supervisorTableInfoButtons : taskTableInfoButtons;

    const tableActionButtions = mode === 'supervisor'
      ? [
          <Button
            key="terminate"
            text={'Terminate'}
            intent={Intent.DANGER}
            onClick={() => id ? terminateSupervisor(id) : null}
          />,
          <Button
            key="resumesuspend"
            text={supervisorSuspended ? 'Resume' : 'Suspend'}
            onClick={() => id ? (supervisorSuspended ? resumeSupervisor(id) : suspendSupervisor(id)) : null}
          />,
          <Button
            key="reset"
            text={'Reset'}
            onClick={() => id ? resetSupervisor(id) : null}
          />
        ]
      : <Button
        text={'Kill'}
        intent={Intent.DANGER}
        onClick={() => id && (status === 'RUNNING' || status === 'WAITING' || status === 'PENDING') ? resetSupervisor(id) : null}
      />;

    return <Dialog
      className={'table-action-dialog'}
      isOpen
      onClose={onClose}
      onOpening={() => this.getJsonInfo(activeEndpoint)}
    >
      <div className={'side-bar'}>
        {
          tableInfoButtons.map((d: any) => {
            return <Button
              className={`info-button`}
              icon={<Icon icon={d.icon} iconSize={20}/>}
              key={d.text}
              text={d.text}
              intent={activeEndpoint === d.endpoint ? Intent.PRIMARY : Intent.NONE}
              minimal={activeEndpoint !== d.endpoint}
              onClick={() => {
                this.getJsonInfo(d.endpoint);
                this.setState({activeEndpoint: d.endpoint});
              }}
            />;
          })
        }
      </div>

      <Divider/>

      <div className={'main-section'}>

        <div className={'top-actions'}>
          {
            mode === 'task' && (activeEndpoint === 'log?offset=-8192' || activeEndpoint === 'log' )
            ? <ButtonGroup>
              <Button
                className={'view-all-button'}
                text={activeEndpoint === 'log' ? 'View last 8kb' : 'View all'}
                minimal
                onClick={() => {
                  const alternate =  activeEndpoint === 'log' ? 'log?offset=-8192' : 'log';
                  this.setState({
                    activeEndpoint: alternate
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
              onClick={this.downloadJson}
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
              onClick={() => window.open(`/druid/indexer/v1/${mode}/${id}/${activeEndpoint}`, '_blank')}
            />
          </ButtonGroup>
        </div>

        <TextArea
          readOnly
          value={jsonValue}
        />

        <div className={'bottom-buttons'}>
          <div>
            {tableActionButtions}
          </div>
          <div>
            <Button
              text={'Done'}
              intent={Intent.PRIMARY}
              onClick={onClose}
            />
          </div>
        </div>

      </div>
    </Dialog>;
  }
}
