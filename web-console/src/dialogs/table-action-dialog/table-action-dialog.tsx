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

import { Button, Classes, Dialog, Icon, IconName, IDialogProps, Intent } from '@blueprintjs/core';
import * as React from 'react';

import './table-action-dialog.scss';

export interface SideButtonMetaData {
  icon: IconName;
  text: string;
  active?: boolean;
  onClick?: () => void;
}

interface TableActionDialogProps extends IDialogProps {
  sideButtonMetadata: SideButtonMetaData[];
  onClose: () => void;
  bottomButtons?: React.ReactNode;
}

export class TableActionDialog extends React.Component<TableActionDialogProps, {}> {
  constructor(props: TableActionDialogProps) {
    super(props);
    this.state = {};
  }

  render() {
    const { sideButtonMetadata, isOpen, onClose, title, bottomButtons } = this.props;

    return <Dialog
      className="table-action-dialog"
      isOpen={isOpen}
      onClose={onClose}
      title={title}
    >
      <div className={Classes.DIALOG_BODY}>
        <div className="side-bar">
          {
            sideButtonMetadata.map((d: SideButtonMetaData) => (
              <Button
                className="tab-button"
                icon={<Icon icon={d.icon} iconSize={20}/>}
                key={d.text}
                text={d.text}
                intent={d.active ? Intent.PRIMARY : Intent.NONE}
                minimal={!d.active}
                onClick={d.onClick}
              />
            ))
          }
        </div>
        <div className="main-section">
          {this.props.children}
        </div>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className="footer-actions-left">
          {bottomButtons}
        </div>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button
            text="Close"
            intent={Intent.PRIMARY}
            onClick={onClose}
          />
        </div>
      </div>
    </Dialog>;
  }
}
