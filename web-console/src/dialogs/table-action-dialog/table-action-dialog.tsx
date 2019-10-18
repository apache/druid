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

import { Button, Classes, Dialog, Icon, IconName, Intent, Popover } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { ReactNode } from 'react';

import { BasicAction, basicActionsToMenu } from '../../utils/basic-action';

import './table-action-dialog.scss';

export interface SideButtonMetaData {
  icon: IconName;
  text: string;
  active?: boolean;
  onClick?: () => void;
}

interface TableActionDialogProps {
  title: string;
  sideButtonMetadata: SideButtonMetaData[];
  onClose: () => void;
  actions?: BasicAction[];
  children?: ReactNode;
}

export function TableActionDialog(props: TableActionDialogProps) {
  const { sideButtonMetadata, onClose, title, actions, children } = props;
  const actionsMenu = actions ? basicActionsToMenu(actions) : undefined;

  return (
    <Dialog className="table-action-dialog" isOpen onClose={onClose} title={title}>
      <div className={Classes.DIALOG_BODY}>
        <div className="side-bar">
          {sideButtonMetadata.map((d, i) => (
            <Button
              className="tab-button"
              icon={<Icon icon={d.icon} iconSize={20} />}
              key={i}
              text={d.text}
              intent={d.active ? Intent.PRIMARY : Intent.NONE}
              minimal={!d.active}
              onClick={d.onClick}
            />
          ))}
        </div>
        <div className="main-section">{children}</div>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        {actionsMenu && (
          <div className="footer-actions-left">
            <Popover content={actionsMenu}>
              <Button icon={IconNames.WRENCH} text="Actions" rightIcon={IconNames.CARET_DOWN} />
            </Popover>
          </div>
        )}
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" intent={Intent.PRIMARY} onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
}
