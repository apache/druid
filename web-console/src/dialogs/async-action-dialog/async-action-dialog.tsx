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

import {
  Button,
  ButtonGroup,
  Classes,
  Dialog,
  FormGroup,
  Icon, Intent, NumericInput, ProgressBar, TagInput
} from '@blueprintjs/core';
import { IconName } from '@blueprintjs/icons';
import classNames from 'classnames';
import * as React from 'react';

import { AppToaster } from '../../singletons/toaster';

export interface AsyncAlertDialogProps extends React.Props<any> {
  action: null | (() => Promise<void>);
  onClose: (success: boolean) => void;
  confirmButtonText: string;
  confirmButtonDisabled?: boolean;
  cancelButtonText?: string;
  className?: string;
  icon?: IconName;
  intent?: Intent;
  successText: string;
  failText: string;
}

export interface AsyncAlertDialogState {
  working: boolean;
}

export class AsyncActionDialog extends React.Component<AsyncAlertDialogProps, AsyncAlertDialogState> {
  constructor(props: AsyncAlertDialogProps) {
    super(props);
    this.state = {
      working: false
    };
  }

  private handleConfirm = async () => {
    const { action, onClose, successText, failText } = this.props;
    if (!action) throw new Error('should never get here');

    this.setState({ working: true });
    try {
      await action();
    } catch (e) {
      AppToaster.show({
        message: `${failText}: ${e.message}`,
        intent: Intent.DANGER
      });
      this.setState({ working: false });
      onClose(false);
      return;
    }
    AppToaster.show({
      message: successText,
      intent: Intent.SUCCESS
    });
    this.setState({ working: false });
    onClose(true);
  }

  render() {
    const { action, onClose, className, icon, intent, confirmButtonText, cancelButtonText, confirmButtonDisabled, children } = this.props;
    const { working } = this.state;
    if (!action) return null;

    const handleClose = () => onClose(false);

    return <Dialog
      isOpen
      className={classNames(Classes.ALERT, 'async-alert-dialog', className)}
      canEscapeKeyClose={!working}
      onClose={handleClose}
    >
      <div className={Classes.ALERT_BODY}>
        {icon && <Icon icon={icon} />}
        {!working && <div className={Classes.ALERT_CONTENTS}>{children}</div>}
      </div>
      {
        working ?
          <ProgressBar/> :
          <div className={Classes.ALERT_FOOTER}>
            <Button intent={intent} text={confirmButtonText} onClick={this.handleConfirm} disabled={confirmButtonDisabled}/>
            <Button text={cancelButtonText || 'Cancel'} onClick={handleClose}/>
          </div>
      }
    </Dialog>;
  }
}
