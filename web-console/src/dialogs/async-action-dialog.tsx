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

import classNames from 'classnames';
import * as React from 'react';
import {
  FormGroup,
  Button,
  InputGroup,
  Dialog,
  NumericInput,
  Classes,
  Tooltip,
  AnchorButton,
  TagInput,
  Intent,
  ButtonGroup,
  ProgressBar,
  MaybeElement,
  Icon,
  IconName
} from "@blueprintjs/core";
import { AppToaster } from '../singletons/toaster';

export interface AsyncAlertDialogProps extends React.Props<any> {
  action: null | (() => Promise<void>),
  onClose: (success: boolean) => void,
  confirmButtonText: string;
  cancelButtonText?: string;
  className?: string,
  icon?: IconName | MaybeElement;
  intent?: Intent;
  successText: string;
  failText: string;
}

export interface AsyncAlertDialogState {
  working: boolean
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
      await action()
    } catch (e) {
      AppToaster.show({
        message: `${failText}: ${e.message}`,
        intent: Intent.DANGER
      });
      onClose(false);
      this.setState({ working: false });
      return;
    }
    AppToaster.show({
      message: successText,
      intent: Intent.SUCCESS
    });
    onClose(true);
    this.setState({ working: false });
  }

  render() {
    const { action, onClose, className, icon, intent, confirmButtonText, cancelButtonText, children } = this.props;
    const { working } = this.state;

    const handleClose = () => onClose(false);

    return <Dialog
      isOpen={Boolean(action)}
      className={classNames(Classes.ALERT, 'async-alert-dialog', className)}
      canEscapeKeyClose={!working}
      onClose={handleClose}
    >
      <div className={Classes.ALERT_BODY}>
        { icon && <Icon icon={icon} iconSize={40} intent={intent} /> }
        { !working && <div className={Classes.ALERT_CONTENTS}>{children}</div> }
      </div>
      {
        working ?
          <ProgressBar/> :
          <div className={Classes.ALERT_FOOTER}>
            <Button intent={intent} text={confirmButtonText} onClick={this.handleConfirm}/>
            <Button text={cancelButtonText || 'Cancel'} onClick={handleClose}/>
          </div>
      }
    </Dialog>
  }
}
