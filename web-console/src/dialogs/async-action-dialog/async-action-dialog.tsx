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
  Classes,
  Dialog,
  FormGroup,
  Icon,
  IconName,
  Intent,
  ProgressBar,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import React, { ReactNode, useState } from 'react';

import { AppToaster } from '../../singletons/toaster';

import './async-action-dialog.scss';

export interface AsyncActionDialogProps {
  action: () => Promise<void>;
  onClose: () => void;
  onSuccess?: () => void;
  confirmButtonText: string;
  confirmButtonDisabled?: boolean;
  cancelButtonText?: string;
  className?: string;
  icon?: IconName;
  intent?: Intent;
  successText: string;
  failText: string;
  children?: ReactNode;
}

export function AsyncActionDialog(props: AsyncActionDialogProps) {
  const {
    action,
    onClose,
    onSuccess,
    successText,
    failText,
    className,
    intent,
    icon,
    confirmButtonText,
    confirmButtonDisabled,
    cancelButtonText,
    children,
  } = props;
  const [working, setWorking] = useState(false);

  async function handleConfirm() {
    setWorking(true);
    try {
      await action();
    } catch (e) {
      AppToaster.show({
        message: `${failText}: ${e.message}`,
        intent: Intent.DANGER,
      });
      setWorking(false);
      onClose();

      return;
    }
    AppToaster.show({
      message: successText,
      intent: Intent.SUCCESS,
    });

    setWorking(false);

    if (onSuccess) onSuccess();
    onClose();
  }

  return (
    <Dialog
      isOpen
      className={classNames(Classes.ALERT, 'async-action-dialog', className)}
      canEscapeKeyClose={!working}
      onClose={onClose}
    >
      <div className={Classes.ALERT_BODY}>
        {working ? (
          <FormGroup className="progress-group" label="Processing action...">
            <ProgressBar intent={intent || Intent.PRIMARY} />
          </FormGroup>
        ) : (
          <>
            {icon && <Icon icon={icon} />}
            <div className={Classes.ALERT_CONTENTS}>{children}</div>
          </>
        )}
      </div>
      <div className={Classes.ALERT_FOOTER}>
        {working ? (
          <Button icon={IconNames.EYE_OFF} text="Run in background" onClick={onClose} />
        ) : (
          <>
            <Button
              intent={intent}
              text={confirmButtonText}
              onClick={handleConfirm}
              disabled={confirmButtonDisabled}
            />
            <Button text={cancelButtonText || 'Cancel'} onClick={onClose} />
          </>
        )}
      </div>
    </Dialog>
  );
}
