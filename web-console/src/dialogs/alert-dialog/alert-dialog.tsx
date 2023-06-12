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

import type { MaybeElement } from '@blueprintjs/core';
import { Button, Classes, Dialog, Icon, Intent } from '@blueprintjs/core';
import type { IconName } from '@blueprintjs/icons';
import classNames from 'classnames';
import React from 'react';

export interface AlertDialogProps {
  className?: string;
  icon?: IconName | MaybeElement;
  intent?: Intent;
  canEscapeKeyCancel?: boolean;
  canOutsideClickCancel?: boolean;
  autoFocusConfirm?: boolean;
  cancelButtonText?: string;
  onCancel?(): void;
  confirmButtonText?: string;
  onConfirm?(): void;
  onClose?(confirmed: boolean): void;
  children?: React.ReactNode;
  isOpen?: boolean;
}

export const AlertDialog = React.memo(function AlertDialog(props: AlertDialogProps) {
  const {
    className,
    icon,
    intent,
    canEscapeKeyCancel,
    canOutsideClickCancel,
    autoFocusConfirm,
    cancelButtonText,
    onCancel,
    confirmButtonText,
    onConfirm,
    onClose,
    children,
    isOpen,
  } = props;

  function handleCancel() {
    onCancel?.();
    onClose?.(false);
  }

  function handleConfirm() {
    onConfirm?.();
    onClose?.(true);
  }

  return (
    <Dialog
      className={classNames(Classes.ALERT, 'alert-dialog', className)}
      canEscapeKeyClose={canEscapeKeyCancel ?? true}
      canOutsideClickClose={canOutsideClickCancel ?? true}
      onClose={handleCancel}
      isOpen={isOpen ?? true}
    >
      <div className={Classes.ALERT_BODY}>
        <Icon icon={icon} size={40} intent={intent} />
        <div className={Classes.ALERT_CONTENTS}>{children}</div>
      </div>
      <div className={Classes.ALERT_FOOTER}>
        <Button
          intent={intent}
          text={confirmButtonText ?? 'OK'}
          onClick={handleConfirm}
          autoFocus={autoFocusConfirm ?? intent !== Intent.DANGER}
        />
        {cancelButtonText && <Button text={cancelButtonText} onClick={handleCancel} />}
      </div>
    </Dialog>
  );
});
