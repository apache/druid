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

import { Button, Classes, Dialog, Intent, TextArea } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import React from 'react';

import { AppToaster } from '../../singletons';

import './show-value-dialog.scss';

export interface ShowValueDialogProps {
  title?: string;
  str: string;
  size?: 'normal' | 'large';
  onClose: () => void;
}

export const ShowValueDialog = React.memo(function ShowValueDialog(props: ShowValueDialogProps) {
  const { title, onClose, str, size } = props;

  function handleCopy() {
    copy(str, { format: 'text/plain' });
    AppToaster.show({
      message: 'Value copied to clipboard',
      intent: Intent.SUCCESS,
    });
  }

  return (
    <Dialog
      className={classNames('show-value-dialog', size || 'normal')}
      isOpen
      onClose={onClose}
      title={title || 'Full value'}
    >
      <TextArea value={str} spellCheck={false} />
      <div className={Classes.DIALOG_FOOTER_ACTIONS}>
        <Button icon={IconNames.DUPLICATE} text="Copy" onClick={handleCopy} />
        <Button text="Close" intent={Intent.PRIMARY} onClick={onClose} />
      </div>
    </Dialog>
  );
});
