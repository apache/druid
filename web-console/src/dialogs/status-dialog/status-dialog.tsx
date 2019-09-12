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

import { Button, Classes, Dialog, Intent } from '@blueprintjs/core';
import React from 'react';

import { ShowJson } from '../../components';
import { UrlBaser } from '../../singletons/url-baser';

import './status-dialog.scss';

interface StatusDialogProps {
  onClose: () => void;
}

export class StatusDialog extends React.PureComponent<StatusDialogProps> {
  render(): JSX.Element {
    const { onClose } = this.props;

    return (
      <Dialog className={'status-dialog'} onClose={onClose} isOpen title="Status">
        <div className={'status-dialog-main-area'}>
          <ShowJson endpoint={UrlBaser.base(`/status`)} downloadFilename={'status'} />
        </div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button text="Close" intent={Intent.PRIMARY} onClick={onClose} />
          </div>
        </div>
      </Dialog>
    );
  }
}
