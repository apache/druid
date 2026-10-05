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
  Intent,
  TextArea,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import * as JSONBig from 'json-bigint-native';
import React, { useMemo, useState } from 'react';

import { CodeEditor } from '../../components';
import { AppToaster } from '../../singletons';
import { copyToClipboard } from '../../utils';

import './show-value-dialog.scss';

export interface ShowValueDialogProps {
  title?: string;
  str: string;
  size?: 'normal' | 'large';
  onClose: () => void;
}

export const ShowValueDialog = React.memo(function ShowValueDialog(props: ShowValueDialogProps) {
  const { title, onClose, str, size } = props;
  const [tab, setTab] = useState<'formatted' | 'raw'>('formatted');

  const parsed = useMemo(() => {
    try {
      return JSONBig.parse(str);
    } catch {}
  }, [str]);

  const hasParsed = typeof parsed !== 'undefined';

  function handleCopy() {
    copyToClipboard(str);
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
      <div className={Classes.DIALOG_BODY}>
        {hasParsed && (
          <FormGroup>
            <ButtonGroup fill>
              <Button
                text="Formatted"
                active={tab === 'formatted'}
                onClick={() => setTab('formatted')}
              />
              <Button text="Raw" active={tab === 'raw'} onClick={() => setTab('raw')} />
            </ButtonGroup>
          </FormGroup>
        )}
        {hasParsed && tab === 'formatted' && (
          <CodeEditor
            mode="hjson"
            className="query-string"
            height="100%"
            showGutter
            value={JSONBig.stringify(parsed, undefined, 2)}
          />
        )}
        {(!hasParsed || tab === 'raw') && <TextArea value={str} spellCheck={false} />}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button icon={IconNames.DUPLICATE} text="Copy" onClick={handleCopy} />
          <Button text="Close" intent={Intent.PRIMARY} onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
});
