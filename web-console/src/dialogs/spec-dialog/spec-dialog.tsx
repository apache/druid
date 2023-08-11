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
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';
import AceEditor from 'react-ace';

import { AppToaster } from '../../singletons';
import { offsetToRowColumn } from '../../utils';

import './spec-dialog.scss';

export interface SpecDialogProps {
  onSubmit(spec: JSON): void | Promise<void>;
  onClose(): void;
  title: string;
  initSpec?: any;
}

export const SpecDialog = React.memo(function SpecDialog(props: SpecDialogProps) {
  const { onClose, onSubmit, title, initSpec } = props;
  const [spec, setSpec] = useState(() =>
    initSpec ? JSONBig.stringify(initSpec, undefined, 2) : '',
  );

  function handleSubmit(): void {
    let parsed: any;
    try {
      parsed = JSONBig.parse(spec);
    } catch (e) {
      const rowColumn = typeof e.at === 'number' ? offsetToRowColumn(spec, e.at) : undefined;
      AppToaster.show({
        intent: Intent.DANGER,
        message: `Could not parse JSON: ${e.message}${
          rowColumn ? ` (at line ${rowColumn.row + 1}, column ${rowColumn.column + 1})` : ''
        }`,
        timeout: 5000,
      });
      return;
    }

    void onSubmit(parsed);
    onClose();
  }

  return (
    <Dialog
      className="spec-dialog"
      isOpen
      onClose={onClose}
      title={title}
      canOutsideClickClose={false}
    >
      <AceEditor
        mode="hjson"
        theme="solarized_dark"
        className="spec-dialog-textarea placeholder-padding"
        onChange={setSpec}
        fontSize={12}
        showPrintMargin={false}
        showGutter
        highlightActiveLine
        value={spec}
        width="100%"
        setOptions={{
          showLineNumbers: true,
          tabSize: 2,
          newLineMode: 'unix' as any, // newLineMode is incorrectly assumed to be boolean in the typings
        }}
        style={{}}
        placeholder="{ JSON spec... }"
        onLoad={editor => {
          editor.renderer.setPadding(10);
          editor.renderer.setScrollMargin(10, 10, 0, 0);
        }}
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button text="Submit" intent={Intent.PRIMARY} onClick={handleSubmit} disabled={!spec} />
        </div>
      </div>
    </Dialog>
  );
});
