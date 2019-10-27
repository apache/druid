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
import React, { useState } from 'react';
import AceEditor from 'react-ace';

import { validJson } from '../../utils';

import './spec-dialog.scss';

export interface SpecDialogProps {
  onSubmit: (spec: JSON) => void;
  onClose: () => void;
  title: string;
  initSpec?: any;
}

export const SpecDialog = React.memo(function SpecDialog(props: SpecDialogProps) {
  const { onClose, onSubmit, title, initSpec } = props;
  const [spec, setSpec] = useState(() => (initSpec ? JSON.stringify(initSpec, null, 2) : '{\n\n}'));

  function postSpec(): void {
    if (!validJson(spec)) return;
    onSubmit(JSON.parse(spec));
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
        className="spec-dialog-textarea"
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
        }}
        style={{}}
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            onClick={postSpec}
            disabled={!validJson(spec)}
          />
        </div>
      </div>
    </Dialog>
  );
});
