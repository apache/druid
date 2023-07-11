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

import { Execution } from '../../../druid-models';
import { AppToaster } from '../../../singletons';
import { validJson } from '../../../utils';

import './execution-submit-dialog.scss';

export interface ExecutionSubmitDialogProps {
  onSubmit: (execution: Execution) => void;
  onClose: () => void;
}

export const ExecutionSubmitDialog = React.memo(function ExecutionSubmitDialog(
  props: ExecutionSubmitDialogProps,
) {
  const { onClose, onSubmit } = props;
  const [archive, setArchive] = useState('');

  function submitProfile(): void {
    if (!validJson(archive)) return;
    let parsed: any;
    try {
      parsed = JSON.parse(archive);
    } catch (e) {
      AppToaster.show({
        intent: Intent.DANGER,
        message: `Could not parse JSON: ${e.message}`,
      });
      return;
    }

    let execution: Execution | undefined;
    const detailArchiveVersion = parsed.detailArchiveVersion ?? parsed.profileVersion;
    if (typeof detailArchiveVersion === 'number') {
      try {
        if (detailArchiveVersion === 2) {
          execution = Execution.fromTaskPayloadAndReport(parsed.payload, parsed.reports);
        } else {
          AppToaster.show({
            intent: Intent.DANGER,
            message: `Unsupported detail archive version: ${detailArchiveVersion}`,
          });
          return;
        }
      } catch (e) {
        AppToaster.show({
          intent: Intent.DANGER,
          message: `Could not decode profile: ${e.message}`,
        });
        return;
      }
    } else if (typeof parsed.multiStageQuery === 'object') {
      try {
        execution = Execution.fromTaskPayloadAndReport({} as any, parsed);
      } catch (e) {
        AppToaster.show({
          intent: Intent.DANGER,
          message: `Could not decode report payload: ${e.message}`,
        });
        return;
      }
    } else {
      AppToaster.show({
        intent: Intent.DANGER,
        message: `The input has not been recognized`,
      });
      return;
    }

    onSubmit(execution);
    onClose();
  }

  return (
    <Dialog
      className="execution-submit-dialog"
      isOpen
      onClose={onClose}
      title="Load query detail archive"
      canOutsideClickClose={false}
    >
      <AceEditor
        mode="hjson"
        theme="solarized_dark"
        className="execution-submit-dialog-textarea placeholder-padding"
        onChange={setArchive}
        fontSize={12}
        showPrintMargin={false}
        showGutter
        highlightActiveLine
        value={archive}
        width="100%"
        setOptions={{
          showLineNumbers: true,
          tabSize: 2,
          newLineMode: 'unix' as any, // newLineMode is incorrectly assumed to be boolean in the typings
        }}
        style={{}}
        placeholder="{ Query detail archive or query report... }"
        onLoad={editor => {
          editor.renderer.setPadding(10);
          editor.renderer.setScrollMargin(10, 10, 0, 0);
        }}
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            onClick={submitProfile}
            disabled={!validJson(archive)}
          />
        </div>
      </div>
    </Dialog>
  );
});
