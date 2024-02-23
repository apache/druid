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

import { Button, Classes, Code, Dialog, FileInput, FormGroup, Intent } from '@blueprintjs/core';
import * as JSONBig from 'json-bigint-native';
import type { DragEvent } from 'react';
import React, { useState } from 'react';

import { Execution } from '../../../druid-models';
import { AppToaster } from '../../../singletons';
import type { QueryDetailArchive } from '../../../utils';
import { offsetToRowColumn } from '../../../utils';

import './execution-submit-dialog.scss';

function getDraggedFile(ev: DragEvent<HTMLDivElement>): File | undefined {
  if (!ev.dataTransfer) return;

  if (ev.dataTransfer.items) {
    // Use DataTransferItemList interface to access the file(s)
    const item = ev.dataTransfer.items[0];
    if (item.kind === 'file') {
      return item.getAsFile() || undefined;
    }
  } else {
    return ev.dataTransfer.files[0];
  }

  return;
}

export interface ExecutionSubmitDialogProps {
  onSubmit(execution: Execution): void;
  onClose(): void;
}

export const ExecutionSubmitDialog = React.memo(function ExecutionSubmitDialog(
  props: ExecutionSubmitDialogProps,
) {
  const { onClose, onSubmit } = props;
  const [selectedFile, setSelectedFile] = useState<File | undefined>();
  const [dragging, setDragging] = useState(false);

  async function handleSubmit(): Promise<void> {
    if (!selectedFile) return;

    const text = await selectedFile.text();

    let parsed: QueryDetailArchive;
    try {
      parsed = JSONBig.parse(text);
    } catch (e) {
      const rowColumn = typeof e.at === 'number' ? offsetToRowColumn(text, e.at) : undefined;
      AppToaster.show({
        intent: Intent.DANGER,
        message: `Could not parse JSON: ${e.message}${
          rowColumn ? ` (at line ${rowColumn.row + 1}, column ${rowColumn.column + 1})` : ''
        }`,
        timeout: 5000,
      });
      return;
    }

    let execution: Execution | undefined;
    const detailArchiveVersion: unknown =
      parsed.detailArchiveVersion ?? (parsed as any).profileVersion;
    if (typeof detailArchiveVersion === 'number') {
      try {
        if (detailArchiveVersion === 2) {
          execution = Execution.fromTaskReport(parsed.reports)
            .updateWithTaskPayload(parsed.payload)
            .updateWithAsyncStatus(parsed.statementsStatus);
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
    } else if (typeof (parsed as any).multiStageQuery === 'object') {
      try {
        execution = Execution.fromTaskReport(parsed as any);
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
      backdropClassName={dragging ? `dragging-file` : undefined}
      style={dragging ? { pointerEvents: 'none' } : undefined}
      isOpen
      onClose={onClose}
      title="Load query detail archive"
      backdropProps={{
        onDrop(ev: DragEvent<HTMLDivElement>) {
          // Prevent default behavior (Prevent file from being opened)
          ev.preventDefault();
          if (dragging) setDragging(false);

          const droppedFile = getDraggedFile(ev);

          if (droppedFile) {
            if (!droppedFile.name.endsWith('.json')) {
              AppToaster.show({
                intent: Intent.DANGER,
                message: `The Query Detail Archive must be a .json file`,
                timeout: 5000,
              });
              return;
            }

            setSelectedFile(droppedFile);
          }
        },
        onDragOver(ev: DragEvent<HTMLDivElement>) {
          ev.preventDefault(); // Prevent default behavior (Prevent file from being opened)
          if (!dragging) setDragging(true);
        },
        onDragLeave(ev: DragEvent<HTMLDivElement>) {
          ev.preventDefault(); // Prevent default behavior (Prevent file from being opened)
          if (dragging) setDragging(false);
        },
      }}
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <p>
          You can load query detail archive files from other Druid clusters to render the query
          detail here.
        </p>
        <p>
          To download the query detail archive for a query, click on the query in the{' '}
          <Code>Recent query tasks</Code> panel in the query view.
        </p>
        <FormGroup label="Select query detail archive file">
          <FileInput
            hasSelection={Boolean(selectedFile)}
            text={selectedFile?.name ?? 'Choose file...'}
            onInputChange={e => setSelectedFile((e.target as any).files[0])}
            inputProps={{ accept: '.json' }}
            fill
          />
        </FormGroup>
        <p>Alternatively, drag a file directly onto this dialog.</p>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            onClick={() => void handleSubmit()}
            disabled={!selectedFile}
          />
        </div>
      </div>
    </Dialog>
  );
});
