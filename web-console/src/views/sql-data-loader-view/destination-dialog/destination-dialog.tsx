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
import React, { useEffect, useState } from 'react';

import { DestinationInfo, getDestinationInfo, IngestQueryPattern } from '../../../druid-models';
import { DestinationForm } from '../destination-form/destination-form';

import './destination-dialog.scss';

interface DestinationDialogProps {
  existingTables: string[];
  ingestQueryPattern: IngestQueryPattern;
  changeIngestQueryPattern(ingestQueryPattern: IngestQueryPattern): void;
  onClose(): void;
}

export const DestinationDialog = React.memo(function DestinationDialog(
  props: DestinationDialogProps,
) {
  const { ingestQueryPattern, changeIngestQueryPattern, existingTables, onClose } = props;

  const [info, setInfo] = useState<DestinationInfo | undefined>();

  useEffect(() => {
    if (!existingTables) return;
    setInfo(getDestinationInfo(ingestQueryPattern, existingTables));
  }, [ingestQueryPattern, existingTables]);

  return (
    <Dialog
      className="destination-dialog"
      onClose={onClose}
      isOpen
      title="Destination"
      canOutsideClickClose={false}
    >
      {existingTables && info && (
        <DestinationForm
          className={Classes.DIALOG_BODY}
          existingTables={existingTables}
          destinationInfo={info}
          changeDestinationInfo={setInfo}
        />
      )}
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={
              !existingTables ||
              !info ||
              (info.mode === 'new' && existingTables.includes(info.table)) ||
              (info.mode !== 'new' && !existingTables.includes(info.table))
            }
            onClick={() => {
              if (!info || !existingTables) return;

              changeIngestQueryPattern({
                ...ingestQueryPattern,
                mode: info.mode === 'new' ? 'replace' : info.mode,
                destinationTableName: info.table,
              });
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
