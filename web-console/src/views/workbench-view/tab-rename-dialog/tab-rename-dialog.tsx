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

import { Button, Classes, Dialog, FormGroup, InputGroup, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import { useGlobalEventListener } from '../../../hooks';

interface TabRenameDialogProps {
  initialTabName: string;
  onSave(newTabName: string): void;
  onClose(): void;
}

export const TabRenameDialog = React.memo(function TabRenameDialog(props: TabRenameDialogProps) {
  const { initialTabName, onSave, onClose } = props;

  const [newTabName, setNewTabName] = useState<string>(initialTabName || '');

  useGlobalEventListener('keydown', (e: KeyboardEvent) => {
    if (e.key !== 'Enter') return;
    onSave(newTabName);
    onClose();
  });

  return (
    <Dialog
      className="tab-rename-dialog"
      onClose={onClose}
      isOpen
      title="Rename tab"
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <FormGroup label="New tab name">
          <InputGroup value={newTabName} onChange={e => setNewTabName(e.target.value)} autoFocus />
        </FormGroup>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={!newTabName}
            onClick={() => {
              onSave(newTabName);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
