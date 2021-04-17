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

import { AutoForm, FormJsonSelector, FormJsonTabs, JsonInput } from '../../components';
import { CompactionConfig, COMPACTION_CONFIG_FIELDS } from '../../druid-models';

import './compaction-dialog.scss';

export interface CompactionDialogProps {
  onClose: () => void;
  onSave: (compactionConfig: CompactionConfig) => void;
  onDelete: () => void;
  datasource: string;
  compactionConfig: CompactionConfig | undefined;
}

export const CompactionDialog = React.memo(function CompactionDialog(props: CompactionDialogProps) {
  const { datasource, compactionConfig, onSave, onClose, onDelete } = props;

  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [currentConfig, setCurrentConfig] = useState<CompactionConfig>(
    compactionConfig || {
      dataSource: datasource,
      tuningConfig: { partitionsSpec: { type: 'dynamic' } },
    },
  );

  const issueWithCurrentConfig = AutoForm.issueWithModel(currentConfig, COMPACTION_CONFIG_FIELDS);
  function handleSubmit() {
    if (issueWithCurrentConfig) return;
    onSave(currentConfig);
  }

  return (
    <Dialog
      className="compaction-dialog"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title={`Compaction config: ${datasource}`}
    >
      <FormJsonSelector tab={currentTab} onChange={setCurrentTab} />
      <div className="content">
        {currentTab === 'form' ? (
          <AutoForm
            fields={COMPACTION_CONFIG_FIELDS}
            model={currentConfig}
            onChange={m => setCurrentConfig(m)}
          />
        ) : (
          <JsonInput
            value={currentConfig}
            onChange={setCurrentConfig}
            issueWithValue={value => AutoForm.issueWithModel(value, COMPACTION_CONFIG_FIELDS)}
            height="100%"
          />
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          {compactionConfig && <Button text="Delete" intent={Intent.DANGER} onClick={onDelete} />}
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            onClick={handleSubmit}
            disabled={Boolean(issueWithCurrentConfig)}
          />
        </div>
      </div>
    </Dialog>
  );
});
