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

import { Button, Callout, Classes, Code, Dialog, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import type { FormJsonTabs } from '../../components';
import { AutoForm, FormJsonSelector, JsonInput } from '../../components';
import type { CompactionConfig } from '../../druid-models';
import {
  COMPACTION_CONFIG_FIELDS,
  compactionConfigHasLegacyInputSegmentSizeBytesSet,
} from '../../druid-models';
import { deepDelete, formatBytesCompact } from '../../utils';
import { CompactionHistoryDialog } from '../compaction-history-dialog/compaction-history-dialog';

import './compaction-config-dialog.scss';

export interface CompactionConfigDialogProps {
  onClose: () => void;
  onSave: (compactionConfig: CompactionConfig) => void | Promise<void>;
  onDelete: () => void;
  datasource: string;
  compactionConfig: CompactionConfig | undefined;
}

export const CompactionConfigDialog = React.memo(function CompactionConfigDialog(
  props: CompactionConfigDialogProps,
) {
  const { datasource, compactionConfig, onSave, onClose, onDelete } = props;

  const [showHistory, setShowHistory] = useState(false);
  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [currentConfig, setCurrentConfig] = useState<CompactionConfig>(
    compactionConfig || {
      dataSource: datasource,
      tuningConfig: { partitionsSpec: { type: 'dynamic' } },
    },
  );
  const [jsonError, setJsonError] = useState<Error | undefined>();

  const issueWithCurrentConfig = AutoForm.issueWithModel(currentConfig, COMPACTION_CONFIG_FIELDS);
  const disableSubmit = Boolean(jsonError || issueWithCurrentConfig);

  if (showHistory) {
    return (
      <CompactionHistoryDialog datasource={datasource} onClose={() => setShowHistory(false)} />
    );
  }

  return (
    <Dialog
      className="compaction-config-dialog"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title={`Compaction config: ${datasource}`}
    >
      {compactionConfigHasLegacyInputSegmentSizeBytesSet(currentConfig) && (
        <Callout className="legacy-callout" intent={Intent.WARNING}>
          <p>
            You current config sets the legacy <Code>inputSegmentSizeBytes</Code> to{' '}
            <Code>{formatBytesCompact(currentConfig.inputSegmentSizeBytes!)}</Code> it is
            recommended to unset this property.
          </p>
          <p>
            <Button
              intent={Intent.WARNING}
              text="Remove legacy setting"
              onClick={() => setCurrentConfig(deepDelete(currentConfig, 'inputSegmentSizeBytes'))}
            />
          </p>
        </Callout>
      )}
      <FormJsonSelector tab={currentTab} onChange={setCurrentTab} />
      <div className="content">
        {currentTab === 'form' ? (
          <AutoForm
            fields={COMPACTION_CONFIG_FIELDS}
            model={currentConfig}
            onChange={m => setCurrentConfig(m as CompactionConfig)}
          />
        ) : (
          <JsonInput
            value={currentConfig}
            onChange={v => {
              setCurrentConfig(v);
              setJsonError(undefined);
            }}
            onError={setJsonError}
            issueWithValue={value => AutoForm.issueWithModel(value, COMPACTION_CONFIG_FIELDS)}
            height="100%"
          />
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button
            className="history-button"
            text="History"
            minimal
            onClick={() => setShowHistory(true)}
          />
          {compactionConfig && <Button text="Delete" intent={Intent.DANGER} onClick={onDelete} />}
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            disabled={disableSubmit}
            onClick={() => void onSave(currentConfig)}
          />
        </div>
      </div>
    </Dialog>
  );
});
