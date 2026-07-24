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
  Callout,
  Classes,
  Code,
  Dialog,
  FormGroup,
  HTMLSelect,
  Intent,
  Switch,
} from '@blueprintjs/core';
import React, { useState } from 'react';

import type { Field, FormJsonTabs } from '../../components';
import {
  AutoForm,
  ExternalLink,
  FormGroupWithInfo,
  FormJsonSelector,
  JsonInput,
  PopoverText,
} from '../../components';
import type {
  CompactionConfig,
  ExpressionVirtualColumn,
  InlineRuleProvider,
} from '../../druid-models';
import {
  COMPACTION_CONFIG_FIELDS,
  compactionConfigHasLegacyInputSegmentSizeBytesSet,
  newReindexCascadeConfig,
  REINDEX_CASCADE_CONFIG_FIELDS,
} from '../../druid-models';
import { getLink } from '../../links';
import { deepDelete, deepGet, deepSet, formatBytesCompact } from '../../utils';
import { CompactionHistoryDialog } from '../compaction-history-dialog/compaction-history-dialog';

import { COMPACTION_CONFIG_COMPLETIONS } from './compaction-config-completions';
import { RuleProviderEditor } from './rule-provider-editor';
import { VirtualColumnsEditor } from './virtual-columns-editor';

import './compaction-config-dialog.scss';

export interface CompactionConfigDialogProps {
  onClose: () => void;
  onSave: (compactionConfig: CompactionConfig) => void | Promise<void>;
  onDelete: () => void;
  datasource: string;
  compactionConfig: CompactionConfig | undefined;
  useSupervisors?: boolean;
}

type TemplateType = 'inline' | 'reindexCascade';

function getTemplateType(config: CompactionConfig): TemplateType {
  return config.type === 'reindexCascade' ? 'reindexCascade' : 'inline';
}

export const CompactionConfigDialog = React.memo(function CompactionConfigDialog(
  props: CompactionConfigDialogProps,
) {
  const { datasource, compactionConfig, useSupervisors, onSave, onClose, onDelete } = props;

  const [showHistory, setShowHistory] = useState(false);
  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [currentConfig, setCurrentConfig] = useState<CompactionConfig>(
    compactionConfig || {
      dataSource: datasource,
      tuningConfig: { partitionsSpec: { type: 'dynamic' } },
    },
  );
  const [jsonError, setJsonError] = useState<Error | undefined>();

  const templateType = getTemplateType(currentConfig);
  const isReindexCascade = templateType === 'reindexCascade';

  const fields: Field<CompactionConfig>[] = isReindexCascade
    ? (REINDEX_CASCADE_CONFIG_FIELDS as Field<CompactionConfig>[])
    : COMPACTION_CONFIG_FIELDS;
  const issueWithCurrentConfig = AutoForm.issueWithModel(currentConfig, fields);
  const disableSubmit = Boolean(jsonError || issueWithCurrentConfig);

  function handleTemplateTypeChange(newType: TemplateType) {
    if (newType === templateType) return;
    if (newType === 'reindexCascade') {
      setCurrentConfig(newReindexCascadeConfig(datasource));
    } else {
      setCurrentConfig({
        dataSource: datasource,
        tuningConfig: { partitionsSpec: { type: 'dynamic' } },
      });
    }
  }

  function makeVirtualColumnsDialog({
    value,
    onValueChange,
    onClose: dialogClose,
  }: {
    value: any;
    onValueChange: (v: any) => void;
    onClose: () => void;
  }) {
    return (
      <VirtualColumnsEditor
        virtualColumns={value as ExpressionVirtualColumn[] | undefined}
        onSave={(vcs: ExpressionVirtualColumn[]) => {
          onValueChange(vcs.length ? vcs : undefined);
        }}
        onClose={dialogClose}
      />
    );
  }

  // Build the reindexCascade field list with customDialog wired up
  function getFieldsWithDialogs(): Field<CompactionConfig>[] {
    if (!isReindexCascade) return fields;
    return fields.map(field => {
      if (field.name === 'ruleProvider') {
        return {
          ...field,
          customDialog: ({
            value,
            onValueChange,
            onClose: dialogClose,
          }: {
            value: any;
            onValueChange: (v: any) => void;
            onClose: () => void;
          }) => (
            <RuleProviderEditor
              ruleProvider={value as InlineRuleProvider | undefined}
              onSave={onValueChange}
              onClose={dialogClose}
            />
          ),
        };
      }
      if (field.name === 'defaultPartitioningVirtualColumns') {
        return {
          ...field,
          customDialog: makeVirtualColumnsDialog,
        };
      }
      return field;
    });
  }

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
      {!isReindexCascade && compactionConfigHasLegacyInputSegmentSizeBytesSet(currentConfig) && (
        <Callout className="legacy-callout" intent={Intent.WARNING}>
          <p>
            Your current config sets the legacy <Code>inputSegmentSizeBytes</Code> to{' '}
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
      <FormJsonSelector
        tab={currentTab}
        onChange={t => {
          setJsonError(undefined);
          setCurrentTab(t);
        }}
      />
      <div className="content">
        {currentTab === 'form' ? (
          <>
            {useSupervisors && (
              <FormGroup label="Template type">
                <HTMLSelect
                  value={templateType}
                  onChange={e => handleTemplateTypeChange(e.target.value as TemplateType)}
                >
                  <option value="inline">Inline</option>
                  <option value="reindexCascade">Cascading reindexing</option>
                </HTMLSelect>
              </FormGroup>
            )}
            <AutoForm
              fields={getFieldsWithDialogs()}
              model={currentConfig}
              onChange={m => setCurrentConfig(m as CompactionConfig)}
            />
            {!isReindexCascade && (
              <FormGroupWithInfo
                inlineInfo
                info={
                  <PopoverText>
                    <p>
                      If you want to append data to a datasource while compaction is running, you
                      need to enable concurrent append and replace for the datasource by updating
                      the compaction settings.
                    </p>
                    <p>
                      For more information refer to the{' '}
                      <ExternalLink href={`${getLink('DOCS')}/ingestion/concurrent-append-replace`}>
                        documentation
                      </ExternalLink>
                      .
                    </p>
                  </PopoverText>
                }
              >
                <Switch
                  label="Use concurrent locks"
                  checked={Boolean(deepGet(currentConfig, 'taskContext.useConcurrentLocks'))}
                  onChange={() => {
                    setCurrentConfig(
                      (deepGet(currentConfig, 'taskContext.useConcurrentLocks')
                        ? deepDelete(currentConfig, 'taskContext.useConcurrentLocks')
                        : deepSet(currentConfig, 'taskContext.useConcurrentLocks', true)) as any,
                    );
                  }}
                />
              </FormGroupWithInfo>
            )}
          </>
        ) : (
          <JsonInput
            value={currentConfig}
            onChange={setCurrentConfig}
            setError={setJsonError}
            issueWithValue={value => AutoForm.issueWithModel(value, fields)}
            height="100%"
            jsonCompletions={!isReindexCascade ? COMPACTION_CONFIG_COMPLETIONS : undefined}
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
          {compactionConfig ? (
            <Button text="Delete" intent={Intent.DANGER} onClick={onDelete} />
          ) : (
            <Button
              text="Delete"
              disabled
              intent={Intent.DANGER}
              data-tooltip="There is no compaction config currently set for this datasource"
            />
          )}
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
