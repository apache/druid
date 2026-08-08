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

import { Button, Card, Classes, Collapse, Dialog, H5, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import type { FormJsonTabs } from '../../components';
import { AutoForm, FormJsonSelector, JsonInput } from '../../components';
import type {
  DataSchemaRule,
  DeletionRule,
  ExpressionVirtualColumn,
  IndexSpecRule,
  InlineRuleProvider,
  PartitioningRule,
  ReindexingRule,
} from '../../druid-models';
import {
  DATA_SCHEMA_RULE_FIELDS,
  DELETION_RULE_FIELDS,
  INDEX_SPEC_RULE_FIELDS,
  newDataSchemaRule,
  newDeletionRule,
  newIndexSpecRule,
  newPartitioningRule,
  PARTITIONING_RULE_FIELDS,
  summarizeRule,
} from '../../druid-models';
import { swapElements } from '../../utils';

import { VirtualColumnsEditor } from './virtual-columns-editor';

import './rule-provider-editor.scss';

export interface RuleProviderEditorProps {
  onClose: () => void;
  onSave: (ruleProvider: InlineRuleProvider) => void;
  ruleProvider: InlineRuleProvider | undefined;
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

// Augment partitioning rule fields with customDialog for virtualColumns
const PARTITIONING_RULE_FIELDS_WITH_DIALOGS = PARTITIONING_RULE_FIELDS.map(field => {
  if (field.name === 'virtualColumns') {
    return { ...field, customDialog: makeVirtualColumnsDialog };
  }
  return field;
});

export const RuleProviderEditor = React.memo(function RuleProviderEditor(
  props: RuleProviderEditorProps,
) {
  const { onClose, onSave, ruleProvider } = props;

  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [currentProvider, setCurrentProvider] = useState<InlineRuleProvider>(
    ruleProvider || { type: 'inline' },
  );
  const [jsonError, setJsonError] = useState<Error | undefined>();

  return (
    <Dialog
      className="rule-provider-editor"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title="Edit rule provider"
    >
      <FormJsonSelector
        tab={currentTab}
        onChange={t => {
          setJsonError(undefined);
          setCurrentTab(t);
        }}
      />
      <div className="content">
        {currentTab === 'form' ? (
          <div className="rule-sections">
            <RuleSection<PartitioningRule>
              title="Partitioning rules"
              description="Control segment granularity and partitioning as data ages. Non-additive: only one applies per interval."
              rules={currentProvider.partitioningRules || []}
              fields={PARTITIONING_RULE_FIELDS_WITH_DIALOGS}
              onNewRule={newPartitioningRule}
              onChange={rules =>
                setCurrentProvider({ ...currentProvider, partitioningRules: rules })
              }
            />
            <RuleSection<DeletionRule>
              title="Deletion rules"
              description="Specify rows to remove during compaction. Additive: multiple rules can combine."
              rules={currentProvider.deletionRules || []}
              fields={DELETION_RULE_FIELDS}
              onNewRule={newDeletionRule}
              onChange={rules => setCurrentProvider({ ...currentProvider, deletionRules: rules })}
            />
            <RuleSection<IndexSpecRule>
              title="Index spec rules"
              description="Control compression and encoding settings. Non-additive: only one applies per interval."
              rules={currentProvider.indexSpecRules || []}
              fields={INDEX_SPEC_RULE_FIELDS}
              onNewRule={newIndexSpecRule}
              onChange={rules => setCurrentProvider({ ...currentProvider, indexSpecRules: rules })}
            />
            <RuleSection<DataSchemaRule>
              title="Data schema rules"
              description="Control dimensions, metrics, query granularity, rollup, and projections. Non-additive: only one applies per interval."
              rules={currentProvider.dataSchemaRules || []}
              fields={DATA_SCHEMA_RULE_FIELDS}
              onNewRule={newDataSchemaRule}
              onChange={rules => setCurrentProvider({ ...currentProvider, dataSchemaRules: rules })}
            />
          </div>
        ) : (
          <JsonInput
            value={currentProvider}
            onChange={v => setCurrentProvider(v)}
            setError={setJsonError}
            height="100%"
          />
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={Boolean(jsonError)}
            onClick={() => {
              onSave(currentProvider);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});

// --- Generic rule section component ---

interface RuleSectionProps<R extends ReindexingRule> {
  title: string;
  description: string;
  rules: R[];
  fields: any[];
  onNewRule: () => R;
  onChange: (rules: R[]) => void;
}

function RuleSection<R extends ReindexingRule>(props: RuleSectionProps<R>) {
  const { title, description, rules, fields, onNewRule, onChange } = props;
  const [sectionOpen, setSectionOpen] = useState(true);
  const [openRules, setOpenRules] = useState<Record<number, boolean>>({});

  function toggleRule(index: number) {
    setOpenRules({ ...openRules, [index]: !openRules[index] });
  }

  function addRule() {
    onChange([...rules, onNewRule()]);
    setOpenRules({ ...openRules, [rules.length]: true });
  }

  function deleteRule(index: number) {
    onChange(rules.filter((_r, i) => i !== index));
  }

  function changeRule(index: number, newRule: R) {
    onChange(rules.map((r, i) => (i === index ? newRule : r)));
  }

  function moveRule(index: number, direction: number) {
    onChange(swapElements(rules, index, index + direction));
  }

  return (
    <div className="rule-section">
      <div className="rule-section-header">
        <Button
          minimal
          icon={sectionOpen ? IconNames.CARET_DOWN : IconNames.CARET_RIGHT}
          onClick={() => setSectionOpen(!sectionOpen)}
        />
        <H5 className="rule-section-title">{title}</H5>
        <span className="rule-section-count">({rules.length})</span>
      </div>
      <Collapse isOpen={sectionOpen}>
        <p className="rule-section-description">{description}</p>
        {rules.map((rule, index) => (
          <Card key={index} className="rule-card" elevation={1}>
            <div className="rule-card-header">
              <Button
                minimal
                small
                icon={openRules[index] ? IconNames.CARET_DOWN : IconNames.CARET_RIGHT}
                onClick={() => toggleRule(index)}
              />
              <span className="rule-card-summary">{summarizeRule(rule)}</span>
              <span className="rule-card-age">olderThan: {rule.olderThan}</span>
              <div className="spacer" />
              {index > 0 && (
                <Button
                  minimal
                  small
                  icon={IconNames.ARROW_UP}
                  onClick={() => moveRule(index, -1)}
                />
              )}
              {index < rules.length - 1 && (
                <Button
                  minimal
                  small
                  icon={IconNames.ARROW_DOWN}
                  onClick={() => moveRule(index, 1)}
                />
              )}
              <Button
                minimal
                small
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                onClick={() => deleteRule(index)}
              />
            </div>
            <Collapse isOpen={Boolean(openRules[index])}>
              <div className="rule-card-form">
                <AutoForm fields={fields} model={rule} onChange={m => changeRule(index, m as R)} />
              </div>
            </Collapse>
          </Card>
        ))}
        <Button icon={IconNames.PLUS} minimal onClick={addRule}>
          Add {title.toLowerCase().replace(' rules', ' rule')}
        </Button>
      </Collapse>
    </div>
  );
}
