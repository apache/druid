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

import { Button, Divider, FormGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { SnitchDialog } from '..';
import { ExternalLink, RuleEditor } from '../../components';
import { useQueryManager } from '../../hooks';
import { getLink } from '../../links';
import { Api } from '../../singletons';
import { swapElements } from '../../utils';
import { Rule, RuleUtil } from '../../utils/load-rule';

import './retention-dialog.scss';

export interface RetentionDialogProps {
  datasource: string;
  rules: Rule[];
  defaultRules: Rule[];
  tiers: string[];
  onEditDefaults: () => void;
  onCancel: () => void;
  onSave: (datasource: string, newRules: Rule[], comment: string) => void;
}

export const RetentionDialog = React.memo(function RetentionDialog(props: RetentionDialogProps) {
  const { datasource, onCancel, onEditDefaults, rules, defaultRules, tiers } = props;
  const [currentRules, setCurrentRules] = useState(props.rules);

  const [historyQueryState] = useQueryManager<string, any[]>({
    processQuery: async datasource => {
      const historyResp = await Api.instance.get(
        `/druid/coordinator/v1/rules/${Api.encodePath(datasource)}/history`,
      );
      return historyResp.data;
    },
    initQuery: props.datasource,
  });

  const historyRecords = historyQueryState.data || [];

  function saveHandler(comment: string) {
    const { datasource, onSave } = props;
    onSave(datasource, currentRules, comment);
  }

  function addRule() {
    setCurrentRules(
      currentRules.concat({
        type: 'loadForever',
        tieredReplicants: { [tiers[0]]: 2 },
      }),
    );
  }

  function deleteRule(index: number) {
    setCurrentRules(currentRules.filter((_r, i) => i !== index));
  }

  function changeRule(newRule: Rule, index: number) {
    setCurrentRules(currentRules.map((r, i) => (i === index ? newRule : r)));
  }

  function moveRule(index: number, direction: number) {
    setCurrentRules(swapElements(currentRules, index, index + direction));
  }

  function renderRule(rule: Rule, index: number) {
    return (
      <RuleEditor
        rule={rule}
        tiers={tiers}
        key={index}
        onChange={r => changeRule(r, index)}
        onDelete={() => deleteRule(index)}
        moveUp={index > 0 ? () => moveRule(index, -1) : undefined}
        moveDown={index < currentRules.length - 1 ? () => moveRule(index, 1) : undefined}
      />
    );
  }

  function renderDefaultRule(rule: Rule, index: number) {
    return (
      <div className="default-rule" key={index}>
        <Button disabled>{RuleUtil.ruleToString(rule)}</Button>
      </div>
    );
  }

  return (
    <SnitchDialog
      className="retention-dialog"
      saveDisabled={false}
      onClose={onCancel}
      title={`Edit retention rules: ${datasource}${
        datasource === '_default' ? ' (cluster defaults)' : ''
      }`}
      onReset={() => setCurrentRules(rules)}
      onSave={saveHandler}
      historyRecords={historyRecords}
    >
      <p>
        Druid uses rules to determine what data should be retained in the cluster. The rules are
        evaluated in order from top to bottom. For more information please refer to the{' '}
        <ExternalLink href={`${getLink('DOCS')}/operations/rule-configuration.html`}>
          documentation
        </ExternalLink>
        .
      </p>
      <FormGroup>
        {currentRules.length ? (
          currentRules.map(renderRule)
        ) : datasource !== '_default' ? (
          <p className="no-rules-message">
            This datasource currently has no rules, it will use the cluster defaults.
          </p>
        ) : undefined}
        <div>
          <Button icon={IconNames.PLUS} onClick={addRule}>
            New rule
          </Button>
        </div>
      </FormGroup>
      {datasource !== '_default' && (
        <>
          <Divider />
          <FormGroup>
            <p>
              Cluster defaults (<a onClick={onEditDefaults}>edit</a>):
            </p>
            {defaultRules.map(renderDefaultRule)}
          </FormGroup>
        </>
      )}
    </SnitchDialog>
  );
});
