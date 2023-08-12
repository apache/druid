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

import { Button, Divider, FormGroup, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import type { FormJsonTabs } from '../../components';
import { ExternalLink, FormJsonSelector, JsonInput, RuleEditor } from '../../components';
import type { Capabilities } from '../../helpers';
import { useQueryManager } from '../../hooks';
import { getLink } from '../../links';
import { Api } from '../../singletons';
import { filterMap, queryDruidSql, swapElements } from '../../utils';
import type { Rule } from '../../utils/load-rule';
import { SnitchDialog } from '..';

import './retention-dialog.scss';

const CLUSTER_DEFAULT_FAKE_DATASOURCE = '_default';

export interface RetentionDialogProps {
  datasource: string;
  rules: Rule[];
  defaultRules: Rule[];
  capabilities: Capabilities;
  onEditDefaults(): void;
  onCancel(): void;
  onSave(datasource: string, newRules: Rule[], comment: string): void | Promise<void>;
}

export const RetentionDialog = React.memo(function RetentionDialog(props: RetentionDialogProps) {
  const { datasource, onCancel, onEditDefaults, rules, defaultRules, capabilities } = props;
  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [currentRules, setCurrentRules] = useState(props.rules);
  const [jsonError, setJsonError] = useState<Error | undefined>();

  const [tiersState] = useQueryManager<Capabilities, string[]>({
    initQuery: capabilities,
    processQuery: async capabilities => {
      if (capabilities.hasSql()) {
        const sqlResp = await queryDruidSql<{ tier: string }>({
          query: `SELECT "tier"
FROM "sys"."servers"
WHERE "server_type" = 'historical'
GROUP BY 1
ORDER BY 1`,
        });

        return sqlResp.map(d => d.tier);
      } else if (capabilities.hasCoordinatorAccess()) {
        const allServiceResp = await Api.instance.get('/druid/coordinator/v1/servers?simple');
        return filterMap(allServiceResp.data, (s: any) =>
          s.type === 'historical' ? s.tier : undefined,
        );
      } else {
        throw new Error(`must have sql or coordinator access`);
      }
    },
  });

  const tiers = tiersState.data || [];

  const [historyQueryState] = useQueryManager<string, any[]>({
    initQuery: props.datasource,
    processQuery: async datasource => {
      const historyResp = await Api.instance.get(
        `/druid/coordinator/v1/rules/${Api.encodePath(datasource)}/history?count=200`,
      );
      return historyResp.data;
    },
  });

  const historyRecords = historyQueryState.data || [];

  function saveHandler(comment: string) {
    const { datasource, onSave } = props;
    void onSave(datasource, currentRules, comment);
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

  return (
    <SnitchDialog
      className="retention-dialog"
      saveDisabled={Boolean(jsonError)}
      onClose={onCancel}
      title={`Edit retention rules: ${datasource}${
        datasource === CLUSTER_DEFAULT_FAKE_DATASOURCE ? ' (cluster defaults)' : ''
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
      <FormJsonSelector
        tab={currentTab}
        onChange={t => {
          setJsonError(undefined);
          setCurrentTab(t);
        }}
      />
      {currentTab === 'form' ? (
        <FormGroup>
          {currentRules.length ? (
            currentRules.map((rule, index) => (
              <RuleEditor
                key={index}
                rule={rule}
                tiers={tiers}
                onChange={r => changeRule(r, index)}
                onDelete={() => deleteRule(index)}
                moveUp={index > 0 ? () => moveRule(index, -1) : undefined}
                moveDown={index < currentRules.length - 1 ? () => moveRule(index, 1) : undefined}
              />
            ))
          ) : datasource !== CLUSTER_DEFAULT_FAKE_DATASOURCE ? (
            <p className="no-rules-message">
              This datasource currently has no rules, it will use the cluster defaults.
            </p>
          ) : undefined}
          <div>
            <Button
              icon={IconNames.PLUS}
              onClick={addRule}
              intent={currentRules.length ? undefined : Intent.PRIMARY}
            >
              New rule
            </Button>
          </div>
        </FormGroup>
      ) : (
        <JsonInput
          value={currentRules}
          onChange={setCurrentRules}
          setError={setJsonError}
          height="100%"
        />
      )}
      {datasource !== CLUSTER_DEFAULT_FAKE_DATASOURCE && (
        <>
          <Divider />
          <FormGroup
            label={
              <>
                Cluster defaults (<a onClick={onEditDefaults}>edit</a>)
              </>
            }
          >
            <p>The cluster default rules are evaluated if none of the above rules match.</p>
            {currentTab === 'form' ? (
              defaultRules.map((rule, index) => (
                <RuleEditor key={index} rule={rule} tiers={tiers} />
              ))
            ) : (
              <JsonInput value={defaultRules} />
            )}
          </FormGroup>
        </>
      )}
    </SnitchDialog>
  );
});
