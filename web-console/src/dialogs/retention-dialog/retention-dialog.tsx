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
import axios from 'axios';
import React from 'react';

import { SnitchDialog } from '..';
import { ExternalLink, RuleEditor } from '../../components';
import { getLink } from '../../links';
import { QueryManager } from '../../utils';
import { Rule, RuleUtil } from '../../utils/load-rule';

import './retention-dialog.scss';

export function reorderArray<T>(items: T[], oldIndex: number, newIndex: number): T[] {
  const newItems = items.concat();

  if (newIndex > oldIndex) newIndex--;

  newItems.splice(newIndex, 0, newItems.splice(oldIndex, 1)[0]);

  return newItems;
}

export interface RetentionDialogProps {
  datasource: string;
  rules: Rule[];
  defaultRules: Rule[];
  tiers: string[];
  onEditDefaults: () => void;
  onCancel: () => void;
  onSave: (datasource: string, newRules: Rule[], comment: string) => void;
}

export interface RetentionDialogState {
  currentRules: Rule[];
  historyRecords: any[] | undefined;
}

export class RetentionDialog extends React.PureComponent<
  RetentionDialogProps,
  RetentionDialogState
> {
  private historyQueryManager: QueryManager<string, any[]>;

  constructor(props: RetentionDialogProps) {
    super(props);

    this.state = {
      currentRules: props.rules,
      historyRecords: [],
    };

    this.historyQueryManager = new QueryManager({
      processQuery: async datasource => {
        const historyResp = await axios(`/druid/coordinator/v1/rules/${datasource}/history`);
        return historyResp.data;
      },
      onStateChange: ({ result }) => {
        this.setState({
          historyRecords: result,
        });
      },
    });
  }

  componentDidMount() {
    const { datasource } = this.props;
    this.historyQueryManager.runQuery(datasource);
  }

  private save = (comment: string) => {
    const { datasource, onSave } = this.props;
    const { currentRules } = this.state;

    onSave(datasource, currentRules, comment);
  };

  private changeRule = (newRule: Rule, index: number) => {
    const { currentRules } = this.state;

    const newRules = (currentRules || []).map((r, i) => {
      if (i === index) return newRule;
      return r;
    });

    this.setState({
      currentRules: newRules,
    });
  };

  onDeleteRule = (index: number) => {
    const { currentRules } = this.state;

    const newRules = (currentRules || []).filter((_r, i) => i !== index);

    this.setState({
      currentRules: newRules,
    });
  };

  moveRule(index: number, offset: number) {
    const { currentRules } = this.state;
    if (!currentRules) return;

    const newIndex = index + offset;

    this.setState({
      currentRules: reorderArray(currentRules, index, newIndex),
    });
  }

  renderRule = (rule: Rule, index: number) => {
    const { tiers } = this.props;
    const { currentRules } = this.state;

    return (
      <RuleEditor
        rule={rule}
        tiers={tiers}
        key={index}
        onChange={r => this.changeRule(r, index)}
        onDelete={() => this.onDeleteRule(index)}
        moveUp={index > 0 ? () => this.moveRule(index, -1) : null}
        moveDown={index < (currentRules || []).length - 1 ? () => this.moveRule(index, 2) : null}
      />
    );
  };

  renderDefaultRule = (rule: Rule, index: number) => {
    return (
      <Button disabled key={index}>
        {RuleUtil.ruleToString(rule)}
      </Button>
    );
  };

  reset = () => {
    const { rules } = this.props;

    this.setState({
      currentRules: rules.concat(),
    });
  };

  addRule = () => {
    const { tiers } = this.props;
    const { currentRules } = this.state;

    const newRules = (currentRules || []).concat({
      type: 'loadForever',
      tieredReplicants: { [tiers[0]]: 2 },
    });

    this.setState({
      currentRules: newRules,
    });
  };

  render(): JSX.Element {
    const { datasource, onCancel, onEditDefaults, defaultRules } = this.props;
    const { currentRules, historyRecords } = this.state;

    return (
      <SnitchDialog
        className="retention-dialog"
        saveDisabled={false}
        onClose={onCancel}
        title={`Edit retention rules: ${datasource}${
          datasource === '_default' ? ' (cluster defaults)' : ''
        }`}
        onReset={this.reset}
        onSave={this.save}
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
            currentRules.map(this.renderRule)
          ) : datasource !== '_default' ? (
            <p className="no-rules-message">
              This datasource currently has no rules, it will use the cluster defaults.
            </p>
          ) : (
            undefined
          )}
          <div>
            <Button icon={IconNames.PLUS} onClick={this.addRule}>
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
              {defaultRules.map(this.renderDefaultRule)}
            </FormGroup>
          </>
        )}
      </SnitchDialog>
    );
  }
}
