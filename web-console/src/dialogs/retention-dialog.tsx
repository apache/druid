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

import * as React from 'react';
import axios from 'axios';
import {
  FormGroup,
  Button,
} from "@blueprintjs/core";
import { IconNames } from "@blueprintjs/icons";
import { RuleEditor, Rule } from '../components/rule-editor';
import { SnitchDialog } from './snitch-dialog';

import './retention-dialog.scss';

export function reorderArray<T>(items: T[], oldIndex: number, newIndex: number): T[] {
  let newItems = items.concat();

  if (newIndex > oldIndex) newIndex--;

  newItems.splice(newIndex, 0, newItems.splice(oldIndex, 1)[0]);

  return newItems;
}

export interface RetentionDialogProps extends React.Props<any> {
  datasource: string;
  rules: any[];
  tiers: string[];
  onEditDefaults: () => void;
  onCancel: () => void;
  onSave: (datasource: string, newRules: any[], author: string, comment: string) => void;
}

export interface RetentionDialogState {
  currentRules: any[];
}

export class RetentionDialog extends React.Component<RetentionDialogProps, RetentionDialogState> {

  constructor(props: RetentionDialogProps) {
    super(props);

    this.state = {
      currentRules: props.rules
    };
  }

  private save = (author: string, comment: string) => {
    const { datasource, onSave } = this.props;
    const { currentRules } = this.state;

    onSave(datasource, currentRules, author, comment);
  }

  private changeRule = (newRule: any, index: number) => {
    const { currentRules } = this.state;

    const newRules = (currentRules || []).map((r, i) => {
      if (i === index) return newRule;
      return r;
    });

    this.setState({
      currentRules: newRules
    });
  }

  onDeleteRule = (index: number) => {
    const { currentRules } = this.state;

    const newRules = (currentRules || []).filter((r, i) => i !== index);

    this.setState({
      currentRules: newRules
    });
  }

  moveRule(index: number, offset: number) {
    const { currentRules } = this.state;
    if (!currentRules) return;

    const newIndex = index + offset;

    this.setState({
      currentRules: reorderArray(currentRules, index, newIndex)
    });
  }

  renderRule = (rule: any, index: number) => {
    const { tiers } = this.props;
    const { currentRules } = this.state;

    return <RuleEditor
      rule={rule}
      tiers={tiers}
      key={index}
      onChange={r => this.changeRule(r, index)}
      onDelete={() => this.onDeleteRule(index)}
      moveUp={index > 0 ? () => this.moveRule(index, -1) : null}
      moveDown={index < (currentRules || []).length - 1 ? () => this.moveRule(index, 2) : null}
    />
  }

  reset = () => {
    const { rules } = this.props;

    this.setState({
      currentRules: rules.concat()
    });
  }

  addRule = () => {
    const { tiers } = this.props;
    const { currentRules } = this.state;

    const newRules = (currentRules || []).concat({
      type: 'loadForever',
      tieredReplicants: { [tiers[0]]: 2 }
    });

    this.setState({
      currentRules: newRules
    });
  }

  render() {
    const { datasource, onCancel, onEditDefaults } = this.props;
    const { currentRules } = this.state;

    return <SnitchDialog
      className="retention-dialog"
      saveDisabled={false}
      canOutsideClickClose={false}
      isOpen
      onClose={onCancel}
      title={`Edit retention rules: ${datasource}${datasource === '_default' ? ' (cluster defaults)' : ''}`}
      onReset={this.reset}
      onSave={this.save}
    >
      <p>
        Druid uses rules to determine what data should be retained in the cluster.
        The rules are evaluated in order from top to bottom.
        For more information please refer to the <a href="http://druid.io/docs/latest/operations/rule-configuration.html" target="_blank">documentation</a>.
      </p>
      <FormGroup>
        {(currentRules || []).map(this.renderRule)}
      </FormGroup>
      <FormGroup className="right">
        <Button icon={IconNames.PLUS} onClick={this.addRule}>New rule</Button>
      </FormGroup>
      {
        (!currentRules.length && datasource !== '_default') &&
        <p>
          This datasource currently has no rule, it will use the cluster defaults (<a onClick={onEditDefaults}>edit cluster defaults</a>)
        </p>
      }
    </SnitchDialog>;
  }
}
