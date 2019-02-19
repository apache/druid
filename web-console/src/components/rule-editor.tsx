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
  ControlGroup,
  Card,
  InputGroup,
  HTMLSelect,
  Collapse, NumericInput, TagInput,
} from "@blueprintjs/core";
import { IconNames } from "@blueprintjs/icons";
import './rule-editor.scss';

export interface Rule {
  type: 'loadForever' | 'loadByInterval' | 'loadByPeriod' | 'dropForever' | 'dropByInterval' | 'dropByPeriod' | 'broadcastForever' | 'broadcastByInterval' | 'broadcastByPeriod';
  interval?: string;
  period?: string;
  tieredReplicants?: Record<string, number>;
  colocatedDataSources?: string[];
}

export type LoadType = 'load' | 'drop' | 'broadcast';
export type TimeType = 'Forever' | 'ByInterval' | 'ByPeriod';

export interface RuleEditorProps extends React.Props<any> {
  rule: Rule;
  tiers: any[];
  onChange: (newRule: Rule) => void;
  onDelete: () => void;
  moveUp: (() => void) | null;
  moveDown: (() => void) | null;
}

export interface RuleEditorState {
  isOpen: boolean;
}

export class RuleEditor extends React.Component<RuleEditorProps, RuleEditorState> {
  static ruleToString(rule: Rule): string {
    return rule.type + (rule.period ? `(${rule.period})` : '') + (rule.interval ? `(${rule.interval})` : '');
  }

  static getLoadType(rule: Rule): LoadType {
    const m = rule.type.match(/^(load|drop|broadcast)(\w+)$/);
    if (!m) throw new Error(`unknown rule type: '${rule.type}'`);
    return m[1] as any;
  }

  static getTimeType(rule: Rule): TimeType {
    const m = rule.type.match(/^(load|drop|broadcast)(\w+)$/);
    if (!m) throw new Error(`unknown rule type: '${rule.type}'`);
    return m[2] as any;
  }

  static changeLoadType(rule: Rule, loadType: LoadType): Rule {
    const newRule = Object.assign({}, rule, { type: loadType + RuleEditor.getTimeType(rule) });
    if (loadType !== 'load') delete newRule.tieredReplicants;
    if (loadType !== 'broadcast') delete newRule.colocatedDataSources;
    return newRule;
  }

  static changeTimeType(rule: Rule, timeType: TimeType): Rule {
    const newRule = Object.assign({}, rule, { type: RuleEditor.getLoadType(rule) + timeType });
    if (timeType !== 'ByPeriod') delete newRule.period;
    if (timeType !== 'ByInterval') delete newRule.interval;
    return newRule;
  }

  static changePeriod(rule: Rule, period: string): Rule {
    return Object.assign({}, rule, { period });
  }

  static changeInterval(rule: Rule, interval: string): Rule {
    return Object.assign({}, rule, { interval });
  }

  static changeTier(rule: Rule, oldTier: string, newTier: string): Rule {
    const newRule = Object.assign({}, rule);
    newRule.tieredReplicants = Object.assign({}, newRule.tieredReplicants);
    newRule.tieredReplicants[newTier] = newRule.tieredReplicants[oldTier];
    delete newRule.tieredReplicants[oldTier];
    return newRule;
  }

  static changeTierReplication(rule: Rule, tier: string, replication: number): Rule {
    const newRule = Object.assign({}, rule);
    newRule.tieredReplicants = Object.assign({}, newRule.tieredReplicants, { [tier]: replication });
    return newRule;
  }

  static changeColocatedDataSources(rule: Rule, colocatedDataSources: string[]): Rule {
    return Object.assign({}, rule, { colocatedDataSources });
  }

  constructor(props: RuleEditorProps) {
    super(props);
    this.state = {
      isOpen: true
    }
  }

  private removeTier = (key: string) => {
    const { rule, onChange } = this.props;

    const newTierReplicants = Object.assign({}, rule.tieredReplicants);
    delete newTierReplicants[key];

    const newRule = Object.assign({}, rule, {tieredReplicants: newTierReplicants});
    onChange(newRule);
  }

  private addTier = () => {
    const { rule, onChange, tiers } = this.props;

    let newTierName = tiers[0];

    if (rule.tieredReplicants) {
      for (let i = 0; i < tiers.length; i++) {
        if (rule.tieredReplicants[tiers[i]] === undefined) {
          newTierName = tiers[i];
          break;
        }
      }
    }

    onChange(RuleEditor.changeTierReplication(rule, newTierName, 1));
  }

  renderTiers() {
    const { tiers, onChange, rule } = this.props;
    if (!rule) return null;
    if (RuleEditor.getLoadType(rule) !== 'load') return null;

    const tieredReplicants = rule.tieredReplicants;
    if (!tieredReplicants) return null;

    const ruleTiers = Object.keys(tieredReplicants).sort();
    return ruleTiers.map(tier => {
      return <ControlGroup key={tier}>
        <Button minimal style={{pointerEvents: 'none'}}>Replicants:</Button>
        <NumericInput
          value={tieredReplicants[tier]}
          onValueChange={v => {
            if (isNaN(v)) return;
            onChange(RuleEditor.changeTierReplication(rule, tier, v));
          }}
          min={1}
          max={256}
        />
        <Button minimal style={{pointerEvents: 'none'}}>Tier:</Button>
        <HTMLSelect
          fill={true}
          value={tier}
          options={tiers.filter(t => t === tier || !tieredReplicants[t])}
          onChange={e => onChange(RuleEditor.changeTier(rule, tier, e.target.value))}
        />
        <Button
          disabled={ruleTiers.length === 1}
          onClick={() => this.removeTier(tier)}
          icon={IconNames.TRASH}
        />
      </ControlGroup>;
    });
  }

  renderTierAdder() {
    const { rule, tiers } = this.props;
    if (Object.keys(rule.tieredReplicants || {}).length >= Object.keys(tiers).length) return null;

    return <FormGroup className="right">
      <Button onClick={this.addTier} minimal icon={IconNames.PLUS}>Add a tier</Button>
    </FormGroup>;
  }

  renderColocatedDataSources() {
    const { rule, onChange } = this.props;

    return <ControlGroup>
      <Button minimal style={{pointerEvents: 'none'}}>Colocated datasources:</Button>
      <TagInput
        values={rule.colocatedDataSources || []}
        onChange={(v: any) => onChange(RuleEditor.changeColocatedDataSources(rule, v)) }
        addOnBlur={true}
        fill={true}
      />
    </ControlGroup>;
  }

  render() {
    const { tiers, onChange, rule, onDelete, moveUp, moveDown } = this.props;
    const { isOpen } = this.state;

    if (!rule) return null;

    const ruleLoadTypes: {label: string, value: LoadType}[] = [
      {label: 'Load', value: 'load'},
      {label: 'Drop', value: 'drop'},
      {label: 'Broadcast', value: 'broadcast'}
    ];

    const ruleTimeTypes: {label: string, value: TimeType}[] = [
      {label: 'forever', value: 'Forever'},
      {label: 'by period', value: 'ByPeriod'},
      {label: 'by interval', value: 'ByInterval'}
    ];

    const ruleLoadType = RuleEditor.getLoadType(rule);
    const ruleTimeType = RuleEditor.getTimeType(rule);

    return <div className="rule-editor">
      <div className="title">
        <Button className="left" minimal rightIcon={isOpen ? IconNames.CARET_DOWN : IconNames.CARET_RIGHT} onClick={() => this.setState({isOpen: !isOpen})}>
          {RuleEditor.ruleToString(rule)}
        </Button>
        <div className="spacer"/>
        {moveUp ? <Button minimal icon={IconNames.ARROW_UP} onClick={moveUp}/> : null}
        {moveDown ? <Button minimal icon={IconNames.ARROW_DOWN} onClick={moveDown}/> : null}
        <Button minimal icon={IconNames.TRASH} onClick={onDelete}/>
      </div>

      <Collapse isOpen={isOpen}>
        <Card>
          <FormGroup>
            <ControlGroup>
              <HTMLSelect
                value={ruleLoadType}
                options={ruleLoadTypes}
                onChange={e => onChange(RuleEditor.changeLoadType(rule, e.target.value as any))}
              />
              <HTMLSelect
                value={ruleTimeType}
                options={ruleTimeTypes}
                onChange={e => onChange(RuleEditor.changeTimeType(rule, e.target.value as any))}
              />
              { ruleTimeType === 'ByPeriod' && <InputGroup value={rule.period || ''} onChange={(e: any) => onChange(RuleEditor.changePeriod(rule, e.target.value as any))}/>}
              { ruleTimeType === 'ByInterval' && <InputGroup value={rule.interval || ''} onChange={(e: any) => onChange(RuleEditor.changeInterval(rule, e.target.value as any))}/>}
            </ControlGroup>
          </FormGroup>
          {
            ruleLoadType === 'load' &&
            <FormGroup>
              { this.renderTiers() }
              { this.renderTierAdder() }
            </FormGroup>
          }
          {
            ruleLoadType === 'broadcast' &&
            <FormGroup>
              { this.renderColocatedDataSources() }
            </FormGroup>
          }
        </Card>
      </Collapse>
    </div>;
  }
}
