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
  Card,
  Collapse,
  ControlGroup,
  FormGroup,
  HTMLSelect,
  InputGroup,
  NumericInput,
  Switch,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { Rule, RuleUtil } from '../../utils/load-rule';

import './rule-editor.scss';

export interface RuleEditorProps {
  rule: Rule;
  tiers: any[];
  onChange: (newRule: Rule) => void;
  onDelete: () => void;
  moveUp: (() => void) | null;
  moveDown: (() => void) | null;
}

export const RuleEditor = React.memo(function RuleEditor(props: RuleEditorProps) {
  const { rule, onChange, tiers, onDelete, moveUp, moveDown } = props;
  const [isOpen, setIsOpen] = useState(true);
  if (!rule) return null;

  function removeTier(key: string) {
    const newTierReplicants = Object.assign({}, rule.tieredReplicants);
    delete newTierReplicants[key];

    const newRule = Object.assign({}, rule, { tieredReplicants: newTierReplicants });
    onChange(newRule);
  }

  function addTier() {
    let newTierName = tiers[0];

    if (rule.tieredReplicants) {
      for (const tier of tiers) {
        if (rule.tieredReplicants[tier] === undefined) {
          newTierName = tier;
          break;
        }
      }
    }

    onChange(RuleUtil.addTieredReplicant(rule, newTierName, 1));
  }

  function renderTiers() {
    const tieredReplicants = rule.tieredReplicants;
    if (!tieredReplicants) return;

    const ruleTiers = Object.keys(tieredReplicants).sort();
    return ruleTiers.map(tier => {
      return (
        <ControlGroup key={tier}>
          <Button minimal style={{ pointerEvents: 'none' }}>
            Replicants:
          </Button>
          <NumericInput
            value={tieredReplicants[tier]}
            onValueChange={(v: number) => {
              if (isNaN(v)) return;
              onChange(RuleUtil.addTieredReplicant(rule, tier, v));
            }}
            min={1}
            max={256}
          />
          <Button minimal style={{ pointerEvents: 'none' }}>
            Tier:
          </Button>
          <HTMLSelect
            fill
            value={tier}
            onChange={(e: any) =>
              onChange(RuleUtil.renameTieredReplicants(rule, tier, e.target.value))
            }
          >
            <option key={tier} value={tier}>
              {tier}
            </option>
            {tiers
              .filter(t => t !== tier && !tieredReplicants[t])
              .map(t => {
                return (
                  <option key={t} value={t}>
                    {t}
                  </option>
                );
              })}
          </HTMLSelect>
          <Button
            disabled={ruleTiers.length === 1}
            onClick={() => removeTier(tier)}
            icon={IconNames.TRASH}
          />
        </ControlGroup>
      );
    });
  }

  function renderTierAdder() {
    const { rule, tiers } = props;
    if (Object.keys(rule.tieredReplicants || {}).length >= Object.keys(tiers).length) return;

    return (
      <FormGroup className="right">
        <Button onClick={addTier} minimal icon={IconNames.PLUS}>
          Add a tier
        </Button>
      </FormGroup>
    );
  }

  return (
    <div className="rule-editor">
      <div className="title">
        <Button
          className="left"
          minimal
          rightIcon={isOpen ? IconNames.CARET_DOWN : IconNames.CARET_RIGHT}
          onClick={() => setIsOpen(!isOpen)}
        >
          {RuleUtil.ruleToString(rule)}
        </Button>
        <div className="spacer" />
        {moveUp && <Button minimal icon={IconNames.ARROW_UP} onClick={moveUp} />}
        {moveDown && <Button minimal icon={IconNames.ARROW_DOWN} onClick={moveDown} />}
        <Button minimal icon={IconNames.TRASH} onClick={onDelete} />
      </div>

      <Collapse isOpen={isOpen}>
        <Card>
          <FormGroup>
            <ControlGroup>
              <HTMLSelect
                value={rule.type}
                onChange={(e: any) =>
                  onChange(RuleUtil.changeRuleType(rule, e.target.value as any))
                }
              >
                {RuleUtil.TYPES.map(type => {
                  return (
                    <option key={type} value={type}>
                      {type}
                    </option>
                  );
                })}
              </HTMLSelect>
              {RuleUtil.hasPeriod(rule) && (
                <InputGroup
                  value={rule.period || ''}
                  onChange={(e: any) =>
                    onChange(RuleUtil.changePeriod(rule, e.target.value as any))
                  }
                  placeholder="P1D"
                />
              )}
              {RuleUtil.hasIncludeFuture(rule) && (
                <Switch
                  className="include-future"
                  checked={rule.includeFuture || false}
                  label="Include future"
                  onChange={() => {
                    onChange(RuleUtil.changeIncludeFuture(rule, !rule.includeFuture));
                  }}
                />
              )}
              {RuleUtil.hasInterval(rule) && (
                <InputGroup
                  value={rule.interval || ''}
                  onChange={(e: any) =>
                    onChange(RuleUtil.changeInterval(rule, e.target.value as any))
                  }
                  placeholder="2010-01-01/2020-01-01"
                />
              )}
            </ControlGroup>
          </FormGroup>
          {RuleUtil.hasTieredReplicants(rule) && (
            <FormGroup>
              {renderTiers()}
              {renderTierAdder()}
            </FormGroup>
          )}
        </Card>
      </Collapse>
    </div>
  );
});
