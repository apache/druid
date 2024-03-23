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

import { durationSanitizer } from '../../utils';
import type { Rule } from '../../utils/load-rule';
import { RuleUtil } from '../../utils/load-rule';
import { SuggestibleInput } from '../suggestible-input/suggestible-input';

import './rule-editor.scss';

const PERIOD_SUGGESTIONS: string[] = ['P1D', 'P7D', 'P1M', 'P1Y', 'P1000Y'];

export interface RuleEditorProps {
  rule: Rule;
  tiers: string[];
  onChange?: (newRule: Rule) => void;
  onDelete?: () => void;
  moveUp?: () => void;
  moveDown?: () => void;
}

export const RuleEditor = React.memo(function RuleEditor(props: RuleEditorProps) {
  const { rule, onChange, tiers, onDelete, moveUp, moveDown } = props;
  const [isOpen, setIsOpen] = useState(true);
  const disabled = !onChange;

  function removeTier(key: string) {
    const newTierReplicants = { ...rule.tieredReplicants };
    delete newTierReplicants[key];

    const newRule = { ...rule, tieredReplicants: newTierReplicants };
    onChange?.(newRule);
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

    onChange?.(RuleUtil.addTieredReplicant(rule, newTierName, 1));
  }

  function renderTiers() {
    const tieredReplicants = rule.tieredReplicants || {};
    const tieredReplicantsList = Object.entries(tieredReplicants);
    if (!tieredReplicantsList.length) {
      return (
        <FormGroup>
          There is no historical replication configured, data will not be loaded on historicals.
        </FormGroup>
      );
    }

    return (
      <FormGroup>
        {tieredReplicantsList.map(([tier, replication]) => (
          <ControlGroup key={tier}>
            <Button minimal disabled={disabled} style={{ pointerEvents: 'none' }}>
              Tier:
            </Button>
            <HTMLSelect
              fill
              value={tier}
              disabled={disabled}
              onChange={(e: any) =>
                onChange?.(RuleUtil.renameTieredReplicants(rule, tier, e.target.value))
              }
            >
              <option key={tier} value={tier}>
                {tier}
              </option>
              {tiers
                .filter(t => t !== tier && !tieredReplicants[t])
                .map(t => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
            </HTMLSelect>
            <Button minimal disabled={disabled} style={{ pointerEvents: 'none' }}>
              Replicants:
            </Button>
            <NumericInput
              value={replication}
              disabled={disabled}
              onValueChange={(v: number) => {
                if (isNaN(v)) return;
                onChange?.(RuleUtil.addTieredReplicant(rule, tier, v));
              }}
              min={0}
              max={256}
            />
            {onChange && <Button onClick={() => removeTier(tier)} icon={IconNames.TRASH} />}
          </ControlGroup>
        ))}
      </FormGroup>
    );
  }

  function renderTierAdder() {
    if (!onChange) return;
    const disabled = Object.keys(rule.tieredReplicants || {}).length >= Object.keys(tiers).length;

    return (
      <FormGroup>
        <Button
          onClick={addTier}
          minimal
          icon={IconNames.PLUS}
          disabled={disabled}
          title={disabled ? 'There are no tiers left to assign' : ''}
        >
          Add historical tier replication
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
        {onDelete && <Button minimal icon={IconNames.TRASH} onClick={onDelete} />}
      </div>

      <Collapse isOpen={isOpen}>
        <Card elevation={2}>
          <FormGroup>
            <ControlGroup>
              <HTMLSelect
                value={rule.type}
                disabled={disabled}
                onChange={(e: any) => onChange?.(RuleUtil.changeRuleType(rule, e.target.value))}
              >
                {RuleUtil.TYPES.map(type => (
                  <option key={type} value={type}>
                    {type}
                  </option>
                ))}
              </HTMLSelect>
              {RuleUtil.hasPeriod(rule) && (
                <SuggestibleInput
                  value={rule.period || ''}
                  sanitizer={durationSanitizer}
                  disabled={disabled}
                  onValueChange={period => {
                    if (typeof period === 'undefined') return;
                    onChange?.(RuleUtil.changePeriod(rule, period));
                  }}
                  placeholder={PERIOD_SUGGESTIONS[0]}
                  suggestions={PERIOD_SUGGESTIONS}
                />
              )}
              {RuleUtil.hasIncludeFuture(rule) && (
                <Switch
                  className="include-future"
                  checked={RuleUtil.getIncludeFuture(rule)}
                  label="Include future"
                  disabled={disabled}
                  onChange={() => {
                    onChange?.(
                      RuleUtil.changeIncludeFuture(rule, !RuleUtil.getIncludeFuture(rule)),
                    );
                  }}
                />
              )}
              {RuleUtil.hasInterval(rule) && (
                <InputGroup
                  value={rule.interval || ''}
                  readOnly={!onChange}
                  onChange={(e: any) => onChange?.(RuleUtil.changeInterval(rule, e.target.value))}
                  placeholder="2010-01-01/2020-01-01"
                />
              )}
            </ControlGroup>
          </FormGroup>
          {RuleUtil.canHaveTieredReplicants(rule) && (
            <>
              {renderTiers()}
              {renderTierAdder()}
            </>
          )}
        </Card>
      </Collapse>
    </div>
  );
});
