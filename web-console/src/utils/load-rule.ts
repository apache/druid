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

import { deepMove, deepSet } from './object-change';

export type RuleType =
  | 'loadForever'
  | 'loadByInterval'
  | 'loadByPeriod'
  | 'dropForever'
  | 'dropByInterval'
  | 'dropByPeriod'
  | 'dropBeforeByPeriod'
  | 'broadcastForever'
  | 'broadcastByInterval'
  | 'broadcastByPeriod';

export interface Rule {
  type: RuleType;
  interval?: string;
  period?: string;
  includeFuture?: boolean;
  tieredReplicants?: Record<string, number>;
}

export class RuleUtil {
  static TYPES: RuleType[] = [
    'loadForever',
    'loadByInterval',
    'loadByPeriod',
    'dropForever',
    'dropByInterval',
    'dropByPeriod',
    'dropBeforeByPeriod',
    'broadcastForever',
    'broadcastByInterval',
    'broadcastByPeriod',
  ];

  static ruleToString(rule: Rule): string {
    const params: string[] = [];

    if (RuleUtil.hasPeriod(rule))
      params.push(`${rule.period}${rule.includeFuture ? '+future' : ''}`);
    if (RuleUtil.hasInterval(rule)) params.push(rule.interval || '?');
    if (RuleUtil.hasTieredReplicants(rule)) params.push(`${RuleUtil.totalReplicas(rule)}x`);

    return `${rule.type}(${params.join(', ')})`;
  }

  static changeRuleType(rule: Rule, type: RuleType): Rule {
    const newRule = deepSet(rule, 'type', type);

    if (RuleUtil.hasPeriod(newRule)) {
      if (!newRule.period) newRule.period = 'P1M';
    } else {
      delete newRule.period;
      delete newRule.includeFuture;
    }

    if (RuleUtil.hasInterval(newRule)) {
      if (!newRule.interval) newRule.interval = '2010-01-01/2020-01-01';
    } else {
      delete newRule.interval;
    }

    if (RuleUtil.hasTieredReplicants(newRule)) {
      if (!newRule.tieredReplicants) newRule.tieredReplicants = { _default_tier: 2 };
    } else {
      delete newRule.tieredReplicants;
    }

    return newRule;
  }

  static hasPeriod(rule: Rule): boolean {
    return rule.type.endsWith('ByPeriod');
  }

  static changePeriod(rule: Rule, period: string): Rule {
    return deepSet(rule, 'period', period);
  }

  static hasIncludeFuture(rule: Rule): boolean {
    return RuleUtil.hasPeriod(rule) && rule.type !== 'dropBeforeByPeriod';
  }

  static changeIncludeFuture(rule: Rule, includeFuture: boolean): Rule {
    return deepSet(rule, 'includeFuture', includeFuture);
  }

  static hasInterval(rule: Rule): boolean {
    return rule.type.endsWith('ByInterval');
  }

  static changeInterval(rule: Rule, interval: string): Rule {
    return deepSet(rule, 'interval', interval);
  }

  static hasTieredReplicants(rule: Rule): boolean {
    return rule.type.startsWith('load');
  }

  static renameTieredReplicants(rule: Rule, oldTier: string, newTier: string): Rule {
    return deepMove(rule, `tieredReplicants.${oldTier}`, `tieredReplicants.${newTier}`);
  }

  static addTieredReplicant(rule: Rule, tier: string, replication: number): Rule {
    const newTieredReplicants = deepSet(rule.tieredReplicants || {}, tier, replication);
    return deepSet(rule, 'tieredReplicants', newTieredReplicants);
  }

  static totalReplicas(rule: Rule): number {
    const tieredReplicants = rule.tieredReplicants || {};
    let total = 0;
    for (const k in tieredReplicants) {
      total += tieredReplicants[k];
    }
    return total;
  }

  static isColdRule(rule: Rule): boolean {
    return RuleUtil.hasTieredReplicants(rule) && RuleUtil.totalReplicas(rule) === 0;
  }

  static hasColdRule(rules: Rule[] | undefined, defaultRules: Rule[] | undefined): boolean {
    return (
      (rules || []).some(RuleUtil.isColdRule) || (defaultRules || []).some(RuleUtil.isColdRule)
    );
  }
}
