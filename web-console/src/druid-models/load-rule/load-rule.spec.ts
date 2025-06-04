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

import type { Rule } from './load-rule';
import { RuleUtil } from './load-rule';

describe('RuleUtil', () => {
  describe('ruleToString', () => {
    it('converts loadForever rule', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 2, cold: 1 },
      };
      expect(RuleUtil.ruleToString(rule)).toBe('loadForever(3x)');
    });

    it('converts loadByInterval rule', () => {
      const rule: Rule = {
        type: 'loadByInterval',
        interval: '2010-01-01/2020-01-01',
        tieredReplicants: { tier1: 1 },
      };
      expect(RuleUtil.ruleToString(rule)).toBe('loadByInterval(2010-01-01/2020-01-01, 1x)');
    });

    it('converts loadByPeriod rule', () => {
      const rule: Rule = {
        type: 'loadByPeriod',
        period: 'P1M',
        tieredReplicants: { hot: 3 },
      };
      expect(RuleUtil.ruleToString(rule)).toBe('loadByPeriod(P1M+future, 3x)');
    });

    it('converts loadByPeriod rule with includeFuture false', () => {
      const rule: Rule = {
        type: 'loadByPeriod',
        period: 'P1D',
        includeFuture: false,
        tieredReplicants: { hot: 1 },
      };
      expect(RuleUtil.ruleToString(rule)).toBe('loadByPeriod(P1D, 1x)');
    });

    it('converts dropForever rule', () => {
      const rule: Rule = { type: 'dropForever' };
      expect(RuleUtil.ruleToString(rule)).toBe('dropForever()');
    });

    it('converts dropByInterval rule', () => {
      const rule: Rule = {
        type: 'dropByInterval',
        interval: '2000-01-01/2010-01-01',
      };
      expect(RuleUtil.ruleToString(rule)).toBe('dropByInterval(2000-01-01/2010-01-01)');
    });

    it('converts dropByPeriod rule', () => {
      const rule: Rule = {
        type: 'dropByPeriod',
        period: 'P1Y',
      };
      expect(RuleUtil.ruleToString(rule)).toBe('dropByPeriod(P1Y+future)');
    });

    it('converts dropBeforeByPeriod rule', () => {
      const rule: Rule = {
        type: 'dropBeforeByPeriod',
        period: 'P6M',
      };
      expect(RuleUtil.ruleToString(rule)).toBe('dropBeforeByPeriod(P6M+future)');
    });

    it('converts broadcastForever rule', () => {
      const rule: Rule = { type: 'broadcastForever' };
      expect(RuleUtil.ruleToString(rule)).toBe('broadcastForever()');
    });

    it('converts broadcastByInterval rule', () => {
      const rule: Rule = {
        type: 'broadcastByInterval',
        interval: '2020-01-01/2021-01-01',
      };
      expect(RuleUtil.ruleToString(rule)).toBe('broadcastByInterval(2020-01-01/2021-01-01)');
    });

    it('converts broadcastByPeriod rule', () => {
      const rule: Rule = {
        type: 'broadcastByPeriod',
        period: 'P3M',
      };
      expect(RuleUtil.ruleToString(rule)).toBe('broadcastByPeriod(P3M+future)');
    });

    it('handles load rule with no tiered replicants', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: {},
      };
      expect(RuleUtil.ruleToString(rule)).toBe('loadForever(0x)');
    });

    it('handles load rule with multiple tiers', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { cold: 1, hot: 2, archive: 3 },
      };
      expect(RuleUtil.ruleToString(rule)).toBe('loadForever(6x)');
    });
  });

  describe('changeRuleType', () => {
    it('changes from loadForever to loadByInterval', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.changeRuleType(rule, 'loadByInterval');
      expect(newRule).toEqual({
        type: 'loadByInterval',
        interval: '2010-01-01/2020-01-01',
        tieredReplicants: { hot: 1 },
      });
    });

    it('changes from loadByInterval to loadByPeriod', () => {
      const rule: Rule = {
        type: 'loadByInterval',
        interval: '2010-01-01/2020-01-01',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.changeRuleType(rule, 'loadByPeriod');
      expect(newRule).toEqual({
        type: 'loadByPeriod',
        period: 'P1M',
        includeFuture: true,
        tieredReplicants: { hot: 1 },
      });
    });

    it('changes from loadForever to dropForever', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.changeRuleType(rule, 'dropForever');
      expect(newRule).toEqual({
        type: 'dropForever',
      });
    });

    it('preserves period when changing between period types', () => {
      const rule: Rule = {
        type: 'loadByPeriod',
        period: 'P3M',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.changeRuleType(rule, 'dropByPeriod');
      expect(newRule).toEqual({
        type: 'dropByPeriod',
        period: 'P3M',
        includeFuture: true,
      });
    });

    it('preserves interval when changing between interval types', () => {
      const rule: Rule = {
        type: 'loadByInterval',
        interval: '2015-01-01/2016-01-01',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.changeRuleType(rule, 'broadcastByInterval');
      expect(newRule).toEqual({
        type: 'broadcastByInterval',
        interval: '2015-01-01/2016-01-01',
      });
    });
  });

  describe('hasPeriod', () => {
    it('returns true for period rules', () => {
      expect(RuleUtil.hasPeriod({ type: 'loadByPeriod' })).toBe(true);
      expect(RuleUtil.hasPeriod({ type: 'dropByPeriod' })).toBe(true);
      expect(RuleUtil.hasPeriod({ type: 'dropBeforeByPeriod' })).toBe(true);
      expect(RuleUtil.hasPeriod({ type: 'broadcastByPeriod' })).toBe(true);
    });

    it('returns false for non-period rules', () => {
      expect(RuleUtil.hasPeriod({ type: 'loadForever' })).toBe(false);
      expect(RuleUtil.hasPeriod({ type: 'loadByInterval' })).toBe(false);
      expect(RuleUtil.hasPeriod({ type: 'dropForever' })).toBe(false);
    });
  });

  describe('changePeriod', () => {
    it('updates period on a rule', () => {
      const rule: Rule = {
        type: 'loadByPeriod',
        period: 'P1M',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.changePeriod(rule, 'P3M');
      expect(newRule).toEqual({
        type: 'loadByPeriod',
        period: 'P3M',
        tieredReplicants: { hot: 1 },
      });
    });
  });

  describe('hasIncludeFuture', () => {
    it('returns true for load and broadcast period rules', () => {
      expect(RuleUtil.hasIncludeFuture({ type: 'loadByPeriod' })).toBe(true);
      expect(RuleUtil.hasIncludeFuture({ type: 'broadcastByPeriod' })).toBe(true);
    });

    it('returns true for drop period rules except dropBeforeByPeriod', () => {
      expect(RuleUtil.hasIncludeFuture({ type: 'dropByPeriod' })).toBe(true);
      expect(RuleUtil.hasIncludeFuture({ type: 'dropBeforeByPeriod' })).toBe(false);
    });

    it('returns false for non-period rules', () => {
      expect(RuleUtil.hasIncludeFuture({ type: 'loadForever' })).toBe(false);
      expect(RuleUtil.hasIncludeFuture({ type: 'loadByInterval' })).toBe(false);
    });
  });

  describe('getIncludeFuture', () => {
    it('returns includeFuture value when present', () => {
      const rule: Rule = {
        type: 'loadByPeriod',
        period: 'P1M',
        includeFuture: false,
      };
      expect(RuleUtil.getIncludeFuture(rule)).toBe(false);
    });

    it('returns true by default', () => {
      const rule: Rule = {
        type: 'loadByPeriod',
        period: 'P1M',
      };
      expect(RuleUtil.getIncludeFuture(rule)).toBe(true);
    });
  });

  describe('changeIncludeFuture', () => {
    it('updates includeFuture value', () => {
      const rule: Rule = {
        type: 'loadByPeriod',
        period: 'P1M',
        includeFuture: true,
      };
      const newRule = RuleUtil.changeIncludeFuture(rule, false);
      expect(newRule).toEqual({
        type: 'loadByPeriod',
        period: 'P1M',
        includeFuture: false,
      });
    });
  });

  describe('hasInterval', () => {
    it('returns true for interval rules', () => {
      expect(RuleUtil.hasInterval({ type: 'loadByInterval' })).toBe(true);
      expect(RuleUtil.hasInterval({ type: 'dropByInterval' })).toBe(true);
      expect(RuleUtil.hasInterval({ type: 'broadcastByInterval' })).toBe(true);
    });

    it('returns false for non-interval rules', () => {
      expect(RuleUtil.hasInterval({ type: 'loadForever' })).toBe(false);
      expect(RuleUtil.hasInterval({ type: 'loadByPeriod' })).toBe(false);
      expect(RuleUtil.hasInterval({ type: 'dropForever' })).toBe(false);
    });
  });

  describe('changeInterval', () => {
    it('updates interval on a rule', () => {
      const rule: Rule = {
        type: 'loadByInterval',
        interval: '2010-01-01/2020-01-01',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.changeInterval(rule, '2015-01-01/2025-01-01');
      expect(newRule).toEqual({
        type: 'loadByInterval',
        interval: '2015-01-01/2025-01-01',
        tieredReplicants: { hot: 1 },
      });
    });
  });

  describe('canHaveTieredReplicants', () => {
    it('returns true for load rules', () => {
      expect(RuleUtil.canHaveTieredReplicants({ type: 'loadForever' })).toBe(true);
      expect(RuleUtil.canHaveTieredReplicants({ type: 'loadByInterval' })).toBe(true);
      expect(RuleUtil.canHaveTieredReplicants({ type: 'loadByPeriod' })).toBe(true);
    });

    it('returns false for drop and broadcast rules', () => {
      expect(RuleUtil.canHaveTieredReplicants({ type: 'dropForever' })).toBe(false);
      expect(RuleUtil.canHaveTieredReplicants({ type: 'dropByInterval' })).toBe(false);
      expect(RuleUtil.canHaveTieredReplicants({ type: 'broadcastForever' })).toBe(false);
    });
  });

  describe('renameTieredReplicant', () => {
    it('renames a tier', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 2, cold: 1 },
      };
      const newRule = RuleUtil.renameTieredReplicant(rule, 'hot', 'warm');
      expect(newRule).toEqual({
        type: 'loadForever',
        tieredReplicants: { warm: 2, cold: 1 },
      });
    });

    it('avoids conflict when renaming to existing tier', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 2, cold: 1 },
      };
      const newRule = RuleUtil.renameTieredReplicant(rule, 'hot', 'cold');
      expect(newRule).toEqual({
        type: 'loadForever',
        tieredReplicants: { 'cold': 1, 'cold+': 2 },
      });
    });

    it('does nothing when old tier does not exist', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 2 },
      };
      const newRule = RuleUtil.renameTieredReplicant(rule, 'nonexistent', 'warm');
      expect(newRule).toEqual(rule);
    });

    it('handles undefined tieredReplicants', () => {
      const rule: Rule = {
        type: 'loadForever',
      };
      const newRule = RuleUtil.renameTieredReplicant(rule, 'hot', 'warm');
      expect(newRule).toEqual(rule);
    });
  });

  describe('addTieredReplicant', () => {
    it('adds a new tier', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.addTieredReplicant(rule, 'cold', 2);
      expect(newRule).toEqual({
        type: 'loadForever',
        tieredReplicants: { hot: 1, cold: 2 },
      });
    });

    it('updates existing tier', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 1 },
      };
      const newRule = RuleUtil.addTieredReplicant(rule, 'hot', 3);
      expect(newRule).toEqual({
        type: 'loadForever',
        tieredReplicants: { hot: 3 },
      });
    });

    it('sets tier to 0 when replication is 0', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 1, cold: 2 },
      };
      const newRule = RuleUtil.addTieredReplicant(rule, 'hot', 0);
      expect(newRule).toEqual({
        type: 'loadForever',
        tieredReplicants: { hot: 0, cold: 2 },
      });
    });

    it('creates tieredReplicants when undefined', () => {
      const rule: Rule = {
        type: 'loadForever',
      };
      const newRule = RuleUtil.addTieredReplicant(rule, 'hot', 1);
      expect(newRule).toEqual({
        type: 'loadForever',
        tieredReplicants: { hot: 1 },
      });
    });
  });

  describe('totalReplicas', () => {
    it('calculates total replicas', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 2, cold: 3, archive: 1 },
      };
      expect(RuleUtil.totalReplicas(rule)).toBe(6);
    });

    it('returns 0 for empty tieredReplicants', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: {},
      };
      expect(RuleUtil.totalReplicas(rule)).toBe(0);
    });

    it('returns 0 for undefined tieredReplicants', () => {
      const rule: Rule = {
        type: 'loadForever',
      };
      expect(RuleUtil.totalReplicas(rule)).toBe(0);
    });

    it('returns 0 for non-load rules', () => {
      const rule: Rule = {
        type: 'dropForever',
      };
      expect(RuleUtil.totalReplicas(rule)).toBe(0);
    });
  });

  describe('isZeroReplicaRule', () => {
    it('returns true for load rule with zero replicas', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: {},
      };
      expect(RuleUtil.isZeroReplicaRule(rule)).toBe(true);
    });

    it('returns false for load rule with replicas', () => {
      const rule: Rule = {
        type: 'loadForever',
        tieredReplicants: { hot: 1 },
      };
      expect(RuleUtil.isZeroReplicaRule(rule)).toBe(false);
    });

    it('returns false for non-load rules', () => {
      const rule: Rule = {
        type: 'dropForever',
      };
      expect(RuleUtil.isZeroReplicaRule(rule)).toBe(false);
    });
  });

  describe('hasZeroReplicaRule', () => {
    it('returns true when rules contain zero replica rule', () => {
      const rules: Rule[] = [
        { type: 'loadForever', tieredReplicants: { hot: 1 } },
        { type: 'loadByPeriod', period: 'P1M', tieredReplicants: {} },
      ];
      expect(RuleUtil.hasZeroReplicaRule(rules, undefined)).toBe(true);
    });

    it('returns true when defaultRules contain zero replica rule', () => {
      const defaultRules: Rule[] = [{ type: 'loadForever', tieredReplicants: {} }];
      expect(RuleUtil.hasZeroReplicaRule(undefined, defaultRules)).toBe(true);
    });

    it('returns false when no zero replica rules', () => {
      const rules: Rule[] = [
        { type: 'loadForever', tieredReplicants: { hot: 1 } },
        { type: 'dropByPeriod', period: 'P1Y' },
      ];
      const defaultRules: Rule[] = [{ type: 'loadForever', tieredReplicants: { cold: 1 } }];
      expect(RuleUtil.hasZeroReplicaRule(rules, defaultRules)).toBe(false);
    });

    it('returns false for undefined arrays', () => {
      expect(RuleUtil.hasZeroReplicaRule(undefined, undefined)).toBe(false);
    });

    it('returns false for empty arrays', () => {
      expect(RuleUtil.hasZeroReplicaRule([], [])).toBe(false);
    });
  });
});
