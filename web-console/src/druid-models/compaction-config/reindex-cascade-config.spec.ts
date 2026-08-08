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
  newDataSchemaRule,
  newDeletionRule,
  newExpressionVirtualColumn,
  newIndexSpecRule,
  newPartitioningRule,
  newReindexCascadeConfig,
  summarizeRule,
  summarizeRuleProvider,
  summarizeVirtualColumns,
} from './reindex-cascade-config';

describe('reindex-cascade-config', () => {
  describe('summarizeRuleProvider', () => {
    it('returns (none) for undefined', () => {
      expect(summarizeRuleProvider(undefined)).toBe('(none)');
    });

    it('returns (no rules) for empty provider', () => {
      expect(summarizeRuleProvider({ type: 'inline' })).toBe('(no rules)');
    });

    it('summarizes partitioning rules', () => {
      expect(
        summarizeRuleProvider({
          type: 'inline',
          partitioningRules: [newPartitioningRule()],
        }),
      ).toBe('1 partitioning rule');
    });

    it('summarizes multiple rule types', () => {
      expect(
        summarizeRuleProvider({
          type: 'inline',
          partitioningRules: [newPartitioningRule(), newPartitioningRule()],
          deletionRules: [newDeletionRule()],
          indexSpecRules: [newIndexSpecRule()],
          dataSchemaRules: [newDataSchemaRule()],
        }),
      ).toBe('2 partitioning rules, 1 deletion rule, 1 index spec rule, 1 data schema rule');
    });
  });

  describe('summarizeVirtualColumns', () => {
    it('returns (none) for undefined', () => {
      expect(summarizeVirtualColumns(undefined)).toBe('(none)');
    });

    it('returns (none) for empty array', () => {
      expect(summarizeVirtualColumns([])).toBe('(none)');
    });

    it('summarizes named columns', () => {
      expect(
        summarizeVirtualColumns([
          { type: 'expression', name: 'v0', expression: 'x + 1' },
          { type: 'expression', name: 'v1', expression: 'y + 2' },
        ]),
      ).toBe('v0, v1');
    });

    it('shows (unnamed) for columns without names', () => {
      expect(summarizeVirtualColumns([{ type: 'expression', name: '', expression: '' }])).toBe(
        '(unnamed)',
      );
    });
  });

  describe('summarizeRule', () => {
    it('uses description if present', () => {
      expect(summarizeRule({ id: 'rule-1', olderThan: 'P30D', description: 'My rule' })).toBe(
        'My rule',
      );
    });

    it('falls back to id', () => {
      expect(summarizeRule({ id: 'rule-1', olderThan: 'P30D' })).toBe('rule-1');
    });
  });

  describe('newReindexCascadeConfig', () => {
    it('returns correct defaults', () => {
      const config = newReindexCascadeConfig('wikipedia');
      expect(config.type).toBe('reindexCascade');
      expect(config.dataSource).toBe('wikipedia');
      expect(config.defaultSegmentGranularity).toBe('DAY');
      expect(config.defaultPartitionsSpec).toEqual({ type: 'dynamic', maxRowsPerSegment: 5000000 });
      expect(config.ruleProvider).toEqual({ type: 'inline' });
    });
  });

  describe('factory functions', () => {
    it('newPartitioningRule returns correct defaults', () => {
      const rule = newPartitioningRule();
      expect(rule.id).toBeTruthy();
      expect(rule.olderThan).toBe('P30D');
      expect(rule.segmentGranularity).toBe('DAY');
      expect(rule.partitionsSpec.type).toBe('dynamic');
    });

    it('newDeletionRule returns correct defaults', () => {
      const rule = newDeletionRule();
      expect(rule.id).toBeTruthy();
      expect(rule.olderThan).toBe('P90D');
      expect(rule.deleteWhere).toBeDefined();
    });

    it('newIndexSpecRule returns correct defaults', () => {
      const rule = newIndexSpecRule();
      expect(rule.id).toBeTruthy();
      expect(rule.olderThan).toBe('P90D');
      expect(rule.indexSpec).toEqual({});
    });

    it('newDataSchemaRule returns correct defaults', () => {
      const rule = newDataSchemaRule();
      expect(rule.id).toBeTruthy();
      expect(rule.olderThan).toBe('P30D');
      expect(rule.queryGranularity).toBeUndefined();
    });

    it('generates unique IDs', () => {
      const rule1 = newPartitioningRule();
      const rule2 = newPartitioningRule();
      expect(rule1.id).not.toBe(rule2.id);
    });
  });

  describe('newExpressionVirtualColumn', () => {
    it('returns correct defaults', () => {
      const vc = newExpressionVirtualColumn();
      expect(vc.type).toBe('expression');
      expect(vc.name).toBe('');
      expect(vc.expression).toBe('');
    });
  });
});
