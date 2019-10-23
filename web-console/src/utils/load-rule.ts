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

export interface Rule {
  type:
    | 'loadForever'
    | 'loadByInterval'
    | 'loadByPeriod'
    | 'dropForever'
    | 'dropByInterval'
    | 'dropByPeriod'
    | 'broadcastForever'
    | 'broadcastByInterval'
    | 'broadcastByPeriod';
  interval?: string;
  period?: string;
  tieredReplicants?: Record<string, number>;
  colocatedDataSources?: string[];
}

export type LoadType = 'load' | 'drop' | 'broadcast';
export type TimeType = 'Forever' | 'ByInterval' | 'ByPeriod';

export class RuleUtil {
  static ruleToString(rule: Rule): string {
    return (
      rule.type +
      (rule.period ? `(${rule.period})` : '') +
      (rule.interval ? `(${rule.interval})` : '')
    );
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
    const newRule = Object.assign({}, rule, { type: loadType + RuleUtil.getTimeType(rule) });
    if (loadType !== 'load') delete newRule.tieredReplicants;
    if (loadType !== 'broadcast') delete newRule.colocatedDataSources;
    return newRule;
  }

  static changeTimeType(rule: Rule, timeType: TimeType): Rule {
    const newRule = Object.assign({}, rule, { type: RuleUtil.getLoadType(rule) + timeType });
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
}
