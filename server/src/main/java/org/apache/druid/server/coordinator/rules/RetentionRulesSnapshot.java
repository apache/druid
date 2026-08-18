/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator.rules;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Immutable snapshot of the retention rules of every datasource, taken once at the
 * start of a coordinator run and carried in
 * {@link org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams}.
 * <p>
 * Duties must read rules from this snapshot rather than from
 * {@link org.apache.druid.metadata.MetadataRuleManager} directly. That manager swaps
 * its entire rule map as soon as a rule update is submitted, so a duty reading it
 * per segment can apply the old rules to some segments and the new rules to others
 * within a single run. The new rules are then evaluated against values the run has
 * already snapshotted from {@link org.apache.druid.server.coordinator.CoordinatorDynamicConfig},
 * notably {@code historicalTierAliases}: a rule naming a virtual tier is meaningful
 * only together with the alias config that resolves it, and a rule resolving to no
 * tier at all makes every existing replica look unwanted.
 */
public class RetentionRulesSnapshot
{
  private static final RetentionRulesSnapshot EMPTY = new RetentionRulesSnapshot(Map.of(), List.of());

  /**
   * Rules of each datasource, already concatenated with {@link #clusterDefaultRules}.
   */
  private final Map<String, List<Rule>> datasourceToRulesWithDefault;
  private final List<Rule> clusterDefaultRules;

  public static RetentionRulesSnapshot empty()
  {
    return EMPTY;
  }

  /**
   * @param datasourceToRules   Rules configured for each datasource, not including the
   *                            cluster defaults.
   * @param clusterDefaultRules Rules configured for the default datasource. These apply
   *                            to every datasource after its own rules.
   */
  public RetentionRulesSnapshot(Map<String, List<Rule>> datasourceToRules, List<Rule> clusterDefaultRules)
  {
    this.clusterDefaultRules = List.copyOf(clusterDefaultRules);

    final Map<String, List<Rule>> rulesWithDefault = Maps.newHashMapWithExpectedSize(datasourceToRules.size());
    datasourceToRules.forEach((datasource, rules) -> {
      final List<Rule> combined = new ArrayList<>(rules.size() + this.clusterDefaultRules.size());
      combined.addAll(rules);
      combined.addAll(this.clusterDefaultRules);
      rulesWithDefault.put(datasource, Collections.unmodifiableList(combined));
    });
    this.datasourceToRulesWithDefault = Map.copyOf(rulesWithDefault);
  }

  /**
   * Rules of the given datasource followed by the cluster default rules, or just the
   * cluster defaults if the datasource has no rules of its own.
   */
  public List<Rule> getRulesWithDefault(String datasource)
  {
    return datasourceToRulesWithDefault.getOrDefault(datasource, clusterDefaultRules);
  }
}
