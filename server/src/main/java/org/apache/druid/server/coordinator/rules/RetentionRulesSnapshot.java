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
 * Immutable snapshot of the retention rules of every datasource.
 */
public class RetentionRulesSnapshot
{
  /**
   * Conventional value of {@code druid.manager.rules.defaultRule}, used only by
   * {@link #withClusterDefaults}. Real snapshots take the configured name from their caller,
   * as an operator may have changed it.
   */
  private static final String DEFAULT_DATASOURCE_NAME = "_default";

  // The default datasource name is immaterial when there are no rules to look it up in
  private static final RetentionRulesSnapshot EMPTY = new RetentionRulesSnapshot(Map.of(), "");

  /**
   * Rules of each datasource exactly as configured, i.e. no entry has {@link #clusterDefaultRules}
   * appended to it. The cluster defaults are still present as the entry for the default datasource
   * ({@code druid.manager.rules.defaultRule}), which is where {@link #clusterDefaultRules} comes from.
   */
  private final Map<String, List<Rule>> datasourceToRules;
  /**
   * Override rules of each datasource, already concatenated with {@link #clusterDefaultRules}.
   * Contains an entry only for datasources that have override rules and are not the default
   * datasource itself, so that everything else can share the {@link #clusterDefaultRules} instance.
   */
  private final Map<String, List<Rule>> datasourceToRulesWithDefault;
  private final List<Rule> clusterDefaultRules;

  public static RetentionRulesSnapshot empty()
  {
    return EMPTY;
  }

  /**
   * Snapshot in which the given rules are the cluster defaults and no datasource has
   * override rules, so that they apply to every datasource.
   */
  public static RetentionRulesSnapshot withClusterDefaults(List<Rule> clusterDefaultRules)
  {
    return new RetentionRulesSnapshot(
        Map.of(DEFAULT_DATASOURCE_NAME, clusterDefaultRules),
        DEFAULT_DATASOURCE_NAME
    );
  }

  /**
   * @param datasourceToRules     Rules configured for each datasource, including the entry for
   *                              {@code defaultDatasourceName}.
   * @param defaultDatasourceName Name of the datasource whose rules serve as the cluster
   *                              defaults, i.e. {@code druid.manager.rules.defaultRule}. The
   *                              cluster defaults are empty if it has no entry in
   *                              {@code datasourceToRules}.
   */
  public RetentionRulesSnapshot(Map<String, List<Rule>> datasourceToRules, String defaultDatasourceName)
  {
    this.clusterDefaultRules = List.copyOf(datasourceToRules.getOrDefault(defaultDatasourceName, List.of()));

    // Copy the rule lists as well as the map spine, so that a caller still holding one of
    // the source lists cannot mutate this snapshot.
    final Map<String, List<Rule>> rules = Maps.newHashMapWithExpectedSize(datasourceToRules.size());
    final Map<String, List<Rule>> rulesWithDefault = Maps.newHashMapWithExpectedSize(datasourceToRules.size());
    datasourceToRules.forEach((datasource, overrideRules) -> {
      rules.put(datasource, List.copyOf(overrideRules));
      // The default datasource is skipped so that its own rules are not appended to themselves.
      if (!overrideRules.isEmpty() && !datasource.equals(defaultDatasourceName)) {
        final List<Rule> combinedRules = new ArrayList<>(overrideRules.size() + this.clusterDefaultRules.size());
        combinedRules.addAll(overrideRules);
        combinedRules.addAll(this.clusterDefaultRules);
        rulesWithDefault.put(datasource, Collections.unmodifiableList(combinedRules));
      }
    });
    this.datasourceToRules = Map.copyOf(rules);
    this.datasourceToRulesWithDefault = Map.copyOf(rulesWithDefault);
  }

  /**
   * Return all rules that exist in the cluster.
   */
  public Map<String, List<Rule>> getAllRules()
  {
    return datasourceToRules;
  }

  /**
   * Override rules configured for this datasource, excluding the cluster defaults.
   * <p>
   * No cluster defaults are appended, so a datasource with no overrides returns an empty
   * list. Use {@link #getEffectiveRules} to get the rules that actually apply to its segments.
   */
  public List<Rule> getOverrideRules(String datasource)
  {
    return datasourceToRules.getOrDefault(datasource, List.of());
  }

  /**
   * All retention rules applicable to segments of this datasource.
   * The returned list contains the override rules specified for the datasource followed by the cluster default rules.
   */
  public List<Rule> getEffectiveRules(String datasource)
  {
    return datasourceToRulesWithDefault.getOrDefault(datasource, clusterDefaultRules);
  }
}
