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
   * Override rules of each datasource. Cluster level rules are present in this map against the
   * default datasource, which is where {@link #clusterDefaultRules} comes from.
   */
  private final Map<String, List<Rule>> datasourceToRules;
  /**
   * Override rules of each datasource, already concatenated with {@link #clusterDefaultRules}.
   * Contains an entry only for datasources that have override rules and are not the default
   * datasource itself, so that everything else can share the {@link #clusterDefaultRules} instance.
   */
  private final Map<String, List<Rule>> datasourceToEffectiveRules;
  private final List<Rule> clusterDefaultRules;

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
    // the source lists cannot mutate this snapshot. The effective rules of each datasource are
    // resolved here rather than in getEffectiveRules(), which is called once per used segment.
    final Map<String, List<Rule>> rules = Maps.newHashMapWithExpectedSize(datasourceToRules.size());
    final Map<String, List<Rule>> effectiveRules = Maps.newHashMapWithExpectedSize(datasourceToRules.size());
    datasourceToRules.forEach((datasource, overrideRules) -> {
      rules.put(datasource, List.copyOf(overrideRules));
      // The default datasource is skipped so that its own rules are not appended to themselves.
      if (!overrideRules.isEmpty() && !datasource.equals(defaultDatasourceName)) {
        final List<Rule> combinedRules = new ArrayList<>(overrideRules.size() + this.clusterDefaultRules.size());
        combinedRules.addAll(overrideRules);
        combinedRules.addAll(this.clusterDefaultRules);
        effectiveRules.put(datasource, Collections.unmodifiableList(combinedRules));
      }
    });
    this.datasourceToRules = Map.copyOf(rules);
    this.datasourceToEffectiveRules = Map.copyOf(effectiveRules);
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
    return datasourceToEffectiveRules.getOrDefault(datasource, clusterDefaultRules);
  }
}
