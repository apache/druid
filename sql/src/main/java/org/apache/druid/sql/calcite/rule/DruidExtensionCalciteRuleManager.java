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

package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptRule;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;
import java.util.Set;

/**
 * Manages the custom calcite rules coming from extensions
 */
public class DruidExtensionCalciteRuleManager
{
  private final Set<DruidExtensionCalciteRuleProvider> druidExtensionCalciteRuleProviderSet;

  @Inject
  public DruidExtensionCalciteRuleManager(
      Set<DruidExtensionCalciteRuleProvider> druidExtensionCalciteRuleProviderSet
  )
  {
    this.druidExtensionCalciteRuleProviderSet = druidExtensionCalciteRuleProviderSet;
  }

  public List<RelOptRule> updateDruidConventionRuleSet(PlannerContext plannerContext, List<RelOptRule> coreRules)
  {
    ImmutableList.Builder<RelOptRule> updatedRulesBuilder = ImmutableList.builder();
    updatedRulesBuilder.addAll(coreRules);
    for (DruidExtensionCalciteRuleProvider druidExtensionCalciteRuleProvider : druidExtensionCalciteRuleProviderSet) {
      updatedRulesBuilder.add(druidExtensionCalciteRuleProvider.getRule(plannerContext));
    }
    return updatedRulesBuilder.build();
  }
}
