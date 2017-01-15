/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.planner;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

/**
 * Our very own subclass of CalcitePrepareImpl, used to alter behaviors of the JDBC driver as necessary.
 *
 * When Calcite 1.11.0 is released, we should override "createConvertletTable" and provide the
 * DruidConvertletTable.
 */
public class DruidPlannerImpl extends CalcitePrepareImpl
{
  private final PlannerConfig plannerConfig;

  public DruidPlannerImpl(PlannerConfig plannerConfig)
  {
    this.plannerConfig = plannerConfig;
  }

  @Override
  protected RelOptPlanner createPlanner(
      final Context prepareContext,
      final org.apache.calcite.plan.Context externalContext0,
      final RelOptCostFactory costFactory
  )
  {
    final org.apache.calcite.plan.Context externalContext = externalContext0 != null
                                                            ? externalContext0
                                                            : Contexts.of(prepareContext.config());

    final VolcanoPlanner planner = new VolcanoPlanner(costFactory, externalContext);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    // Register planner rules.
    for (RelOptRule rule : Rules.ruleSet(plannerConfig)) {
      planner.addRule(rule);
    }

    return planner;
  }

  @Override
  protected SqlRexConvertletTable createConvertletTable()
  {
    return DruidConvertletTable.instance();
  }
}
