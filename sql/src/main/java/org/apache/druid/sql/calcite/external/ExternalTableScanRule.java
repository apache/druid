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

package org.apache.druid.sql.calcite.external;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.run.EngineFeature;

/**
 * Rule that converts an {@link ExternalTableScan} to a call to {@link DruidQueryRel#scanExternal}.
 *
 * This class is exercised in CalciteInsertDmlTest but is not currently exposed to end users.
 */
public class ExternalTableScanRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public ExternalTableScanRule(final PlannerContext plannerContext)
  {
    super(operand(ExternalTableScan.class, any()));
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    if (plannerContext.engineHasFeature(EngineFeature.READ_EXTERNAL_DATA)) {
      return super.matches(call);
    } else {
      plannerContext.setPlanningError(
          "Cannot use '%s' with SQL engine '%s'.",
          ExternalOperatorConversion.FUNCTION_NAME,
          plannerContext.getEngine().name()
      );

      return false;
    }
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    if (!plannerContext.engineHasFeature(EngineFeature.READ_EXTERNAL_DATA)) {
      // Not called because "matches" returns false.
      throw new UnsupportedOperationException();
    }

    final ExternalTableScan scan = call.rel(0);
    call.transformTo(DruidQueryRel.scanExternal(scan, plannerContext));
  }
}
