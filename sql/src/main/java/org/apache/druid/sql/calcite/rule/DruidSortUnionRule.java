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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidUnionRel;
import org.apache.druid.sql.calcite.run.EngineFeature;

import java.util.Collections;

/**
 * Rule that pushes LIMIT and OFFSET into a {@link DruidUnionRel}.
 */
public class DruidSortUnionRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidSortUnionRule(PlannerContext plannerContext)
  {
    super(operand(Sort.class, operand(DruidUnionRel.class, any())));
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    // Defensive check. If the planner disallows top level union all, then the DruidUnionRule would have prevented
    // creating the DruidUnionRel in the first place
    if (!plannerContext.featureAvailable(EngineFeature.ALLOW_TOP_LEVEL_UNION_ALL)) {
      plannerContext.setPlanningError(
          "Top level 'UNION ALL' is unsupported by SQL engine [%s].",
          plannerContext.getEngine().name()
      );
      return false;
    }

    // LIMIT, no ORDER BY
    final Sort sort = call.rel(0);
    return sort.collation.getFieldCollations().isEmpty() && sort.fetch != null;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Sort sort = call.rel(0);
    final DruidUnionRel unionRel = call.rel(1);

    final int limit = RexLiteral.intValue(sort.fetch);
    final int offset = sort.offset != null ? RexLiteral.intValue(sort.offset) : 0;

    final DruidUnionRel newUnionRel = DruidUnionRel.create(
        unionRel.getPlannerContext(),
        unionRel.getRowType(),
        unionRel.getInputs(),
        unionRel.getLimit() >= 0 ? Math.min(limit + offset, unionRel.getLimit()) : limit + offset
    );

    if (offset == 0) {
      call.transformTo(newUnionRel);
    } else {
      call.transformTo(
          call.builder()
              .push(newUnionRel)
              .sortLimit(offset, -1, Collections.emptyList())
              .build()
      );
    }
  }
}
