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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidFreeUnionDataSourceRel;
import org.apache.druid.sql.calcite.rel.DruidRel;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Creates a {@link DruidFreeUnionDataSourceRel} from various {@link DruidRel} inputs that represents ad hoc scans
 *
 */
public class DruidFreeUnionDataSourceRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidFreeUnionDataSourceRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            Union.class,
            operand(DruidRel.class, none()),
            operand(DruidRel.class, none())
        )
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel<?> firstDruidRel = call.rel(1);
    final DruidRel<?> secondDruidRel = call.rel(2);

    return isCompatible(unionRel, firstDruidRel, secondDruidRel, plannerContext);
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel<?> firstDruidRel = call.rel(1);
    final DruidRel<?> secondDruidRel = call.rel(2);

    if (firstDruidRel instanceof DruidFreeUnionDataSourceRel) {
      // Unwrap and flatten the inputs to the Union.
      final RelNode newUnionRel = call.builder()
                                      .pushAll(firstDruidRel.getInputs())
                                      .push(secondDruidRel)
                                      .union(true, firstDruidRel.getInputs().size() + 1)
                                      .build();

      call.transformTo(
          DruidFreeUnionDataSourceRel.create(
              (Union) newUnionRel,
//              getColumnNames(firstDruidRel, plannerContext).get(),
              unionRel.getRowType().getFieldNames(),
              firstDruidRel.getPlannerContext()
          )
      );
    } else {
      call.transformTo(
          DruidFreeUnionDataSourceRel.create(
              unionRel,
//              getColumnNames(firstDruidRel, plannerContext).get(),
              unionRel.getRowType().getFieldNames(),
              firstDruidRel.getPlannerContext()
          )
      );
    }
  }

  // Can only do UNION ALL of inputs that have compatible schemas (or schema mappings) and right side
  // is a simple table scan
  public static boolean isCompatible(
      final Union unionRel,
      final DruidRel<?> first,
      final DruidRel<?> second,
      @Nullable PlannerContext plannerContext
  )
  {
    if (second instanceof DruidFreeUnionDataSourceRel) {
      return false;
    }

    if (!unionRel.all && null != plannerContext) {
      plannerContext.setPlanningError("SQL requires 'UNION' but only 'UNION ALL' is supported.");
    }
//    return unionRel.all && isUnionCompatible(first, second, plannerContext);
    return unionRel.all;
  }

  /**
   * For the current implementation the union is compatible iff the column's data types column and names are same
   */
  private static boolean isUnionCompatible(
      final DruidRel<?> first,
      final DruidRel<?> second,
      @Nullable PlannerContext plannerContext
  )
  {
    final Optional<List<String>> firstColumnNames = getColumnNames(first, plannerContext);
    final Optional<List<String>> secondColumnNames = getColumnNames(second, plannerContext);
    if (!firstColumnNames.isPresent() || !secondColumnNames.isPresent()) {
      // No need to set the planning error here
      return false;
    }
    if (!firstColumnNames.equals(secondColumnNames)) {
      if (null != plannerContext) {
        plannerContext.setPlanningError(
            "SQL requires union between two tables and column names queried for " +
            "each table are different Left: %s, Right: %s.",
            firstColumnNames.orElse(Collections.emptyList()),
            secondColumnNames.orElse(Collections.emptyList())
        );
      }
      return false;
    }
    return true;
  }

  static Optional<List<String>> getColumnNames(final DruidRel<?> druidRel, @Nullable PlannerContext plannerContext)
  {
    return Optional.of(druidRel.getRowType().getFieldNames());
  }
}
