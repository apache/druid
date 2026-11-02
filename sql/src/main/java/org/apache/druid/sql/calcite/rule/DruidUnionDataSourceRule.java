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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidRels;
import org.apache.druid.sql.calcite.rel.DruidUnionDataSourceRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import javax.annotation.Nullable;

/**
 * Creates a {@link DruidUnionDataSourceRel} from various {@link DruidQueryRel} inputs that represent simple
 * table scans.
 */
public class DruidUnionDataSourceRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidUnionDataSourceRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            Union.class,
            operand(DruidRel.class, none()),
            operand(DruidQueryRel.class, none())
        )
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel<?> firstDruidRel = call.rel(1);
    final DruidQueryRel secondDruidRel = call.rel(2);

    return isCompatible(unionRel, firstDruidRel, secondDruidRel, plannerContext);
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel<?> firstDruidRel = call.rel(1);
    final DruidQueryRel secondDruidRel = call.rel(2);

    if (firstDruidRel instanceof DruidUnionDataSourceRel) {
      // Unwrap and flatten the inputs to the Union.
      final RelNode newUnionRel = call.builder()
                                      .pushAll(firstDruidRel.getInputs())
                                      .push(secondDruidRel)
                                      .union(true, firstDruidRel.getInputs().size() + 1)
                                      .build();

      call.transformTo(DruidUnionDataSourceRel.create((Union) newUnionRel, firstDruidRel.getPlannerContext()));
    } else {
      // Sanity check.
      if (!(firstDruidRel instanceof DruidQueryRel)) {
        throw new ISE("Expected first rel to be a DruidQueryRel, but it was %s", firstDruidRel.getClass().getName());
      }

      call.transformTo(DruidUnionDataSourceRel.create(unionRel, firstDruidRel.getPlannerContext()));
    }
  }

  /**
   * Whether a UNION ALL of these two inputs can be a {@link DruidUnionDataSourceRel}. Both must read a table and do
   * nothing to it beyond filtering and projecting, which {@link DruidUnionDataSourceRel} represents with
   * {@link org.apache.druid.query.FilteredDataSource} and {@link org.apache.druid.query.ProjectDataSource}. Column
   * names do not have to line up: UNION ALL matches columns by position, and each arm projects to the names the
   * union outputs.
   */
  public static boolean isCompatible(
      final Union unionRel,
      final DruidRel<?> first,
      final DruidRel<?> second,
      @Nullable PlannerContext plannerContext
  )
  {
    if (!(second instanceof DruidQueryRel)) {
      return false;
    }

    if (!unionRel.all) {
      return false;
    }

    return isUnionArm(first, plannerContext) && isUnionArm(second, plannerContext);
  }

  /**
   * Whether a single input can be an arm of a {@link DruidUnionDataSourceRel}.
   */
  static boolean isUnionArm(final DruidRel<?> druidRel, @Nullable PlannerContext plannerContext)
  {
    // A union input is flattened into the new union.
    final boolean isUnion = druidRel instanceof DruidUnionDataSourceRel;

    final boolean isTable =
        DruidRels.druidTableIfLeafRel(druidRel)
                 .filter(table -> table.getDataSource() instanceof TableDataSource)
                 .isPresent();

    if (!isUnion && !isTable) {
      if (null != plannerContext) {
        plannerContext.setPlanningError(
            "Query requires union with an input that reads from something other than a regular table, "
            + "which is not supported."
        );
      }
      return false;
    }

    // Flattening a union discards anything layered on top of it, so there must be nothing.
    final boolean isSimple = isUnion
                             ? druidRel.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SCAN
                             : DruidRels.isScanOrFilterOrProject(druidRel);

    if (!isSimple) {
      if (null != plannerContext) {
        plannerContext.setPlanningError(
            "Query requires union with an input that does more than scan, filter, or project a table, "
            + "which is not supported."
        );
      }
      return false;
    }

    return true;
  }
}
