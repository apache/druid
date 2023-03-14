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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

/**
 * Rule that pulls a {@link Filter} from the right-hand side of a {@link Correlate} above the Correlate in presence of an unneeded Project
 * Allows filters on unnested fields to be added to queries that use {@link org.apache.druid.query.UnnestDataSource}.
 *
 * @see CorrelateFilterRTransposeRule similar, but for without a Project atop Filter
 */
public class CorrelateProjectOnFIlterRightTransposeRule extends RelOptRule
{
  private static final CorrelateProjectOnFIlterRightTransposeRule INSTANCE = new CorrelateProjectOnFIlterRightTransposeRule();

  public CorrelateProjectOnFIlterRightTransposeRule()
  {
    super(
        operand(
            Correlate.class,
            operand(RelNode.class, any()),
            operand(Project.class, operand(Filter.class, operand(Uncollect.class, any())))
        ));
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final Filter right = call.rel(3);

    // Can't pull up filters that explicitly refer to the correlation variable.
    return !CorrelateFilterRTransposeRule.usesCorrelationId(correlate.getCorrelationId(), right.getCondition());
  }

  public static CorrelateProjectOnFIlterRightTransposeRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final RelNode left = call.rel(1);
    final Project rightP = call.rel(2);
    final Filter rightF = call.rel(3);
    final Uncollect uncollect = call.rel(4);

    // The project is top of Uncollect and can only refer to the unnested output column
    // the project can be a cast if on a string column
    // e.g. LogicalProject(subset=[rel#123:Subset#5.NONE.[]], d3=[CAST('b':VARCHAR):VARCHAR])
    // Or for numeric columns takes the shape
    // e.g. LogicalProject(subset=[rel#126:Subset#5.NONE.[]], d3=[1.0E0:FLOAT])
    // The projection is always bound to be on a single column reference which will be output of the uncollect
    // So we check if there is only a single projection and is it either a CAST or a LITERAL

    final SqlKind rightProjectKind = rightP.getChildExps().get(0).getKind();
    if (rightP.getProjects().size() == 1 && (rightProjectKind == SqlKind.CAST || rightProjectKind == SqlKind.LITERAL)) {
      call.transformTo(
          call.builder()
              .push(correlate.copy(correlate.getTraitSet(), ImmutableList.of(left, rightF.getInput())))
              .filter(RexUtil.shift(rightF.getCondition(), left.getRowType().getFieldCount()))
              .build()
      );
    }
  }
}
