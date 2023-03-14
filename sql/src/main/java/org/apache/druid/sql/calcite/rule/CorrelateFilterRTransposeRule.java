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
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;

/**
 * Rule that pulls a {@link Filter} from the right-hand side of a {@link Correlate} above the Correlate.
 * Allows filters on unnested fields to be added to queries that use {@link org.apache.druid.query.UnnestDataSource}.
 *
 * @see CorrelateFilterLTransposeRule similar, but for left-hand side filters
 */
public class CorrelateFilterRTransposeRule extends RelOptRule
{
  private static final CorrelateFilterRTransposeRule INSTANCE = new CorrelateFilterRTransposeRule();

  public CorrelateFilterRTransposeRule()
  {
    super(
        operand(
            Correlate.class,
            operand(RelNode.class, any()),
            operand(Filter.class, any())
        ));
  }

  public static CorrelateFilterRTransposeRule instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final Filter right = call.rel(2);

    // Can't pull up filters that explicitly refer to the correlation variable.
    return !usesCorrelationId(correlate.getCorrelationId(), right.getCondition());
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final RelNode left = call.rel(1);
    final Filter right = call.rel(2);

    call.transformTo(
        call.builder()
            .push(correlate.copy(correlate.getTraitSet(), ImmutableList.of(left, right.getInput())))
            .filter(RexUtil.shift(right.getCondition(), left.getRowType().getFieldCount()))
            .build()
    );
  }

  /**
   * Whether an expression refers to correlation variables.
   */
  private static boolean usesCorrelationId(final CorrelationId correlationId, final RexNode rexNode)
  {
    class CorrelationVisitor extends RexVisitorImpl<Void>
    {
      private boolean found = false;

      public CorrelationVisitor()
      {
        super(true);
      }

      @Override
      public Void visitCorrelVariable(RexCorrelVariable correlVariable)
      {
        if (correlVariable.id.equals(correlationId)) {
          found = true;
        }
        return null;
      }
    }

    final CorrelationVisitor visitor = new CorrelationVisitor();
    rexNode.accept(visitor);
    return visitor.found;
  }
}
