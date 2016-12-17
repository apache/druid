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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;

import java.math.BigDecimal;

/**
 * Rule that applies Aggregate to Values. Currently only applies to empty Values.
 *
 * This is still useful because PruneEmptyRules doesn't handle Aggregate, which is in turn because
 * Aggregate of empty relations need some special handling: a single row will be generated, where
 * each column's value depends on the specific aggregate calls (e.g. COUNT is 0, SUM is NULL).
 * Sample query where this matters: <code>SELECT COUNT(*) FROM s.foo WHERE 1 = 0</code>.
 *
 * Can be replaced by AggregateValuesRule in Calcite 1.11.0, when released.
 */
public class AggregateValuesRule extends RelOptRule
{
  public static final AggregateValuesRule INSTANCE = new AggregateValuesRule();

  private AggregateValuesRule()
  {
    super(
        operand(Aggregate.class, null, Predicates.not(Aggregate.IS_NOT_GRAND_TOTAL),
                operand(Values.class, null, Values.IS_EMPTY, none())
        )
    );
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Values values = call.rel(1);

    final ImmutableList.Builder<RexLiteral> literals = ImmutableList.builder();

    final RexBuilder rexBuilder = call.builder().getRexBuilder();
    for (final AggregateCall aggregateCall : aggregate.getAggCallList()) {
      switch (aggregateCall.getAggregation().getKind()) {
        case COUNT:
        case SUM0:
          literals.add((RexLiteral) rexBuilder.makeLiteral(
              BigDecimal.ZERO, aggregateCall.getType(), false));
          break;

        case MIN:
        case MAX:
        case SUM:
          literals.add(rexBuilder.constantNull());
          break;

        default:
          // Unknown what this aggregate call should do on empty Values. Bail out to be safe.
          return;
      }
    }

    call.transformTo(
        LogicalValues.create(
            values.getCluster(),
            aggregate.getRowType(),
            ImmutableList.of(literals.build())
        )
    );

    // New plan is absolutely better than old plan.
    call.getPlanner().setImportance(aggregate, 0.0);
  }
}
