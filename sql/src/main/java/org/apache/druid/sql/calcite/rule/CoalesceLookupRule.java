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
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;

/**
 * Rule that rewrites {@code COALESCE(LOOKUP(x, 'lookupName'), 'missingValue')} to
 * {@code LOOKUP(x, 'lookupName', 'missingValue')}.
 */
public class CoalesceLookupRule extends RelOptRule implements SubstitutionRule
{
  public CoalesceLookupRule()
  {
    super(operand(RelNode.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final RelNode oldNode = call.rel(0);
    final CoalesceLookupShuttle shuttle = new CoalesceLookupShuttle(oldNode.getCluster().getRexBuilder());
    final RelNode newNode = oldNode.accept(shuttle);

    //noinspection ObjectEquality
    if (newNode != oldNode) {
      call.transformTo(newNode);
      call.getPlanner().prune(oldNode);
    }
  }

  private static class CoalesceLookupShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    public CoalesceLookupShuttle(RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call)
    {
      if (call.getKind() == SqlKind.COALESCE
          && call.getOperands().size() == 2
          && call.getOperands().get(0).isA(SqlKind.OTHER_FUNCTION)) {
        final RexCall lookupCall = (RexCall) call.getOperands().get(0);
        if (lookupCall.getOperator().equals(QueryLookupOperatorConversion.SQL_FUNCTION)
            && lookupCall.getOperands().size() == 2
            && RexUtil.isLiteral(call.getOperands().get(1), true)) {
          return rexBuilder.makeCast(
              call.getType(),
              rexBuilder.makeCall(
                  QueryLookupOperatorConversion.SQL_FUNCTION,
                  lookupCall.getOperands().get(0),
                  lookupCall.getOperands().get(1),
                  call.getOperands().get(1)
              ),
              true,
              false
          );
        }
      }

      return super.visitCall(call);
    }
  }
}
