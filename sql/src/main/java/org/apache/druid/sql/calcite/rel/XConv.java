/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.druid.sql.calcite.rel;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.planner.PlannerContext;

public class XConv extends RexShuttle
{

  private PlannerContext plannerContext;
  private RexNode condition;
  private RelBuilder relBuilder;
  private RexExecutor executor;
  private RexSimplify rexSimplify;
  private RexBuilder rexBuilder;

  public XConv(PlannerContext plannerContext, RexNode condition)
  {
    this.plannerContext = plannerContext;
    this.condition = condition;

    executor = plannerContext.unwrap(RexExecutor.class);
    relBuilder = plannerContext.unwrap(RelBuilder.class);

    rexSimplify = plannerContext.unwrap(RexSimplify.class);

    rexBuilder = relBuilder.getRexBuilder();
    S1 s1 = new S1(rexBuilder, false, executor);
    s1.simplifyUnknownAs(condition, null);

  }

  public RexNode getCond()
  {
    return condition;

  }

  @Override
  public RexNode visitCall(RexCall call)
  {
//    Strong.isStrong(call)
    switch (call.getKind())
    {
    case IS_NOT_NULL:
    case IS_NULL:
    case IS_TRUE:
    case IS_NOT_TRUE:
    case IS_FALSE:
    case IS_NOT_FALSE:
      RexNode op = call.getOperands().get(0);
      switch (op.getKind())
      {
      case AND:
      case OR:
        call = distributive(call);
        break;
      case EQUALS:
        call = evalEquals(call, (RexCall) op);
        break;
      }
    }

    return super.visitCall(call);
  }

  private RexCall evalEquals(RexCall parentOp, RexCall op)
  {
    switch (parentOp.getKind())
    {
    case IS_TRUE:
      // op && op is not null

    case IS_NOT_NULL:
      // l is not null and r is not null
    case IS_NULL:
      // l is null or r is null

    case IS_NOT_TRUE:
      // return not(eq())
    case IS_FALSE:
      // not(eq()) && is not null
    case IS_NOT_FALSE:
      // eq() || l is null || r is null
    }
    return op;

  }

  private RexCall distributive(RexCall call)
  {
    RexCall topOp = call;
    RexCall bottomOp = (RexCall) call.getOperands().get(0);

    List<RexNode> oldops = bottomOp.getOperands();
    List<RexNode> newOperands = new ArrayList<>();
    for (RexNode rexNode : oldops) {
      newOperands.add(rexBuilder.makeCall(topOp.getOperator(), rexNode));
    }
    return (RexCall) rexBuilder.makeCall(bottomOp.getOperator(), newOperands);

  }

  static class S1 extends RexSimplify
  {

    public S1(RexBuilder rexBuilder, boolean unknownAsFalse, RexExecutor executor)
    {
      super(rexBuilder, unknownAsFalse, executor);
    }

  }

}
