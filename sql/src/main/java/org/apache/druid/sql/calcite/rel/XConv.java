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

import static com.google.common.collect.Iterables.getOnlyElement;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.planner.PlannerContext;

/**
 * Partially translates 3 valued logic expressions to 2/3 valued one.
 *
 * Translates the top level of the expression tree to be evaluated in 2 valued
 * logic.
 *
 * Execution engine:
 * <ol>
 * <li>supports a limited set of expression types
 * <li>always uses {@link RexUnknownAs#FALSE}</li>
 * <li>execution of 2 valued expressions are faster</li>
 * <li>can handle 3 valued logic inside a IS [NOT] (FALSE|TRUE) marker
 * block</li>
 * <li>doesn't need to support NOT_EQUALS</li>
 * </ol>
 */
public class XConv extends RexShuttle
{

  private PlannerContext plannerContext;
  private RexNode condition;
  private RelBuilder relBuilder;
  private RexExecutor executor;
  private RexSimplify rexSimplify;
  private RexBuilder rexBuilder;
  private RexUnknownAs unknownAs;

  public XConv(PlannerContext plannerContext, RexNode condition)
  {
    this.plannerContext = plannerContext;
    this.condition = condition;

    executor = plannerContext.unwrap(RexExecutor.class);
    relBuilder = plannerContext.unwrap(RelBuilder.class);
    rexSimplify = plannerContext.unwrap(RexSimplify.class);

    rexBuilder = relBuilder.getRexBuilder();
  }

  public RexNode getCond()
  {
    unknownAs = RexUnknownAs.FALSE;
    return condition.accept(this);
  }

  @Override
  public RexNode visitCall(RexCall call)
  {
    SqlKind kind = call.getKind();
    if (isKind(kind)) {
      SqlKind unknownAsKind = getUnknownAsKind(unknownAs);
      RexNode op = getOnlyElement(call.getOperands());

      if (kind == unknownAsKind) {
        return op.accept(this);
      }
      // if (kind == unknownAsKind.negate()) {
      // op = rexBuilder.makeCall(SqlStdOperatorTable.NOT, op);
      // return op.accept(this);
      // }

      if (isKind(op.getKind())) {
        // we have isX(isY(op))
        return isXisY(kind, op).accept(this);
      }

      switch (op.getKind())
      {
      case AND:
      case OR:
        call = distributive(call);
        return call.accept(this);
      case NOT_EQUALS:
        // case EQUALS:
        // call = evalEquals(call, (RexCall) op);
        // break;
      }
    }

    if (!isSupportedKind(kind)) {
      // FIXME: should it be wrapped or left alone?
      return call;
    }

    RexUnknownAs oldUnknownAs = unknownAs;
    try {

      switch (call.getKind())
      {
      case NOT:
      {
        unknownAs = unknownAs.negate();
        RexNode op = getOnlyElement(call.getOperands());
        RexNode newOp = op.accept(this);
        if (op == newOp) {
          return call;
        }
        return rexBuilder.makeCall(SqlStdOperatorTable.NOT, newOp);
      }
      case IS_NOT_NULL:
      case IS_NULL:
      {
        unknownAs = unknownAs.negate();
        RexNode op = getOnlyElement(call.getOperands());
        RexNode newOp = op.accept(this);
        if (op == newOp) {
          return call;
        }
        return rexBuilder.makeCall(call.getOperator(), newOp);
      }
      case EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      {
        if (unknownAs == RexUnknownAs.FALSE || kind != SqlKind.EQUALS) {
          RexNode newCall = super.visitCall(call);
          return call == newCall ? call : newCall;
        } else {
          List<RexNode> ops = new ArrayList<>();
          List<RexNode> newOperands = new ArrayList<>();
          for (RexNode op : call.getOperands()) {
            RexNode newOp = op.accept(this);
            newOperands.add(newOp);
            if (RexUtil.isLiteral(newOp, true)) {
              // FIXME: handle null or not?
              continue;
            }
            ops.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, newOp));
          }
          ops.add(rexBuilder.makeCall(call.getOperator(), newOperands));
          return RexUtil.composeDisjunction(rexBuilder, ops);
        }
      }
      // case EQUAL:
      case NOT_EQUALS:
      {
        RexCall newCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.NOT,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, call.getOperands()));
        return visitCall(newCall);
      }

      case IS_FALSE:
      case IS_TRUE:
      {
        RexNode op = getOnlyElement(call.getOperands());
        unknownAs = kind == SqlKind.IS_TRUE ? RexUnknownAs.FALSE : RexUnknownAs.TRUE;
        RexNode literal = rexBuilder.makeLiteral(kind == SqlKind.IS_TRUE);
        op = op.accept(this);

        RexNode eq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, op, literal);
        return eq;
      }
      case IS_NOT_FALSE:
      case IS_NOT_TRUE:
      {
        RexNode op = getOnlyElement(call.getOperands());
        op=rexBuilder.makeCall(
            kind==SqlKind.IS_NOT_TRUE?SqlStdOperatorTable.IS_TRUE:SqlStdOperatorTable.IS_FALSE,
            op);
        op=rexBuilder.makeCall(SqlStdOperatorTable.NOT, op);
        return visitCall((RexCall) op);
      }


      //      case IS_NOT_FALSE:
//      case IS_NOT_TRUE:
//      {
//
//        // RexNode op = getOnlyElement(call.getOperands());
//        //
//        // op = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, op,
//        // rexBuilder.makeNullLiteral(op.getType()));
//      }
      case AND:
      case OR:
        return super.visitCall(call);

      default:
        throw new RuntimeException("unhandled: " + kind);
      }
    } finally {
      unknownAs = oldUnknownAs;
    }
  }

  /**
   * {@link SqlKind}-s supported by the execution layer as indexes/filters/etc.
   */
  private boolean isSupportedKind(SqlKind kind)
  {
    switch (kind)
    {
    case AND:
    case OR:
    case IS_NULL:
    case IS_NOT_NULL:
    case NOT_EQUALS:
    case EQUALS:
    case NOT:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      return true;
    default:
      return false;
    }

  }

  private RexNode isXisY(SqlKind topKind, RexNode op)
  {
    switch (topKind)
    {
    case IS_NOT_NULL:
      return rexBuilder.makeLiteral(true);
    case IS_NULL:
      return rexBuilder.makeLiteral(false);
    case IS_TRUE:
    case IS_NOT_FALSE:
      return op;
    case IS_NOT_TRUE:
    case IS_FALSE:
      return rexBuilder.makeCall(SqlStdOperatorTable.NOT, op);
    default:
      throw new RuntimeException();
    }
  }

  private boolean isKind(SqlKind kind)
  {
    switch (kind)
    {
    case IS_NOT_NULL:
    case IS_NULL:
    case IS_TRUE:
    case IS_NOT_TRUE:
    case IS_FALSE:
    case IS_NOT_FALSE:
      return true;
    default:
      return false;
    }
  }

  private static SqlKind getUnknownAsKind(RexUnknownAs unknownAs)
  {
    switch (unknownAs)
    {
    case TRUE:
      return SqlKind.IS_NOT_FALSE;
    case FALSE:
      return SqlKind.IS_TRUE;
    default:
      throw new RuntimeException("invalid");
    }
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
}
