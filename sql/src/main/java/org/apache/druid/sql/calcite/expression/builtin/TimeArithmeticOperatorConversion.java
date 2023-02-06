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

package org.apache.druid.sql.calcite.expression.builtin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;

/**
 * Base class for a number of time arithmetic related operators.
 */
public abstract class TimeArithmeticOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperator operator;
  private final int direction;

  public TimeArithmeticOperatorConversion(final SqlOperator operator, final int direction)
  {
    this.operator = operator;
    this.direction = direction;
    Preconditions.checkArgument(direction != 0);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    if (operands.size() != 2) {
      throw new IAE("Expected 2 args, got %s", operands.size());
    }

    final RexNode leftRexNode = operands.get(0);
    final RexNode rightRexNode = operands.get(1);

    final DruidExpression leftExpr = Expressions.toDruidExpression(plannerContext, rowSignature, leftRexNode);
    final DruidExpression rightExpr = Expressions.toDruidExpression(plannerContext, rowSignature, rightRexNode);

    if (leftExpr == null || rightExpr == null) {
      return null;
    }

    final ColumnType outputType = Calcites.getColumnTypeForRelDataType(rexNode.getType());

    if (rightRexNode.getType().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
      // timestamp_expr { + | - } <interval_expr> (year-month interval)
      // Period is a value in months.
      return DruidExpression.ofExpression(
          outputType,
          DruidExpression.functionCall("timestamp_shift"),
          ImmutableList.of(
              leftExpr,
              rightExpr.map(
                  simpleExtraction -> null,
                  expression ->
                    rightRexNode.isA(SqlKind.LITERAL) ?
                    StringUtils.format("'P%sM'", RexLiteral.value(rightRexNode)) :
                    StringUtils.format("concat('P', %s, 'M')", expression)
              ),
              DruidExpression.ofLiteral(ColumnType.LONG, DruidExpression.longLiteral(direction > 0 ? 1 : -1)),
              DruidExpression.ofStringLiteral(plannerContext.getTimeZone().getID())
          )
      );
    } else if (rightRexNode.getType().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
      // timestamp_expr { + | - } <interval_expr> (day-time interval)
      // Period is a value in milliseconds. Ignore time zone.
      return DruidExpression.ofExpression(
          outputType,
          (args) -> StringUtils.format(
              "(%s %s %s)",
              args.get(0).getExpression(),
              direction > 0 ? "+" : "-",
              args.get(1).getExpression()
          ),
          ImmutableList.of(leftExpr, rightExpr)
      );
    } else if ((leftRexNode.getType().getFamily() == SqlTypeFamily.TIMESTAMP ||
        leftRexNode.getType().getFamily() == SqlTypeFamily.DATE) &&
        (rightRexNode.getType().getFamily() == SqlTypeFamily.TIMESTAMP ||
        rightRexNode.getType().getFamily() == SqlTypeFamily.DATE)) {
      // Calcite represents both TIMESTAMP - INTERVAL and TIMESTAMPDIFF (TIMESTAMP - TIMESTAMP)
      // with a MINUS_DATE operator, so we must tell which case we're in by checking the type of
      // the second argument.
      Preconditions.checkState(direction < 0, "Time arithmetic require direction < 0");
      if (call.getType().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
        return DruidExpression.ofExpression(
            outputType,
            DruidExpression.functionCall("subtract_months"),
            ImmutableList.of(
                leftExpr,
                rightExpr,
                DruidExpression.ofStringLiteral(plannerContext.getTimeZone().getID())
            )
        );
      } else {
        return DruidExpression.ofExpression(
            outputType,
            (args) -> StringUtils.format(
                "(%s %s %s)",
                args.get(0).getExpression(),
                "-",
                args.get(1).getExpression()
            ),
            ImmutableList.of(leftExpr, rightExpr)
        );
      }
    } else {
      // Shouldn't happen if subclasses are behaving.
      throw new ISE("Got unexpected type period type family[%s]", rightRexNode.getType().getFamily());
    }
  }

  public static class TimePlusIntervalOperatorConversion extends TimeArithmeticOperatorConversion
  {
    public TimePlusIntervalOperatorConversion()
    {
      super(SqlStdOperatorTable.DATETIME_PLUS, 1);
    }
  }

  public static class TimeMinusIntervalOperatorConversion extends TimeArithmeticOperatorConversion
  {
    public TimeMinusIntervalOperatorConversion()
    {
      super(SqlStdOperatorTable.MINUS_DATE, -1);
    }
  }
}
