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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class CaseOperatorConversion implements SqlOperatorConversion
{
  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.CASE;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );

    // coalesce and nvl are rewritten during planning as case statements
    // rewrite simple case_searched(notnull(expr1), expr1, expr2) to nvl(expr1, expr2) since the latter is vectorized
    // at the native layer
    // this conversion won't help if the condition expression is only part of then expression, like if the input
    // expression to coalesce was an expression itself, but this is better than nothing
    if (druidExpressions != null && druidExpressions.size() == 3) {
      final DruidExpression condition = druidExpressions.get(0);
      final DruidExpression thenExpression = druidExpressions.get(1);
      final DruidExpression elseExpression = druidExpressions.get(2);
      final String thenNotNull = StringUtils.format("notnull(%s)", thenExpression.getExpression());
      if (condition.getExpression().equals(thenNotNull)) {
        return DruidExpression.ofFunctionCall(
            Calcites.getColumnTypeForRelDataType(
                rexNode.getType()),
            "nvl",
            ImmutableList.of(thenExpression, elseExpression)
        );
      }
    }

    return OperatorConversions.convertDirectCall(
        plannerContext,
        rowSignature,
        rexNode,
        "case_searched"
    );
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );

    // rewrite case_searched(notnull(someColumn), then, else) into better native filters
    //    or(then, and(else, isNull(someColumn))
    if (druidExpressions != null && druidExpressions.size() == 3) {
      final DruidExpression condition = druidExpressions.get(0);
      final DruidExpression thenExpression = druidExpressions.get(1);
      final DruidExpression elseExpression = druidExpressions.get(2);
      if (condition.getExpression().startsWith("notnull") && condition.getArguments().get(0).isDirectColumnAccess()) {

        DimFilter thenFilter = null, elseFilter = null;
        final DimFilter isNull;
        if (plannerContext.isUseBoundsAndSelectors()) {
          isNull = new SelectorDimFilter(condition.getArguments().get(0).getDirectColumn(), null, null);
        } else {
          isNull = NullFilter.forColumn(condition.getArguments().get(0).getDirectColumn());
        }

        if (call.getOperands().get(1) instanceof RexCall) {
          final RexCall thenCall = (RexCall) call.getOperands().get(1);
          thenFilter = Expressions.toFilter(plannerContext, rowSignature, virtualColumnRegistry, thenCall);
        }

        if (thenFilter != null && call.getOperands().get(2) instanceof RexLiteral) {
          if (call.getOperands().get(2).isAlwaysTrue()) {
            return new OrDimFilter(thenFilter, isNull);
          } else {
            // else is always false, we can leave it out
            return thenFilter;
          }
        } else if (call.getOperands().get(2) instanceof RexCall) {
          RexCall elseCall = (RexCall) call.getOperands().get(2);
          elseFilter = Expressions.toFilter(plannerContext, rowSignature, virtualColumnRegistry, elseCall);
        }

        // if either then or else filters produced a native filter (that wasn't just another expression filter)
        // make sure we have filters for both sides by filling in the gaps with expression filter
        if (thenFilter != null && !(thenFilter instanceof ExpressionDimFilter) && elseFilter == null) {
          elseFilter = new ExpressionDimFilter(
              elseExpression.getExpression(),
              plannerContext.parseExpression(elseExpression.getExpression()),
              null
          );
        } else if (thenFilter == null && elseFilter != null && !(elseFilter instanceof ExpressionDimFilter)) {
          thenFilter = new ExpressionDimFilter(
              thenExpression.getExpression(),
              plannerContext.parseExpression(thenExpression.getExpression()),
              null
          );
        }

        if (thenFilter != null && elseFilter != null) {
          return new OrDimFilter(thenFilter, new AndDimFilter(elseFilter, isNull));
        }
      }
    }

    // no special cases (..ha ha!) so fall through to defaul thandling
    return SqlOperatorConversion.super.toDruidFilter(plannerContext, rowSignature, virtualColumnRegistry, rexNode);
  }
}
