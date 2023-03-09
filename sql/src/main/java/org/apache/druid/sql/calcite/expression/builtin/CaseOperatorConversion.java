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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.FalseDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
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
    // this conversion wont help if the condition expression is only part of then expression, like if the input
    // expression to coalesce was an expression itself, but every little bit helps
    if (druidExpressions.size() == 3) {
      final DruidExpression conditionExpression = druidExpressions.get(0);
      final DruidExpression thenExpression = druidExpressions.get(1);
      final DruidExpression elseExpression = druidExpressions.get(2);
      final String condition = druidExpressions.get(0).getExpression();
      final String thenNotNull = StringUtils.format("notnull(%s)", thenExpression.getExpression());
      if (condition.equals(thenNotNull)) {
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
    if (druidExpressions.size() == 3) {
      final DruidExpression conditionExpression = druidExpressions.get(0);
      final DruidExpression thenExpression = druidExpressions.get(1);
      final DruidExpression elseExpression = druidExpressions.get(2);
      final String condition = druidExpressions.get(0).getExpression();
      if (condition.startsWith("notnull") && conditionExpression.getArguments().get(0).isDirectColumnAccess()) {

        DimFilter thenFilter = null, elseFilter = null;
        DimFilter isNull = new SelectorDimFilter(
            conditionExpression.getArguments().get(0).getDirectColumn(),
            null,
            null
        );

        if (call.getOperands().get(1) instanceof RexCall) {
          RexCall thenCall = (RexCall) call.getOperands().get(1);
          SqlOperatorConversion thenOperatorConversion = plannerContext.getPlannerToolbox()
                                                                            .operatorTable()
                                                                            .lookupOperatorConversion(thenCall.getOperator());

          if (thenOperatorConversion != null) {
            thenFilter = thenOperatorConversion.toDruidFilter(
                plannerContext,
                rowSignature,
                virtualColumnRegistry,
                thenCall
            );
          }
        }

        if (call.getOperands().get(2) instanceof RexLiteral && SqlTypeName.BOOLEAN.equals(((RexLiteral) call.getOperands().get(2)).getTypeName())) {
          boolean trueOrFalse = call.getOperands().get(2).isAlwaysTrue();
          if (trueOrFalse) {
            elseFilter = TrueDimFilter.instance();
          } else {
            elseFilter = FalseDimFilter.instance();
          }
        } else if (call.getOperands().get(2) instanceof RexCall) {
          RexCall elseCall = (RexCall) call.getOperands().get(2);
          SqlOperatorConversion elseOperatorConversion = plannerContext.getPlannerToolbox()
                                                                        .operatorTable()
                                                                        .lookupOperatorConversion(elseCall.getOperator());
          elseFilter = elseOperatorConversion.toDruidFilter(plannerContext, rowSignature, virtualColumnRegistry, elseCall);
        }

        if (thenFilter != null && !(thenFilter instanceof ExpressionDimFilter) && elseFilter == null) {
          elseFilter = new ExpressionDimFilter(elseExpression.getExpression(), plannerContext.getExprMacroTable());
        } else if (thenFilter == null && elseFilter != null && !(elseFilter instanceof ExpressionDimFilter)) {
          thenFilter = new ExpressionDimFilter(thenExpression.getExpression(), plannerContext.getExprMacroTable());
        }

        if (thenFilter != null && elseFilter != null) {
          OrDimFilter or = new OrDimFilter(
              thenFilter,
              new AndDimFilter(
                  elseFilter,
                  isNull
              )
          );
          return or;
        }
      }
    }

    return SqlOperatorConversion.super.toDruidFilter(plannerContext, rowSignature, virtualColumnRegistry, rexNode);
  }
}
