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

import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprType;
import io.druid.math.expr.Parser;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.List;

/**
 * A Calcite {@code RexExecutor} that reduces Calcite expressions by evaluating them using Druid's own built-in
 * expressions. This ensures that constant reduction is done in a manner consistent with the query runtime.
 */
public class DruidRexExecutor implements RexExecutor
{
  private static final RowSignature EMPTY_ROW_SIGNATURE = RowSignature.builder().build();

  private final PlannerContext plannerContext;

  public DruidRexExecutor(final PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public void reduce(
      final RexBuilder rexBuilder,
      final List<RexNode> constExps,
      final List<RexNode> reducedValues
  )
  {
    for (RexNode constExp : constExps) {
      final DruidExpression druidExpression = Expressions.toDruidExpression(
          plannerContext,
          EMPTY_ROW_SIGNATURE,
          constExp
      );

      if (druidExpression == null) {
        reducedValues.add(constExp);
      } else {
        final SqlTypeName sqlTypeName = constExp.getType().getSqlTypeName();
        final Expr expr = Parser.parse(druidExpression.getExpression(), plannerContext.getExprMacroTable());
        final ExprEval exprResult = expr.eval(Parser.withMap(ImmutableMap.of()));
        final Object literalValue;

        if (sqlTypeName == SqlTypeName.BOOLEAN) {
          literalValue = exprResult.asBoolean();
        } else if (sqlTypeName == SqlTypeName.DATE || sqlTypeName == SqlTypeName.TIMESTAMP) {
          literalValue = Calcites.jodaToCalciteCalendarLiteral(
              new DateTime(exprResult.asLong()),
              plannerContext.getTimeZone()
          );
        } else if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
          literalValue = exprResult.type() == ExprType.LONG
                         ? new BigDecimal(exprResult.asLong())
                         : new BigDecimal(exprResult.asDouble());
        } else {
          literalValue = exprResult.value();
        }

        reducedValues.add(rexBuilder.makeLiteral(literalValue, constExp.getType(), true));
      }
    }
  }
}
