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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.table.RowSignature;

import java.math.BigDecimal;
import java.util.Arrays;
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

        final ExprEval exprResult = expr.eval(
            name -> {
              // Sanity check. Bindings should not be used for a constant expression.
              throw new UnsupportedOperationException();
            }
        );

        final RexNode literal;

        if (sqlTypeName == SqlTypeName.BOOLEAN) {
          literal = rexBuilder.makeLiteral(exprResult.asBoolean(), constExp.getType(), true);
        } else if (sqlTypeName == SqlTypeName.DATE) {
          // It is possible for an expression to have a non-null String value but it can return null when parsed
          // as a primitive long/float/double.
          // ExprEval.isNumericNull checks whether the parsed primitive value is null or not.
          if (!constExp.getType().isNullable() && exprResult.isNumericNull()) {
            throw new IAE("Illegal DATE constant: %s", constExp);
          }

          literal = rexBuilder.makeDateLiteral(
              Calcites.jodaToCalciteDateString(
                  DateTimes.utc(exprResult.asLong()),
                  plannerContext.getTimeZone()
              )
          );
        } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
          // It is possible for an expression to have a non-null String value but it can return null when parsed
          // as a primitive long/float/double.
          // ExprEval.isNumericNull checks whether the parsed primitive value is null or not.
          if (!constExp.getType().isNullable() && exprResult.isNumericNull()) {
            throw new IAE("Illegal TIMESTAMP constant: %s", constExp);
          }

          literal = rexBuilder.makeTimestampLiteral(
              Calcites.jodaToCalciteTimestampString(
                  DateTimes.utc(exprResult.asLong()),
                  plannerContext.getTimeZone()
              ),
              RelDataType.PRECISION_NOT_SPECIFIED
          );
        } else if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
          final BigDecimal bigDecimal;

          if (exprResult.type() == ExprType.LONG) {
            bigDecimal = BigDecimal.valueOf(exprResult.asLong());
          } else {
            bigDecimal = BigDecimal.valueOf(exprResult.asDouble());
          }

          literal = rexBuilder.makeLiteral(bigDecimal, constExp.getType(), true);
        } else if (sqlTypeName == SqlTypeName.ARRAY) {
          assert exprResult.isArray();
          literal = rexBuilder.makeLiteral(Arrays.asList(exprResult.asArray()), constExp.getType(), true);
        } else {
          literal = rexBuilder.makeLiteral(exprResult.value(), constExp.getType(), true);
        }

        reducedValues.add(literal);
      }
    }
  }
}
