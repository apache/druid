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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;

public class SubstringOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("SUBSTRING")
      .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
      .functionCategory(SqlFunctionCategory.STRING)
      .returnTypeInference(ReturnTypes.ARG0.andThen(SqlTypeTransforms.FORCE_NULLABLE))
      .requiredOperandCount(2)
      .build();

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    // Can't simply pass-through operands, since SQL standard args don't match what Druid's expression language wants.
    // SQL is 1-indexed, Druid is 0-indexed.

    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    final RexNode inputNode = operands.get(0);
    final DruidExpression input = Expressions.toDruidExpression(plannerContext, rowSignature, inputNode);
    if (input == null) {
      return null;
    }

    final RexNode indexNode = operands.get(1);
    final Integer adjustedIndexLiteral = RexUtil.isLiteral(indexNode, true) ? RexLiteral.intValue(indexNode) - 1 : null;
    final String adjustedIndexExpr;
    if (adjustedIndexLiteral != null) {
      adjustedIndexExpr = String.valueOf(adjustedIndexLiteral);
    } else {
      final DruidExpression indexExpr = Expressions.toDruidExpression(plannerContext, rowSignature, indexNode);
      if (indexExpr == null) {
        return null;
      }
      adjustedIndexExpr = StringUtils.format("(%s - 1)", indexExpr.getExpression());
    }
    if (adjustedIndexExpr == null) {
      return null;
    }

    final RexNode lengthNode = operands.size() > 2 ? operands.get(2) : null;
    final Integer lengthLiteral =
        lengthNode != null && RexUtil.isLiteral(lengthNode, true) ? RexLiteral.intValue(lengthNode) : null;
    final String lengthExpr;
    if (lengthNode != null) {
      final DruidExpression lengthExpression = Expressions.toDruidExpression(plannerContext, rowSignature, lengthNode);
      if (lengthExpression == null) {
        return null;
      }
      lengthExpr = lengthExpression.getExpression();
    } else {
      lengthExpr = "-1";
    }

    return input.map(
        simpleExtraction -> {
          if (adjustedIndexLiteral != null && (lengthNode == null || lengthLiteral != null)) {
            return simpleExtraction.cascade(new SubstringDimExtractionFn(adjustedIndexLiteral, lengthLiteral));
          } else {
            return null;
          }
        },
        expression -> StringUtils.format(
            "substring(%s, %s, %s)",
            expression,
            adjustedIndexExpr,
            lengthExpr
        )
    );
  }
}
