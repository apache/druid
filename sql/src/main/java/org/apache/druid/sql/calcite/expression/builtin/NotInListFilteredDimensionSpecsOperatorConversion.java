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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.List;

public class NotInListFilteredDimensionSpecsOperatorConversion implements SqlOperatorConversion
{
  private static final String FUNC_NAME = "NOT_IN_LIST_FILTER";

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(FUNC_NAME)
      .operandTypeChecker(new ListFilteredDimensionSpecsOperatorConversion.ListFilteredOperandTypeChecker())
      .functionCategory(SqlFunctionCategory.STRING)
      .returnTypeNonNull(SqlTypeName.VARCHAR)
      .build();


  @Override
  public SqlFunction calciteOperator()
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
    final RexCall call = (RexCall) rexNode;
    final DruidExpression input = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        call.getOperands().get(0)
    );

    if (input == null || !input.isDirectColumnAccess()) {
      throw new IAE("first operand expected direct column, got [%s]", input);
    }

    final List<RexNode> operands = call.getOperands();
    if (operands.size() < 2) {
      throw new IAE("list filtered dimension operator expected at least 2 args, got [%s]", operands.size());
    }

    List<String> values = new ArrayList<>();
    for (int i = 1; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (operand.getKind() != SqlKind.LITERAL) {
        throw new IAE("[%s] operand expected char literal, got [%s]", i, operand.getKind().name());
      }
      values.add(RexLiteral.stringValue(operand));
    }

    return input.map(
        simpleExtraction -> simpleExtraction.toFDSOExtraction(FUNC_NAME, values),
        expression -> StringUtils.format(
                "%s",
                input.getDirectColumn()
        )
    );
  }


}
