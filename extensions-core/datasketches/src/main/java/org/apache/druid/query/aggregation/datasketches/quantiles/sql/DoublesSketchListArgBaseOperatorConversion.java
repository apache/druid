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

package org.apache.druid.query.aggregation.datasketches.quantiles.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Static;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.BasicOperandTypeChecker;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public abstract class DoublesSketchListArgBaseOperatorConversion implements SqlOperatorConversion
{
  @Override
  public SqlOperator calciteOperator()
  {
    return makeSqlFunction();
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode
  )
  {
    return null;
  }

  @Nullable
  @Override
  public PostAggregator toPostAggregator(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final double[] args = new double[operands.size() - 1];
    final PostAggregator inputSketchPostAgg = OperatorConversions.toPostAggregator(
        plannerContext,
        rowSignature,
        operands.get(0),
        postAggregatorVisitor,
        true
    );

    if (inputSketchPostAgg == null) {
      return null;
    }

    for (int i = 1; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      double arg = ((Number) RexLiteral.value(operand)).doubleValue();
      args[i - 1] = arg;
    }

    return makePostAgg(
        postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
        inputSketchPostAgg,
        args
    );
  }

  private SqlFunction makeSqlFunction()
  {
    return new SqlFunction(
        getFunctionName(),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(
            factory -> Calcites.createSqlType(factory, SqlTypeName.OTHER)
        ),
        null,
        new DoublesSketchListArgOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
  }

  public abstract String getFunctionName();

  public abstract PostAggregator makePostAgg(
      String name,
      PostAggregator field,
      double[] args
  );

  /**
   * Minimum 2 arguments. 2nd and further arguments must be literal numbers.
   */
  private static class DoublesSketchListArgOperandTypeChecker implements SqlOperandTypeChecker
  {
    private static final int REQUIRED_OPERANDS = 2;

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
    {
      for (int i = 1; i < callBinding.operands().size(); i++) {
        final SqlNode operand = callBinding.operands().get(i);
        final RelDataType operandType = callBinding.getValidator().deriveType(callBinding.getScope(), operand);

        // Verify that 'operand' is a literal number.
        if (!SqlUtil.isLiteral(operand)) {
          return BasicOperandTypeChecker.throwOrReturn(
              throwOnFailure,
              callBinding,
              cb -> cb.getValidator()
                      .newValidationError(
                          operand,
                          Static.RESOURCE.argumentMustBeLiteral(callBinding.getOperator().getName())
                      )
          );
        }

        if (!SqlTypeFamily.NUMERIC.contains(operandType)) {
          return BasicOperandTypeChecker.throwOrReturn(
              throwOnFailure,
              callBinding,
              SqlCallBinding::newValidationSignatureError
          );
        }
      }

      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange()
    {
      return SqlOperandCountRanges.from(REQUIRED_OPERANDS);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
      return StringUtils.format("'%s(sketch, arg1, [arg2, ...])'", opName);
    }

    @Override
    public Consistency getConsistency()
    {
      return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i)
    {
      return i + 1 > REQUIRED_OPERANDS;
    }
  }
}
