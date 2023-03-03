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

package org.apache.druid.query.aggregation.datasketches.tuple.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchSetOpPostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public abstract class ArrayOfDoublesSketchSetBaseOperatorConversion implements SqlOperatorConversion
{
  public ArrayOfDoublesSketchSetBaseOperatorConversion()
  {
  }

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
    plannerContext.setPlanningError("%s can only be used on aggregates. " +
        "It cannot be used directly on a column or on a scalar expression.", getFunctionName());
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
    final List<PostAggregator> inputPostAggs = new ArrayList<>();
    Integer nominalEntries = null;
    Integer numberOfvalues = null;

    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      if (i == 0 && operand.isA(SqlKind.LITERAL) && SqlTypeFamily.INTEGER.contains(operand.getType())) {
        nominalEntries = RexLiteral.intValue(operand);
      } else if (i == 1 && operand.isA(SqlKind.LITERAL) && SqlTypeFamily.INTEGER.contains(operand.getType())) {
        numberOfvalues = RexLiteral.intValue(operand);
      } else {
        final PostAggregator convertedPostAgg = OperatorConversions.toPostAggregator(
            plannerContext,
            rowSignature,
            operand,
            postAggregatorVisitor,
            true
        );

        if (convertedPostAgg == null) {
          return null;
        } else {
          inputPostAggs.add(convertedPostAgg);
        }
      }
    }

    return new ArrayOfDoublesSketchSetOpPostAggregator(
        postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
        getSetOperationName(),
        nominalEntries,
        numberOfvalues,
        inputPostAggs
    );
  }

  private SqlFunction makeSqlFunction()
  {
    return new SqlFunction(
        getFunctionName(),
        SqlKind.OTHER_FUNCTION,
        ArrayOfDoublesSketchSqlOperators.RETURN_TYPE_INFERENCE,
        null,
        OperandTypes.variadic(SqlOperandCountRanges.from(2)),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
  }

  public abstract String getSetOperationName();

  public String getFunctionName()
  {
    return StringUtils.format("ARRAY_OF_DOUBLES_SKETCH_%s", getSetOperationName());
  }

}
