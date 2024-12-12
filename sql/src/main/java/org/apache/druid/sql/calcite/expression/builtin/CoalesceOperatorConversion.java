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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

/**
 * Converts SQL COALESCE, NVL, IFNULL to native "coalesce" or "nvl". Native "nvl" is used when there are 2 args, just
 * because it's older and therefore the converted queries have improved backwards-compatibility.
 */
public class CoalesceOperatorConversion implements SqlOperatorConversion
{
  private final SqlFunction function;

  public CoalesceOperatorConversion(SqlFunction function)
  {
    this.function = function;
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return function;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return OperatorConversions.convertDirectCall(
        plannerContext,
        rowSignature,
        rexNode,
        getNativeFunctionName(rexNode)
    );
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpressionWithPostAggOperands(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    return OperatorConversions.convertCallWithPostAggOperands(
        plannerContext,
        rowSignature,
        rexNode,
        operands -> DruidExpression.ofFunctionCall(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            getNativeFunctionName(rexNode),
            operands
        ),
        postAggregatorVisitor
    );
  }

  /**
   * Native function to use for a particular call to COALESCE, NVL, or IFNULL. See class-level javadoc for rationale.
   */
  private static String getNativeFunctionName(final RexNode rexNode)
  {
    final RexCall call = (RexCall) rexNode;
    return call.getOperands().size() == 2 ? "nvl" : "coalesce";
  }
}
