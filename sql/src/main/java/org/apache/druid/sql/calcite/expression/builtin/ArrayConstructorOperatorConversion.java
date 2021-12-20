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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;

public class ArrayConstructorOperatorConversion implements SqlOperatorConversion
{
  private static final SqlOperator SQL_FUNCTION = SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;

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
    // Check if array needs to be unnested
    if (plannerContext.getQueryContext()
                      .getOrDefault(
                          QueryContexts.ENABLE_UNNESTED_ARRAYS_KEY,
                          QueryContexts.DEFAULT_ENABLE_UNNESTED_ARRAYS
                      ).equals(Boolean.FALSE)) {
      List<RexNode> nodes = ((RexCall) rexNode).getOperands();
      Preconditions.checkArgument(
          nodes == null || nodes.size() != 1,
          "ARRAY[] should have exactly one argument"
      );
      if (nodes.get(0).getKind() == SqlKind.LITERAL) {
        throw new UOE("ARRAY[] support for literals not implemented");
      }
      return Expressions.toDruidExpression(plannerContext, rowSignature, nodes.get(0));
    }
    return OperatorConversions.convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> DruidExpression.of(
            null,
            DruidExpression.functionCall("array", druidExpressions)
        )
    );
  }
}
