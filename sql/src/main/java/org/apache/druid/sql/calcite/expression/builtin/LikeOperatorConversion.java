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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class LikeOperatorConversion extends DirectOperatorConversion
{
  private static final SqlOperator SQL_FUNCTION = SqlStdOperatorTable.LIKE;

  public LikeOperatorConversion()
  {
    super(SQL_FUNCTION, "like");
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
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
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        operands.get(0)
    );
    if (druidExpression == null) {
      return null;
    }

    if (druidExpression.isSimpleExtraction()) {
      return new LikeDimFilter(
          druidExpression.getSimpleExtraction().getColumn(),
          RexLiteral.stringValue(operands.get(1)),
          operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
          druidExpression.getSimpleExtraction().getExtractionFn()
      );
    } else if (virtualColumnRegistry != null) {
      VirtualColumn v = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          druidExpression,
          operands.get(0).getType().getSqlTypeName()
      );

      return new LikeDimFilter(
          v.getOutputName(),
          RexLiteral.stringValue(operands.get(1)),
          operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
          null
      );
    } else {
      return null;
    }
  }
}
