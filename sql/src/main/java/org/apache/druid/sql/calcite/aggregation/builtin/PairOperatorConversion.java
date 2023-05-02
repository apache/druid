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

package org.apache.druid.sql.calcite.aggregation.builtin;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;

public class PairOperatorConversion implements SqlOperatorConversion
{
  public static PairOperatorConversion LEFT = new PairOperatorConversion(
      OperatorConversions
          .operatorBuilder("pair_left")
          .operandTypeChecker(RowSignatures.complexTypeChecker(StringLastAggregatorFactory.TYPE))
          .returnTypeNonNull(SqlTypeName.BIGINT)
          .build()
  );
  public static PairOperatorConversion RIGHT = new PairOperatorConversion(
      OperatorConversions
          .operatorBuilder(StringUtils.format("pair_%s", "right"))
          .operandTypeChecker(RowSignatures.complexTypeChecker(StringLastAggregatorFactory.TYPE))
          .returnTypeInference(opBinding -> {
            final RowSignatures.ComplexSqlType type = (RowSignatures.ComplexSqlType) opBinding.getOperandType(0);
            if (StringLastAggregatorFactory.TYPE.equals(type.getColumnType())) {
              return opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            }
            throw new UOE("Cannot determine return type of unknown type [%s]", type);
          })
          .build()
  );

  private final SqlFunction operator;

  private PairOperatorConversion(
      SqlFunction operator
  )
  {
    this.operator = operator;
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode
  )
  {
    return OperatorConversions.convertDirectCall(plannerContext, rowSignature, rexNode, operator.getName());
  }
}
