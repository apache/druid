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

package org.apache.druid.compressedbigdecimal;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public abstract class CompressedBigDecimalSqlAggregatorBase implements SqlAggregator
{
  private final SqlAggFunction sqlAggFunction;
  private final CompressedBigDecimalAggregatorFactoryCreator factoryCreator;

  protected CompressedBigDecimalSqlAggregatorBase(
      String name,
      CompressedBigDecimalAggregatorFactoryCreator factoryCreator
  )
  {
    this.sqlAggFunction = new CompressedBigDecimalSqlAggFunction(name);
    this.factoryCreator = factoryCreator;
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return sqlAggFunction;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    if (aggregateCall.getArgList().size() < 1) {
      return null;
    }

    // fetch sum column expression
    DruidExpression sumColumn = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rowSignature,
            project,
            aggregateCall.getArgList().get(0)
        )
    );

    if (sumColumn == null) {
      return null;
    }

    String sumColumnName;

    if (sumColumn.isDirectColumnAccess()) {
      sumColumnName = sumColumn.getDirectColumn();
    } else {
      sumColumnName =
          virtualColumnRegistry.getOrCreateVirtualColumnForExpression(sumColumn, ColumnType.UNKNOWN_COMPLEX);
    }

    // check if size is provided
    Integer size = null;

    if (aggregateCall.getArgList().size() >= 2) {
      RexNode sizeArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(1)
      );

      size = ((Number) RexLiteral.value(sizeArg)).intValue();
    }

    // check if scale is provided
    Integer scale = null;

    if (aggregateCall.getArgList().size() >= 3) {
      RexNode scaleArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );

      scale = ((Number) RexLiteral.value(scaleArg)).intValue();
    }

    Boolean useStrictNumberParsing = null;

    if (aggregateCall.getArgList().size() >= 4) {
      RexNode useStrictNumberParsingArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(3)
      );

      useStrictNumberParsing = RexLiteral.booleanValue(useStrictNumberParsingArg);
    }

    // create the factory
    AggregatorFactory aggregatorFactory = factoryCreator.create(
        StringUtils.format("%s:agg", name),
        sumColumnName,
        size,
        scale,
        useStrictNumberParsing
    );

    return Aggregation.create(ImmutableList.of(aggregatorFactory), null);
  }

  private static class CompressedBigDecimalSqlAggFunction extends SqlAggFunction
  {
    private CompressedBigDecimalSqlAggFunction(String name)
    {
      super(
          name,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR),
          null,
          OperandTypes.or(
              // first signature is the colum only, BIG_SUM(column)
              OperandTypes.and(OperandTypes.ANY, OperandTypes.family(SqlTypeFamily.ANY)),
              OperandTypes.and(
                  OperandTypes.sequence(
                      "'" + name + "'(column, size)",
                      OperandTypes.ANY,
                      OperandTypes.POSITIVE_INTEGER_LITERAL
                  ),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.EXACT_NUMERIC)
              ),
              OperandTypes.and(
                  OperandTypes.sequence(
                      "'" + name + "'(column, size, scale)",
                      OperandTypes.ANY,
                      OperandTypes.POSITIVE_INTEGER_LITERAL,
                      OperandTypes.POSITIVE_INTEGER_LITERAL
                  ),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.EXACT_NUMERIC)
              ),
              OperandTypes.and(
                  OperandTypes.sequence(
                      "'" + name + "'(column, size, scale, strictNumberParsing)",
                      OperandTypes.ANY,
                      OperandTypes.POSITIVE_INTEGER_LITERAL,
                      OperandTypes.POSITIVE_INTEGER_LITERAL,
                      OperandTypes.BOOLEAN
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.EXACT_NUMERIC,
                      SqlTypeFamily.EXACT_NUMERIC,
                      SqlTypeFamily.BOOLEAN
                  )
              )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.IGNORED
      );
    }
  }
}
