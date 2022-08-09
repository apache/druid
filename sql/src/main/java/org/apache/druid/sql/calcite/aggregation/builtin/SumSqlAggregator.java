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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;

public class SumSqlAggregator extends SimpleSqlAggregator
{
  /**
   * We are using a custom SUM function instead of {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#SUM} to
   * work around the issue described in https://issues.apache.org/jira/browse/CALCITE-4609. Once we upgrade Calcite
   * to 1.27.0+ we can return to using the built-in SUM function, and {@link DruidSumAggFunction and
   * {@link DruidSumSplitter} can be removed.
   */
  private static final SqlAggFunction DRUID_SUM = new DruidSumAggFunction();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return DRUID_SUM;
  }

  @Override
  Aggregation getAggregation(
      final String name,
      final AggregateCall aggregateCall,
      final ExprMacroTable macroTable,
      final String fieldName
  )
  {
    final ColumnType valueType = Calcites.getColumnTypeForRelDataType(aggregateCall.getType());
    if (valueType == null) {
      return null;
    }
    return Aggregation.create(createSumAggregatorFactory(valueType.getType(), name, fieldName, macroTable));
  }

  static AggregatorFactory createSumAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongSumAggregatorFactory(name, fieldName, null, macroTable);
      case FLOAT:
        return new FloatSumAggregatorFactory(name, fieldName, null, macroTable);
      case DOUBLE:
        return new DoubleSumAggregatorFactory(name, fieldName, null, macroTable);
      default:
        throw new UnsupportedSQLQueryException("Sum aggregation is not supported for '%s' type", aggregationType);
    }
  }

  /**
   * Customized verison of {@link org.apache.calcite.sql.fun.SqlSumAggFunction} with a customized
   * implementation of {@link #unwrap(Class)} to provide a customized {@link SqlSplittableAggFunction} that correctly
   * honors Druid's type system. The default sum implementation of {@link SqlSplittableAggFunction} assumes that it can
   * reduce its output to its input in the case of a single row, which means that it doesn't necessarily reflect the
   * output type as if it were run through the SUM function (e.g. INTEGER -> BIGINT)
   */
  private static class DruidSumAggFunction extends SqlAggFunction
  {
    public DruidSumAggFunction()
    {
      super(
          "SUM",
          null,
          SqlKind.SUM,
          ReturnTypes.AGG_SUM,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }

    @Override
    public <T> T unwrap(Class<T> clazz)
    {
      if (clazz == SqlSplittableAggFunction.class) {
        return clazz.cast(DruidSumSplitter.INSTANCE);
      }
      return super.unwrap(clazz);
    }
  }

  /**
   * The default sum implementation of {@link SqlSplittableAggFunction} assumes that it can reduce its output to its
   * input in the case of a single row for the {@link #singleton(RexBuilder, RelDataType, AggregateCall)} method, which
   * is fine for the default type system where the output type of SUM is the same numeric type as the inputs, but
   * Druid SUM always produces DOUBLE or BIGINT, so this is incorrect for
   * {@link org.apache.druid.sql.calcite.planner.DruidTypeSystem}.
   */
  private static class DruidSumSplitter extends SqlSplittableAggFunction.AbstractSumSplitter
  {
    public static DruidSumSplitter INSTANCE = new DruidSumSplitter();

    @Override
    public RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType, AggregateCall aggregateCall)
    {
      final int arg = aggregateCall.getArgList().get(0);
      final RelDataTypeField field = inputRowType.getFieldList().get(arg);
      final RexNode inputRef = rexBuilder.makeInputRef(field.getType(), arg);
      // if input and output do not aggree, we must cast the input to the output type
      if (!aggregateCall.getType().equals(field.getType())) {
        return rexBuilder.makeCast(aggregateCall.getType(), inputRef);
      }
      return inputRef;
    }

    @Override
    protected SqlAggFunction getMergeAggFunctionOfTopSplit()
    {
      return DRUID_SUM;
    }
  }
}
