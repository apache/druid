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

import org.apache.calcite.linq4j.Nullness;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.planner.Calcites;

import javax.annotation.Nullable;

public class SumSqlAggregator extends SimpleSqlAggregator
{
  /**
   * We use this custom aggregation function instead of builtin SqlStdOperatorTable.SUM
   * to avoid transformation to COUNT+SUM0. See CALCITE-6020 for more details.
   * It can be handled differently after CALCITE-6020 is addressed.
   */
  private static final SqlAggFunction DRUID_SUM = new SqlSumAggFunction(Nullness.castNonNull(null)) {};

  @Override
  public SqlAggFunction calciteFunction()
  {
    return DRUID_SUM;
  }

  @Override
  @Nullable
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
    return Aggregation.create(createSumAggregatorFactory(valueType, name, fieldName, macroTable));
  }

  static AggregatorFactory createSumAggregatorFactory(
      final ColumnType aggregationType,
      final String name,
      final String fieldName,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType.getType()) {
      case LONG:
        return new LongSumAggregatorFactory(name, fieldName, null, macroTable);
      case FLOAT:
        return new FloatSumAggregatorFactory(name, fieldName, null, macroTable);
      case DOUBLE:
        return new DoubleSumAggregatorFactory(name, fieldName, null, macroTable);
      default:
        throw SimpleSqlAggregator.badTypeException(fieldName, "SUM", aggregationType);
    }
  }
}
