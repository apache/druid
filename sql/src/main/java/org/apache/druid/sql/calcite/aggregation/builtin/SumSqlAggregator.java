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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.planner.Calcites;

public class SumSqlAggregator extends SimpleSqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.SUM;
  }

  @Override
  Aggregation getAggregation(
      final String name,
      final AggregateCall aggregateCall,
      final ExprMacroTable macroTable,
      final String fieldName,
      final String expression
  )
  {
    final ValueType valueType = Calcites.getValueTypeForSqlTypeName(aggregateCall.getType().getSqlTypeName());
    return Aggregation.create(createSumAggregatorFactory(valueType, name, fieldName, expression, macroTable));
  }

  static AggregatorFactory createSumAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongSumAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatSumAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleSumAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }
}
