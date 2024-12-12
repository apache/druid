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
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.SingleValueAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.planner.Calcites;

import javax.annotation.Nullable;

/**
 * This class serves as binding for Single Value Aggregator.
 * Returns a single value in cases of subqueries used in expressions
 */
public class SingleValueSqlAggregator extends SimpleSqlAggregator
{

  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.SINGLE_VALUE;
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
    return Aggregation.create(createSingleValueAggregatorFactory(
        valueType,
        name,
        fieldName
    ));
  }

  static AggregatorFactory createSingleValueAggregatorFactory(
      final ColumnType aggregationType,
      final String name,
      final String fieldName
  )
  {
    return new SingleValueAggregatorFactory(name, fieldName, aggregationType);
  }
}
