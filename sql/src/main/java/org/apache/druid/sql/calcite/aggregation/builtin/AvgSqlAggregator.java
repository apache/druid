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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.planner.Calcites;

public class AvgSqlAggregator extends SimpleSqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.AVG;
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
    final ValueType sumType;
    // Use 64-bit sum regardless of the type of the AVG aggregator.
    if (SqlTypeName.INT_TYPES.contains(aggregateCall.getType().getSqlTypeName())) {
      sumType = ValueType.LONG;
    } else {
      sumType = ValueType.DOUBLE;
    }

    final String sumName = Calcites.makePrefixedName(name, "sum");
    final String countName = Calcites.makePrefixedName(name, "count");
    final AggregatorFactory sum = SumSqlAggregator.createSumAggregatorFactory(
        sumType,
        sumName,
        fieldName,
        expression,
        macroTable
    );

    final AggregatorFactory count = new CountAggregatorFactory(countName);

    return Aggregation.create(
        ImmutableList.of(sum, count),
        new ArithmeticPostAggregator(
            name,
            "quotient",
            ImmutableList.of(
                new FieldAccessPostAggregator(null, sumName),
                new FieldAccessPostAggregator(null, countName)
            )
        )
    );
  }
}
