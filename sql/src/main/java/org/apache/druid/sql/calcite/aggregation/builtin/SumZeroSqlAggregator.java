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
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.planner.Calcites;

public class SumZeroSqlAggregator extends SumSqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.SUM0;
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

    if (aggregateCall.filterArg >= 0) {
      // SQL SUM0 semantics require 0 when the filter matches nothing, but the native
      // FilteredAggregatorFactory chain outputs null for empty input. Wrap the sum in
      // an expression post-aggregator that replaces null with 0. This keeps the
      // D1 rewrite of DruidAggregateCaseToFilterRule semantically consistent with the
      // original SUM(CASE WHEN ... THEN ... ELSE 0 END).
      // Unfiltered SUM0 keeps the plain factory path to avoid widening this fix.
      final String innerName = name + ":sum";
      final AggregatorFactory innerFactory =
          createSumAggregatorFactory(valueType, innerName, fieldName, macroTable);
      return Aggregation.create(
          ImmutableList.of(innerFactory),
          new ExpressionPostAggregator(
              name,
              "nvl(\"" + innerName + "\", 0)",
              null,
              valueType,
              macroTable
          )
      );
    }

    return Aggregation.create(createSumAggregatorFactory(valueType, name, fieldName, macroTable));
  }
}
