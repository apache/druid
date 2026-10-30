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

import com.google.common.collect.Iterables;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.planner.Calcites;

import javax.annotation.Nullable;

public class SumZeroSqlAggregator extends SumSqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.SUM0;
  }

  @Override
  @Nullable
  Aggregation getAggregation(
      final String name,
      final AggregateCall aggregateCall,
      final ExprMacroTable macroTable,
      final String fieldName,
      final boolean filteredByElseZeroRewrite
  )
  {
    final ColumnType valueType = Calcites.getColumnTypeForRelDataType(aggregateCall.getType());
    if (valueType == null) {
      return null;
    }

    if (filteredByElseZeroRewrite) {
      // This SUM0 only exists because DruidAggregateCaseToFilterRule rewrote the D1 shape
      // SUM(CASE WHEN COND THEN value ELSE 0 END) into SUM0(value) FILTER (WHERE COND), and the
      // native filtered aggregator returns null when the filter matches no row, while the
      // original CASE expression returned 0. Restore that by wrapping the filtered sum in an
      // expression post-aggregator that replaces null with 0.
      //
      // The post-aggregator is created here, instead of leaving it to Aggregation.filter, on
      // purpose: the Aggregation returned by this method carries no aggregator factory, so
      // Aggregation.filter never pushes the filter down into a FilteredAggregatorFactory. The
      // resulting post-aggregator-only Aggregation is what Windowing accepts for window
      // aggregations, while the equivalent factory-based post-aggregator
      // ([expressionPostAgg, filteredAgg]) is rejected by Windowing.fromCalciteStuff.
      return Aggregation.create(
          new ExpressionPostAggregator(
              name,
              "nvl(\"" + fieldName + "\", 0)",
              null,
              valueType,
              macroTable
          )
      );
    }

    // Plain SQL SUM0(x) and SUM0(x) FILTER (WHERE ...): keep the native nullable factory, which
    // preserves the documented null-on-empty behavior for those calls. Only the D1 rewrite above
    // is required to return 0.
    return Aggregation.create(createSumAggregatorFactory(valueType, name, fieldName, macroTable));
  }
}
