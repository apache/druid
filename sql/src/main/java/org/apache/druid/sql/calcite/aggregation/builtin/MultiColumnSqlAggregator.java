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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstraction for simple multi-column post aggregators like greatest, least
 */
public abstract class MultiColumnSqlAggregator implements SqlAggregator
{
  /**
   * Useful Abstraction for passing field information to subclasses from shared parent methods
   */
  protected static class FieldInfo
  {
    String fieldName;
    String expression;

    public FieldInfo(String fieldName, String expression)
    {
      this.fieldName = fieldName;
      this.expression = expression;
    }
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    if (aggregateCall.isDistinct()) {
      return null;
    }

    final List<DruidExpression> arguments = Aggregations.getArgumentsForSimpleAggregator(
        plannerContext,
        rowSignature,
        aggregateCall,
        project
    );

    if (arguments == null) {
      return null;
    }

    final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

    final List<FieldInfo> fieldInfoList = new ArrayList<>();

    // Convert arguments to concise field information & delegate the rest to sub-classes
    for (DruidExpression argument : arguments) {
      if (argument.isDirectColumnAccess()) {
        fieldInfoList.add(new FieldInfo(argument.getDirectColumn(), null));
      } else {
        fieldInfoList.add(new FieldInfo(null, argument.getExpression()));
      }
    }

    return getAggregation(name, aggregateCall, macroTable, fieldInfoList);
  }

  abstract Aggregation getAggregation(
      String name,
      AggregateCall aggregateCall,
      ExprMacroTable macroTable,
      List<FieldInfo> fieldInfoList
  );
}
