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

package org.apache.druid.query.aggregation.cardinality.accurate.sql;

import com.google.common.collect.Iterables;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.AccurateCardinalityAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class AccurateCardinalitySqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new AccurateCardinalitySQLAggFunction();
  private static final String NAME = "ACCURATE_CARDINALITY";

  @Override

  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
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
    final RexNode rexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        Iterables.getOnlyElement(aggregateCall.getArgList())
    );

    final DruidExpression input = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    if (input == null) {
      return null;
    }

    final AggregatorFactory aggregatorFactory;
    final String aggName = StringUtils.format("%s:agg", name);

    // Look for existing matching aggregatorFactory.
    for (final Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        if (factory instanceof AccurateCardinalityAggregatorFactory) {
          final AccurateCardinalityAggregatorFactory theFactory = (AccurateCardinalityAggregatorFactory) factory;

          // Check input for equivalence.
          final boolean inputMatches;
          final VirtualColumn virtualInput =
              existing.getVirtualColumns()
                      .stream()
                      .filter(virtualColumn ->
                                  virtualColumn.getOutputName().equals(theFactory.getField().getOutputName())
                      )
                      .findFirst()
                      .orElse(null);
          if (virtualInput == null) {
            if (input.isDirectColumnAccess()) {
              inputMatches =
                  input.getDirectColumn().equals(theFactory.getField().getDimension());
            } else {
              inputMatches =
                  input.getSimpleExtraction().getColumn().equals(theFactory.getField().getDimension()) &&
                  input.getSimpleExtraction().getExtractionFn().equals(theFactory.getField().getExtractionFn());
            }
          } else {
            inputMatches = ((ExpressionVirtualColumn) virtualInput).getExpression().equals(input.getExpression());
          }

          if (inputMatches) {
            // Found existing one. Use this.
            return Aggregation.create(
                theFactory
            );
          }
        }
      }
    }
    // No existing match found. Create a new one.
    final List<VirtualColumn> virtualColumns = new ArrayList<>();

    final ValueType inputType = Calcites.getValueTypeForSqlTypeName(rexNode.getType().getSqlTypeName());
    final DimensionSpec dimensionSpec;
    if (input.isDirectColumnAccess()) {
      dimensionSpec = new DefaultDimensionSpec(
          input.getSimpleExtraction().getColumn(),
          StringUtils.format("%s:%s", name, input.getSimpleExtraction().getColumn()),
          inputType
      );
    } else if (input.isSimpleExtraction()) {
      dimensionSpec = new ExtractionDimensionSpec(
          input.getSimpleExtraction().getColumn(),
          StringUtils.format("%s:%s", name, input.getSimpleExtraction().getColumn()),
          inputType,
          input.getSimpleExtraction().getExtractionFn()
      );
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          input,
          rexNode.getType().getSqlTypeName()
      );
      dimensionSpec = new DefaultDimensionSpec(virtualColumn.getOutputName(), virtualColumn.getOutputName());
      virtualColumns.add(virtualColumn);
    }

    aggregatorFactory = new AccurateCardinalityAggregatorFactory(
        aggName,
        dimensionSpec
    );

    return Aggregation.create(virtualColumns, aggregatorFactory);
  }

  private static class AccurateCardinalitySQLAggFunction extends SqlAggFunction
  {
    AccurateCardinalitySQLAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC,
          false,
          false
      );
    }
  }
}
