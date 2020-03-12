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

package org.apache.druid.query.aggregation.bloom.sql;

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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.bloom.BloomFilterAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class BloomFilterSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new BloomFilterSqlAggFunction();
  private static final String NAME = "BLOOM_FILTER";

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
    final RexNode inputOperand = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(0)
    );
    final DruidExpression input = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        inputOperand
    );
    if (input == null) {
      return null;
    }

    final AggregatorFactory aggregatorFactory;
    final String aggName = StringUtils.format("%s:agg", name);
    final RexNode maxNumEntriesOperand = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(1)
    );

    if (!maxNumEntriesOperand.isA(SqlKind.LITERAL)) {
      // maxNumEntriesOperand must be a literal in order to plan.
      return null;
    }

    final int maxNumEntries = ((Number) RexLiteral.value(maxNumEntriesOperand)).intValue();

    // Look for existing matching aggregatorFactory.
    for (final Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        if (factory instanceof BloomFilterAggregatorFactory) {
          final BloomFilterAggregatorFactory theFactory = (BloomFilterAggregatorFactory) factory;

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

          final boolean matches = inputMatches && theFactory.getMaxNumEntries() == maxNumEntries;

          if (matches) {
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

    ValueType valueType = Calcites.getValueTypeForSqlTypeName(inputOperand.getType().getSqlTypeName());
    final DimensionSpec spec;
    if (input.isDirectColumnAccess()) {
      spec = new DefaultDimensionSpec(
          input.getSimpleExtraction().getColumn(),
          StringUtils.format("%s:%s", name, input.getSimpleExtraction().getColumn()),
          valueType
      );
    } else if (input.isSimpleExtraction()) {
      spec = new ExtractionDimensionSpec(
          input.getSimpleExtraction().getColumn(),
          StringUtils.format("%s:%s", name, input.getSimpleExtraction().getColumn()),
          valueType,
          input.getSimpleExtraction().getExtractionFn()
      );
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          input,
          inputOperand.getType().getSqlTypeName()
      );
      virtualColumns.add(virtualColumn);
      spec = new DefaultDimensionSpec(
          virtualColumn.getOutputName(),
          StringUtils.format("%s:%s", name, virtualColumn.getOutputName())
      );
    }

    aggregatorFactory = new BloomFilterAggregatorFactory(
        aggName,
        spec,
        maxNumEntries
    );

    return Aggregation.create(
        virtualColumns,
        aggregatorFactory
    );
  }

  private static class BloomFilterSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE1 = "'" + NAME + "(column, maxNumEntries)'\n";

    BloomFilterSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.OTHER),
          null,
          OperandTypes.and(
              OperandTypes.sequence(SIGNATURE1, OperandTypes.ANY, OperandTypes.LITERAL),
              OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false
      );
    }
  }
}
