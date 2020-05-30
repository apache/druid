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

package org.apache.druid.query.aggregation.histogram.sql;

import com.google.common.collect.ImmutableList;
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
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogram;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogramAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.QuantilePostAggregator;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class FixedBucketsHistogramQuantileSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new FixedBucketsHistogramQuantileSqlAggFunction();
  private static final String NAME = "APPROX_QUANTILE_FIXED_BUCKETS";

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
    final DruidExpression input = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rowSignature,
            project,
            aggregateCall.getArgList().get(0)
        )
    );
    if (input == null) {
      return null;
    }

    final AggregatorFactory aggregatorFactory;
    final String histogramName = StringUtils.format("%s:agg", name);
    final RexNode probabilityArg = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(1)
    );

    if (!probabilityArg.isA(SqlKind.LITERAL)) {
      // Probability must be a literal in order to plan.
      return null;
    }

    final float probability = ((Number) RexLiteral.value(probabilityArg)).floatValue();

    final int numBuckets;
    if (aggregateCall.getArgList().size() >= 3) {
      final RexNode numBucketsArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );

      if (!numBucketsArg.isA(SqlKind.LITERAL)) {
        // Resolution must be a literal in order to plan.
        return null;
      }

      numBuckets = ((Number) RexLiteral.value(numBucketsArg)).intValue();
    } else {
      return null;
    }

    final double lowerLimit;
    if (aggregateCall.getArgList().size() >= 4) {
      final RexNode lowerLimitArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(3)
      );

      if (!lowerLimitArg.isA(SqlKind.LITERAL)) {
        // Resolution must be a literal in order to plan.
        return null;
      }

      lowerLimit = ((Number) RexLiteral.value(lowerLimitArg)).doubleValue();
    } else {
      return null;
    }

    final double upperLimit;
    if (aggregateCall.getArgList().size() >= 5) {
      final RexNode upperLimitArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(4)
      );

      if (!upperLimitArg.isA(SqlKind.LITERAL)) {
        // Resolution must be a literal in order to plan.
        return null;
      }

      upperLimit = ((Number) RexLiteral.value(upperLimitArg)).doubleValue();
    } else {
      return null;
    }

    final FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode;
    if (aggregateCall.getArgList().size() >= 6) {
      final RexNode outlierHandlingModeArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(5)
      );

      if (!outlierHandlingModeArg.isA(SqlKind.LITERAL)) {
        // Resolution must be a literal in order to plan.
        return null;
      }

      outlierHandlingMode = FixedBucketsHistogram.OutlierHandlingMode.fromString(
          RexLiteral.stringValue(outlierHandlingModeArg)
      );
    } else {
      outlierHandlingMode = FixedBucketsHistogram.OutlierHandlingMode.IGNORE;
    }

    // Look for existing matching aggregatorFactory.
    for (final Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        if (factory instanceof FixedBucketsHistogramAggregatorFactory) {
          final FixedBucketsHistogramAggregatorFactory theFactory = (FixedBucketsHistogramAggregatorFactory) factory;

          // Check input for equivalence.
          final boolean inputMatches;
          final VirtualColumn virtualInput = existing.getVirtualColumns()
                                                     .stream()
                                                     .filter(
                                                         virtualColumn ->
                                                             virtualColumn.getOutputName()
                                                                          .equals(theFactory.getFieldName())
                                                     )
                                                     .findFirst()
                                                     .orElse(null);

          if (virtualInput == null) {
            inputMatches = input.isDirectColumnAccess()
                           && input.getDirectColumn().equals(theFactory.getFieldName());
          } else {
            inputMatches = ((ExpressionVirtualColumn) virtualInput).getExpression()
                                                                   .equals(input.getExpression());
          }

          final boolean matches = inputMatches
                                  && theFactory.getOutlierHandlingMode() == outlierHandlingMode
                                  && theFactory.getNumBuckets() == numBuckets
                                  && theFactory.getLowerLimit() == lowerLimit
                                  && theFactory.getUpperLimit() == upperLimit;

          if (matches) {
            // Found existing one. Use this.
            return Aggregation.create(
                ImmutableList.of(),
                new QuantilePostAggregator(name, factory.getName(), probability)
            );
          }
        }
      }
    }

    // No existing match found. Create a new one.
    final List<VirtualColumn> virtualColumns = new ArrayList<>();

    if (input.isDirectColumnAccess()) {
      aggregatorFactory = new FixedBucketsHistogramAggregatorFactory(
          histogramName,
          input.getDirectColumn(),
          numBuckets,
          lowerLimit,
          upperLimit,
          outlierHandlingMode,
          false
      );
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          input,
          SqlTypeName.FLOAT
      );
      virtualColumns.add(virtualColumn);
      aggregatorFactory = new FixedBucketsHistogramAggregatorFactory(
          histogramName,
          virtualColumn.getOutputName(),
          numBuckets,
          lowerLimit,
          upperLimit,
          outlierHandlingMode,
          false
      );
    }

    return Aggregation.create(
        virtualColumns,
        ImmutableList.of(aggregatorFactory),
        new QuantilePostAggregator(name, histogramName, probability)
    );
  }

  private static class FixedBucketsHistogramQuantileSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE1 =
        "'"
        + NAME
        + "(column, probability, numBuckets, lowerLimit, upperLimit)'\n";
    private static final String SIGNATURE2 =
        "'"
        + NAME
        + "(column, probability, numBuckets, lowerLimit, upperLimit, outlierHandlingMode)'\n";

    FixedBucketsHistogramQuantileSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.DOUBLE),
          null,
          OperandTypes.or(
              OperandTypes.and(
                  OperandTypes.sequence(
                      SIGNATURE1,
                      OperandTypes.ANY,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC
                  )
              ),
              OperandTypes.and(
                  OperandTypes.sequence(
                      SIGNATURE2,
                      OperandTypes.ANY,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.STRING
                  )
              )
          ),
          SqlFunctionCategory.NUMERIC,
          false,
          false
      );
    }
  }
}
