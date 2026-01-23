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

package org.apache.druid.spectator.histogram.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.spectator.histogram.SpectatorHistogramAggregatorFactory;
import org.apache.druid.spectator.histogram.SpectatorHistogramPercentilePostAggregator;
import org.apache.druid.spectator.histogram.SpectatorHistogramPercentilesPostAggregator;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * SQL aggregator for computing percentiles from SpectatorHistograms.
 * <p>
 * Supports two forms:
 * - SPECTATOR_PERCENTILE(column, percentile) -> DOUBLE (single percentile)
 * - SPECTATOR_PERCENTILE(column, ARRAY[p1, p2, ...]) -> DOUBLE ARRAY (multiple percentiles)
 * <p>
 * Percentile values should be in the range [0, 100] (e.g., 95 for p95).
 */
public class SpectatorHistogramPercentileSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new SpectatorHistogramPercentileSqlAggFunction();
  private static final String NAME = "SPECTATOR_PERCENTILE";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final DruidExpression input = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        inputAccessor.getInputRowSignature(),
        inputAccessor.getField(aggregateCall.getArgList().get(0))
    );
    if (input == null) {
      return null;
    }

    final RexNode percentileArg = inputAccessor.getField(aggregateCall.getArgList().get(1));

    // Check if percentile argument is an array or a single value
    if (percentileArg.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
      return handleArrayPercentiles(
          virtualColumnRegistry,
          name,
          input,
          percentileArg,
          existingAggregations
      );
    } else if (percentileArg.isA(SqlKind.LITERAL)) {
      return handleSinglePercentile(
          virtualColumnRegistry,
          name,
          input,
          percentileArg,
          existingAggregations
      );
    }

    // Cannot handle non-literal percentile arguments
    return null;
  }

  private Aggregation handleSinglePercentile(
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final DruidExpression input,
      final RexNode percentileArg,
      final List<Aggregation> existingAggregations
  )
  {
    final Object value = RexLiteral.value(percentileArg);
    if (!(value instanceof Number)) {
      throw InvalidSqlInput.exception("SPECTATOR_PERCENTILE percentile parameter must be a numeric literal, got %s",
                                      value == null ? "NULL" : value.getClass().getSimpleName());
    }
    final double percentile = ((Number) value).doubleValue();

    final String histogramName = StringUtils.format("%s:agg", name);

    // Look for existing matching aggregatorFactory
    final SpectatorHistogramAggregatorFactory existingFactory =
        SpectatorHistogramSqlUtils.findMatchingAggregatorFactory(
            virtualColumnRegistry,
            input,
            existingAggregations
        );

    if (existingFactory != null) {
      return Aggregation.create(
          ImmutableList.of(),
          new SpectatorHistogramPercentilePostAggregator(
              name,
              new FieldAccessPostAggregator(
                  existingFactory.getName(),
                  existingFactory.getName()
              ),
              percentile
          )
      );
    }

    // No existing match found. Create a new one.
    final SpectatorHistogramAggregatorFactory aggregatorFactory =
        SpectatorHistogramSqlUtils.createAggregatorFactory(
            virtualColumnRegistry,
            input,
            histogramName
        );

    return Aggregation.create(
        ImmutableList.of(aggregatorFactory),
        new SpectatorHistogramPercentilePostAggregator(
            name,
            new FieldAccessPostAggregator(histogramName, histogramName),
            percentile
        )
    );
  }

  @Nullable
  private Aggregation handleArrayPercentiles(
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final DruidExpression input,
      final RexNode percentileArg,
      final List<Aggregation> existingAggregations
  )
  {
    // Extract array elements
    final List<RexNode> arrayElements = ((RexCall) percentileArg).getOperands();
    final double[] percentiles = new double[arrayElements.size()];

    for (int i = 0; i < arrayElements.size(); i++) {
      RexNode element = arrayElements.get(i);
      if (!element.isA(SqlKind.LITERAL)) {
        return null; // All array elements must be literals
      }
      percentiles[i] = ((Number) RexLiteral.value(element)).doubleValue();
    }

    final String histogramName = StringUtils.format("%s:agg", name);

    // Look for existing matching aggregatorFactory
    final SpectatorHistogramAggregatorFactory existingFactory =
        SpectatorHistogramSqlUtils.findMatchingAggregatorFactory(
            virtualColumnRegistry,
            input,
            existingAggregations
        );

    if (existingFactory != null) {
      return Aggregation.create(
          ImmutableList.of(),
          new SpectatorHistogramPercentilesPostAggregator(
              name,
              new FieldAccessPostAggregator(
                  existingFactory.getName(),
                  existingFactory.getName()
              ),
              percentiles
          )
      );
    }

    // No existing match found. Create a new one.
    final SpectatorHistogramAggregatorFactory aggregatorFactory =
        SpectatorHistogramSqlUtils.createAggregatorFactory(
            virtualColumnRegistry,
            input,
            histogramName
        );

    return Aggregation.create(
        ImmutableList.of(aggregatorFactory),
        new SpectatorHistogramPercentilesPostAggregator(
            name,
            new FieldAccessPostAggregator(histogramName, histogramName),
            percentiles
        )
    );
  }

  /**
   * Return type inference that returns DOUBLE for single percentile value
   * and DOUBLE ARRAY when an array of percentiles is provided.
   */
  static class SpectatorHistogramPercentileReturnTypeInference implements SqlReturnTypeInference
  {
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding sqlOperatorBinding)
    {
      RelDataType secondArgType = sqlOperatorBinding.getOperandType(1);
      if (secondArgType.getSqlTypeName() == SqlTypeName.ARRAY) {
        // Return DOUBLE ARRAY when input is an array of percentiles
        return sqlOperatorBinding.getTypeFactory().createArrayType(
            sqlOperatorBinding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
            -1
        );
      }
      // Return DOUBLE for single percentile value
      return sqlOperatorBinding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
    }
  }

  private static class SpectatorHistogramPercentileSqlAggFunction extends SqlAggFunction
  {
    private static final SpectatorHistogramPercentileReturnTypeInference RETURN_TYPE_INFERENCE =
        new SpectatorHistogramPercentileReturnTypeInference();

    SpectatorHistogramPercentileSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          RETURN_TYPE_INFERENCE,
          null,
          OperandTypes.ANY_ANY,
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
