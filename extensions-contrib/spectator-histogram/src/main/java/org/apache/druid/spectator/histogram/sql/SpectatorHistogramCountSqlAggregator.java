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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.spectator.histogram.SpectatorHistogramAggregatorFactory;
import org.apache.druid.spectator.histogram.SpectatorHistogramCountPostAggregator;
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
 * SQL aggregator for counting total observations in SpectatorHistograms.
 *
 * SPECTATOR_COUNT(column) -> BIGINT
 *
 * Returns the sum of all bucket counts (total number of observations recorded in the histogram).
 */
public class SpectatorHistogramCountSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new SpectatorHistogramCountSqlAggFunction();
  private static final String NAME = "SPECTATOR_COUNT";

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
          new SpectatorHistogramCountPostAggregator(
              name,
              new FieldAccessPostAggregator(
                  existingFactory.getName(),
                  existingFactory.getName()
              )
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
        new SpectatorHistogramCountPostAggregator(
            name,
            new FieldAccessPostAggregator(histogramName, histogramName)
        )
    );
  }

  private static class SpectatorHistogramCountSqlAggFunction extends SqlAggFunction
  {
    SpectatorHistogramCountSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
