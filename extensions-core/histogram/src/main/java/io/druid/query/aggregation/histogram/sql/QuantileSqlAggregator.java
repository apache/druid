/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.histogram.sql;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.histogram.ApproximateHistogram;
import io.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import io.druid.query.aggregation.histogram.ApproximateHistogramFoldingAggregatorFactory;
import io.druid.query.aggregation.histogram.QuantilePostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.RowExtraction;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class QuantileSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new QuantileSqlAggFunction();
  private static final String NAME = "APPROX_QUANTILE";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  public Aggregation toDruidAggregation(
      final String name,
      final RowSignature rowSignature,
      final DruidOperatorTable operatorTable,
      final PlannerContext plannerContext,
      final List<Aggregation> existingAggregations,
      final Project project,
      final AggregateCall aggregateCall,
      final DimFilter filter
  )
  {
    final RowExtraction rex = Expressions.toRowExtraction(
        operatorTable,
        plannerContext,
        rowSignature.getRowOrder(),
        Expressions.fromFieldAccess(
            rowSignature,
            project,
            aggregateCall.getArgList().get(0)
        )
    );
    if (rex == null) {
      return null;
    }

    final AggregatorFactory aggregatorFactory;
    final String histogramName = String.format("%s:agg", name);
    final RexNode probabilityArg = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(1)
    );
    final float probability = ((Number) RexLiteral.value(probabilityArg)).floatValue();

    final int resolution;
    if (aggregateCall.getArgList().size() >= 3) {
      final RexNode resolutionArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );
      resolution = ((Number) RexLiteral.value(resolutionArg)).intValue();
    } else {
      resolution = ApproximateHistogram.DEFAULT_HISTOGRAM_SIZE;
    }

    final int numBuckets = ApproximateHistogram.DEFAULT_BUCKET_SIZE;
    final float lowerLimit = Float.NEGATIVE_INFINITY;
    final float upperLimit = Float.POSITIVE_INFINITY;

    // Look for existing matching aggregatorFactory.
    for (final Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        final boolean matches = Aggregations.aggregatorMatches(
            factory,
            filter,
            ApproximateHistogramAggregatorFactory.class,
            new Predicate<ApproximateHistogramAggregatorFactory>()
            {
              @Override
              public boolean apply(final ApproximateHistogramAggregatorFactory theFactory)
              {
                return theFactory.getFieldName().equals(rex.getColumn())
                       && theFactory.getResolution() == resolution
                       && theFactory.getNumBuckets() == numBuckets
                       && theFactory.getLowerLimit() == lowerLimit
                       && theFactory.getUpperLimit() == upperLimit;
              }
            }
        );

        if (matches) {
          // Found existing one. Use this.
          return Aggregation.create(
              ImmutableList.<AggregatorFactory>of(),
              new QuantilePostAggregator(name, factory.getName(), probability)
          );
        }
      }
    }

    if (rowSignature.getColumnType(rex.getColumn()) == ValueType.COMPLEX) {
      aggregatorFactory = new ApproximateHistogramFoldingAggregatorFactory(
          histogramName,
          rex.getColumn(),
          resolution,
          numBuckets,
          lowerLimit,
          upperLimit
      );
    } else {
      aggregatorFactory = new ApproximateHistogramAggregatorFactory(
          histogramName,
          rex.getColumn(),
          resolution,
          numBuckets,
          lowerLimit,
          upperLimit
      );
    }

    return Aggregation.create(
        ImmutableList.of(aggregatorFactory),
        new QuantilePostAggregator(name, histogramName, probability)
    ).filter(filter);
  }

  private static class QuantileSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE1 = "'" + NAME + "(column, probability)'\n";
    private static final String SIGNATURE2 = "'" + NAME + "(column, probability, resolution)'\n";

    QuantileSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.DOUBLE),
          null,
          OperandTypes.or(
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE1, OperandTypes.ANY, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
              ),
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE2, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.EXACT_NUMERIC)
              )
          ),
          SqlFunctionCategory.NUMERIC,
          false,
          false
      );
    }
  }
}
