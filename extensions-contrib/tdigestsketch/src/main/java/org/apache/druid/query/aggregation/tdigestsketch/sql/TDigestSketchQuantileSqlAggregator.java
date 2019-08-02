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

package org.apache.druid.query.aggregation.tdigestsketch.sql;

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
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchAggregatorFactory;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchToQuantilePostAggregator;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchUtils;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class TDigestSketchQuantileSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new TDigestSketchQuantileSqlAggFunction();
  private static final String NAME = "TDIGEST_QUANTILE";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
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
    // This is expected to be a tdigest sketch
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
    final String sketchName = StringUtils.format("%s:agg", name);

    // this is expected to be quantile fraction
    final RexNode quantileArg = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(1)
    );

    if (!quantileArg.isA(SqlKind.LITERAL)) {
      // Quantile must be a literal in order to plan.
      return null;
    }

    final double quantile = ((Number) RexLiteral.value(quantileArg)).floatValue();
    Integer compression = TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION;
    if (aggregateCall.getArgList().size() > 2) {
      final RexNode compressionArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );
      compression = ((Number) RexLiteral.value(compressionArg)).intValue();
    }

    // Look for existing matching aggregatorFactory.
    for (final Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        if (factory instanceof TDigestSketchAggregatorFactory) {
          final boolean matches = TDigestSketchUtils.matchingAggregatorFactoryExists(
              input,
              compression,
              existing,
              (TDigestSketchAggregatorFactory) factory
          );

          if (matches) {
            // Found existing one. Use this.
            return Aggregation.create(
                ImmutableList.of(),
                new TDigestSketchToQuantilePostAggregator(
                    name,
                    new FieldAccessPostAggregator(
                        factory.getName(),
                        factory.getName()
                    ),
                    quantile
                )
            );
          }
        }
      }
    }

    // No existing match found. Create a new one.
    final List<VirtualColumn> virtualColumns = new ArrayList<>();

    if (input.isDirectColumnAccess()) {
      aggregatorFactory = new TDigestSketchAggregatorFactory(
          sketchName,
          input.getDirectColumn(),
          compression
      );
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          input,
          SqlTypeName.FLOAT
      );
      virtualColumns.add(virtualColumn);
      aggregatorFactory = new TDigestSketchAggregatorFactory(
          sketchName,
          virtualColumn.getOutputName(),
          compression
      );
    }

    return Aggregation.create(
        virtualColumns,
        ImmutableList.of(aggregatorFactory),
        new TDigestSketchToQuantilePostAggregator(
            name,
            new FieldAccessPostAggregator(
                sketchName,
                sketchName
            ),
            quantile
        )
    );
  }

  private static class TDigestSketchQuantileSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE1 = "'" + NAME + "(column, quantile)'\n";
    private static final String SIGNATURE2 = "'" + NAME + "(column, quantile, compression)'\n";

    TDigestSketchQuantileSqlAggFunction()
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
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
              )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false
      );
    }
  }
}
