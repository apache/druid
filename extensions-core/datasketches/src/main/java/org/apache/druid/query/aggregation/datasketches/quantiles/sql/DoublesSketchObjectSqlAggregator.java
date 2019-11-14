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

package org.apache.druid.query.aggregation.datasketches.quantiles.sql;

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
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
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

public class DoublesSketchObjectSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new DoublesSketchSqlAggFunction();
  private static final String NAME = "DS_QUANTILES_SKETCH";

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
    final int k;

    if (aggregateCall.getArgList().size() >= 2) {
      final RexNode resolutionArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(1)
      );

      if (!resolutionArg.isA(SqlKind.LITERAL)) {
        // Resolution must be a literal in order to plan.
        return null;
      }

      k = ((Number) RexLiteral.value(resolutionArg)).intValue();
    } else {
      k = DoublesSketchAggregatorFactory.DEFAULT_K;
    }

    // No existing match found. Create a new one.
    final List<VirtualColumn> virtualColumns = new ArrayList<>();

    if (input.isDirectColumnAccess()) {
      aggregatorFactory = new DoublesSketchAggregatorFactory(
          histogramName,
          input.getDirectColumn(),
          k
      );
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          input,
          SqlTypeName.FLOAT
      );
      virtualColumns.add(virtualColumn);
      aggregatorFactory = new DoublesSketchAggregatorFactory(
          histogramName,
          virtualColumn.getOutputName(),
          k
      );
    }

    return Aggregation.create(
        virtualColumns,
        ImmutableList.of(aggregatorFactory),
        null
    );
  }

  private static class DoublesSketchSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE1 = "'" + NAME + "(column)'\n";
    private static final String SIGNATURE2 = "'" + NAME + "(column, k)'\n";

    DoublesSketchSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.OTHER),
          null,
          OperandTypes.or(
              OperandTypes.ANY,
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE2, OperandTypes.ANY, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.EXACT_NUMERIC)
              )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false
      );
    }
  }
}
