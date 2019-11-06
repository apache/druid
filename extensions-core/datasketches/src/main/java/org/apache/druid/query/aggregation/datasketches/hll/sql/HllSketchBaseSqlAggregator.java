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

package org.apache.druid.query.aggregation.datasketches.hll.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchMergeAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
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

public abstract class HllSketchBaseSqlAggregator implements SqlAggregator
{
  private static final boolean ROUND = true;

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
    // Don't use Aggregations.getArgumentsForSimpleAggregator, since it won't let us use direct column access
    // for string columns.
    final RexNode columnRexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(0)
    );

    final DruidExpression columnArg = Expressions.toDruidExpression(plannerContext, rowSignature, columnRexNode);
    if (columnArg == null) {
      return null;
    }

    final int logK;
    if (aggregateCall.getArgList().size() >= 2) {
      final RexNode logKarg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(1)
      );

      if (!logKarg.isA(SqlKind.LITERAL)) {
        // logK must be a literal in order to plan.
        return null;
      }

      logK = ((Number) RexLiteral.value(logKarg)).intValue();
    } else {
      logK = HllSketchAggregatorFactory.DEFAULT_LG_K;
    }

    final String tgtHllType;
    if (aggregateCall.getArgList().size() >= 3) {
      final RexNode tgtHllTypeArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );

      if (!tgtHllTypeArg.isA(SqlKind.LITERAL)) {
        // tgtHllType must be a literal in order to plan.
        return null;
      }

      tgtHllType = RexLiteral.stringValue(tgtHllTypeArg);
    } else {
      tgtHllType = HllSketchAggregatorFactory.DEFAULT_TGT_HLL_TYPE.name();
    }

    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final AggregatorFactory aggregatorFactory;
    final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;

    if (columnArg.isDirectColumnAccess()
        && rowSignature.getColumnType(columnArg.getDirectColumn()) == ValueType.COMPLEX) {
      aggregatorFactory = new HllSketchMergeAggregatorFactory(
          aggregatorName,
          columnArg.getDirectColumn(),
          logK,
          tgtHllType,
          ROUND
      );
    } else {
      final SqlTypeName sqlTypeName = columnRexNode.getType().getSqlTypeName();
      final ValueType inputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
      if (inputType == null) {
        throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, aggregatorName);
      }

      final DimensionSpec dimensionSpec;

      if (columnArg.isDirectColumnAccess()) {
        dimensionSpec = columnArg.getSimpleExtraction().toDimensionSpec(null, inputType);
      } else {
        VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            plannerContext,
            columnArg,
            sqlTypeName
        );
        dimensionSpec = new DefaultDimensionSpec(virtualColumn.getOutputName(), null, inputType);
        virtualColumns.add(virtualColumn);
      }

      aggregatorFactory = new HllSketchBuildAggregatorFactory(
          aggregatorName,
          dimensionSpec.getDimension(),
          logK,
          tgtHllType,
          ROUND
      );
    }

    return toAggregation(
        name,
        finalizeAggregations,
        virtualColumns,
        aggregatorFactory
    );
  }

  protected abstract Aggregation toAggregation(
      String name,
      boolean finalizeAggregations,
      List<VirtualColumn> virtualColumns,
      AggregatorFactory aggregatorFactory
  );
}
