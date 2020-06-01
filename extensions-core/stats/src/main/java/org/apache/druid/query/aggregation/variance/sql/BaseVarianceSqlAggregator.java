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

package org.apache.druid.query.aggregation.variance.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import org.apache.druid.query.aggregation.variance.VarianceAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
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

public abstract class BaseVarianceSqlAggregator implements SqlAggregator
{
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
    final SqlTypeName sqlTypeName = inputOperand.getType().getSqlTypeName();
    final ValueType inputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final DimensionSpec dimensionSpec;
    final String aggName = StringUtils.format("%s:agg", name);
    final SqlAggFunction func = calciteFunction();
    final String estimator;
    final String inputTypeName;
    PostAggregator postAggregator = null;

    if (input.isSimpleExtraction()) {
      dimensionSpec = input.getSimpleExtraction().toDimensionSpec(null, inputType);
    } else {
      VirtualColumn virtualColumn =
          virtualColumnRegistry.getOrCreateVirtualColumnForExpression(plannerContext, input, sqlTypeName);
      dimensionSpec = new DefaultDimensionSpec(virtualColumn.getOutputName(), null, inputType);
      virtualColumns.add(virtualColumn);
    }

    switch (inputType) {
      case LONG:
      case DOUBLE:
      case FLOAT:
        inputTypeName = StringUtils.toLowerCase(inputType.name());
        break;
      default:
        throw new IAE("VarianceSqlAggregator[%s] has invalid inputType[%s]", func, inputType);
    }


    if (func == SqlStdOperatorTable.VAR_POP || func == SqlStdOperatorTable.STDDEV_POP) {
      estimator = "population";
    } else {
      estimator = "sample";
    }

    aggregatorFactory = new VarianceAggregatorFactory(
        aggName,
        dimensionSpec.getDimension(),
        estimator,
        inputTypeName
    );

    if (func == SqlStdOperatorTable.STDDEV_POP 
        || func == SqlStdOperatorTable.STDDEV_SAMP
        || func == SqlStdOperatorTable.STDDEV) {
      postAggregator = new StandardDeviationPostAggregator(
          name,
          aggregatorFactory.getName(),
          estimator);
    }

    return Aggregation.create(
        virtualColumns,
        ImmutableList.of(aggregatorFactory),
        postAggregator
    );
  }

  public static class VarPopSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return SqlStdOperatorTable.VAR_POP;
    }
  }
  
  public static class VarSampSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return SqlStdOperatorTable.VAR_SAMP;
    }
  }

  public static class VarianceSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return SqlStdOperatorTable.VARIANCE;
    }
  }

  public static class StdDevPopSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return SqlStdOperatorTable.STDDEV_POP;
    }
  }

  public static class StdDevSampSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return SqlStdOperatorTable.STDDEV_SAMP;
    }
  }

  public static class StdDevSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return SqlStdOperatorTable.STDDEV;
    }
  }
}
