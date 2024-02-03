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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import org.apache.druid.query.aggregation.variance.VarianceAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.List;

public abstract class BaseVarianceSqlAggregator implements SqlAggregator
{
  private static final String VARIANCE_NAME = "VARIANCE";
  private static final String STDDEV_NAME = "STDDEV";

  private static final SqlAggFunction VARIANCE_SQL_AGG_FUNC_INSTANCE =
      buildSqlVarianceAggFunction(VARIANCE_NAME);
  private static final SqlAggFunction VARIANCE_POP_SQL_AGG_FUNC_INSTANCE =
      buildSqlVarianceAggFunction(SqlKind.VAR_POP.name());
  private static final SqlAggFunction VARIANCE_SAMP_SQL_AGG_FUNC_INSTANCE =
      buildSqlVarianceAggFunction(SqlKind.VAR_SAMP.name());
  private static final SqlAggFunction STDDEV_SQL_AGG_FUNC_INSTANCE =
      buildSqlVarianceAggFunction(STDDEV_NAME);
  private static final SqlAggFunction STDDEV_POP_SQL_AGG_FUNC_INSTANCE =
      buildSqlVarianceAggFunction(SqlKind.STDDEV_POP.name());
  private static final SqlAggFunction STDDEV_SAMP_SQL_AGG_FUNC_INSTANCE =
      buildSqlVarianceAggFunction(SqlKind.STDDEV_SAMP.name());

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      VirtualColumnRegistry virtualColumnRegistry,
      String name,
      AggregateCall aggregateCall,
      InputAccessor inputAccessor,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    final RexNode inputOperand = inputAccessor.getField(aggregateCall.getArgList().get(0));

    final DruidExpression input = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        inputAccessor.getInputRowSignature(),
        inputOperand
    );
    if (input == null) {
      return null;
    }

    final AggregatorFactory aggregatorFactory;
    final RelDataType dataType = inputOperand.getType();
    final ColumnType inputType = Calcites.getColumnTypeForRelDataType(dataType);
    final DimensionSpec dimensionSpec;
    final String aggName = StringUtils.format("%s:agg", name);
    final SqlAggFunction func = calciteFunction();
    final String estimator;
    final String inputTypeName;
    PostAggregator postAggregator = null;

    if (input.isSimpleExtraction()) {
      dimensionSpec = input.getSimpleExtraction().toDimensionSpec(null, inputType);
    } else {
      String virtualColumnName =
          virtualColumnRegistry.getOrCreateVirtualColumnForExpression(input, dataType);
      dimensionSpec = new DefaultDimensionSpec(virtualColumnName, null, inputType);
    }

    if (inputType == null) {
      throw new IAE("VarianceSqlAggregator[%s] has invalid inputType", func);
    }

    if (inputType.isNumeric()) {
      inputTypeName = StringUtils.toLowerCase(inputType.getType().name());
    } else if (inputType.equals(VarianceAggregatorFactory.TYPE)) {
      inputTypeName = VarianceAggregatorFactory.VARIANCE_TYPE_NAME;
    } else {
      throw new IAE("VarianceSqlAggregator[%s] has invalid inputType[%s]", func, inputType.asTypeString());
    }

    if (func.getName().equals(SqlKind.VAR_POP.name()) || func.getName().equals(SqlKind.STDDEV_POP.name())) {
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

    if (func.getName().equals(STDDEV_NAME)
        || func.getName().equals(SqlKind.STDDEV_POP.name())
        || func.getName().equals(SqlKind.STDDEV_SAMP.name())) {
      postAggregator = new StandardDeviationPostAggregator(
          name,
          aggregatorFactory.getName(),
          estimator);
    }

    return Aggregation.create(
        ImmutableList.of(aggregatorFactory),
        postAggregator
    );
  }

  /**
   * Creates a {@link SqlAggFunction}
   *
   * It accepts variance aggregator objects in addition to numeric inputs.
   */
  private static SqlAggFunction buildSqlVarianceAggFunction(String name)
  {
    return OperatorConversions
        .aggregatorBuilder(name)
        .returnTypeInference(ReturnTypes.explicit(SqlTypeName.DOUBLE))
        .operandTypeChecker(
            OperandTypes.or(
                OperandTypes.NUMERIC,
                RowSignatures.complexTypeChecker(VarianceAggregatorFactory.TYPE)
            )
        )
        .functionCategory(SqlFunctionCategory.NUMERIC)
        .build();
  }

  public static class VarPopSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return VARIANCE_POP_SQL_AGG_FUNC_INSTANCE;
    }
  }

  public static class VarSampSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return VARIANCE_SAMP_SQL_AGG_FUNC_INSTANCE;
    }
  }

  public static class VarianceSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return VARIANCE_SQL_AGG_FUNC_INSTANCE;
    }
  }

  public static class StdDevPopSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return STDDEV_POP_SQL_AGG_FUNC_INSTANCE;
    }
  }

  public static class StdDevSampSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return STDDEV_SAMP_SQL_AGG_FUNC_INSTANCE;
    }
  }

  public static class StdDevSqlAggregator extends BaseVarianceSqlAggregator
  {
    @Override
    public SqlAggFunction calciteFunction()
    {
      return STDDEV_SQL_AGG_FUNC_INSTANCE;
    }
  }
}
