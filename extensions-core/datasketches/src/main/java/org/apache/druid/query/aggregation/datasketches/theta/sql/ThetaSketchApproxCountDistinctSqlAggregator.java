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

package org.apache.druid.query.aggregation.datasketches.theta.sql;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.CastedLiteralOperandTypeCheckers;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.Collections;

/**
 * Approximate count distinct aggregator using theta sketches.
 * Supported column types: String, Numeric, Theta Sketch.
 */
public class ThetaSketchApproxCountDistinctSqlAggregator extends ThetaSketchBaseSqlAggregator implements SqlAggregator
{
  public static final String NAME = "APPROX_COUNT_DISTINCT_DS_THETA";

  private static final SqlSingleOperandTypeChecker AGGREGATED_COLUMN_TYPE_CHECKER = OperandTypes.or(
      OperandTypes.STRING,
      OperandTypes.NUMERIC,
      RowSignatures.complexTypeChecker(SketchModule.THETA_SKETCH_TYPE)
  );

  private static final SqlAggFunction FUNCTION_INSTANCE =
      OperatorConversions.aggregatorBuilder(NAME)
                         .operandTypeInference(InferTypes.VARCHAR_1024)
                         .operandTypeChecker(
                             OperandTypes.or(
                                 // APPROX_COUNT_DISTINCT_DS_THETA(expr)
                                 AGGREGATED_COLUMN_TYPE_CHECKER,
                                 // APPROX_COUNT_DISTINCT_DS_THETA(expr, size)
                                 OperandTypes.and(
                                     OperandTypes.sequence(
                                         StringUtils.format("'%s(expr, size)'", NAME),
                                         AGGREGATED_COLUMN_TYPE_CHECKER,
                                         CastedLiteralOperandTypeCheckers.POSITIVE_INTEGER_LITERAL
                                     ),
                                     OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.EXACT_NUMERIC)
                                 )
                             )
                         )
                         .returnTypeNonNull(SqlTypeName.BIGINT)
                         .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
                         .build();

  public ThetaSketchApproxCountDistinctSqlAggregator()
  {
    super(true);
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  protected Aggregation toAggregation(
      String name,
      boolean finalizeAggregations,
      AggregatorFactory aggregatorFactory
  )
  {
    return Aggregation.create(
        Collections.singletonList(aggregatorFactory),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(
            name,
            aggregatorFactory.getName()
        ) : null
    );
  }
}
