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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;

import java.util.Collections;
import java.util.List;

public class ThetaSketchApproxCountDistinctSqlAggregator extends ThetaSketchBaseSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new ThetaSketchSqlAggFunction();
  private static final String NAME = "APPROX_COUNT_DISTINCT_DS_THETA";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  protected Aggregation toAggregation(
      String name,
      boolean finalizeAggregations,
      List<VirtualColumn> virtualColumns,
      AggregatorFactory aggregatorFactory
  )
  {
    return Aggregation.create(
        virtualColumns,
        Collections.singletonList(aggregatorFactory),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(
            name,
            aggregatorFactory.getName()
        ) : null
    );
  }

  private static class ThetaSketchSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE = "'" + NAME + "(column, size)'\n";

    ThetaSketchSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          InferTypes.VARCHAR_1024,
          OperandTypes.or(
              OperandTypes.ANY,
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE, OperandTypes.ANY, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
              )
          ),
          SqlFunctionCategory.NUMERIC,
          false,
          false
      );
    }
  }
}
