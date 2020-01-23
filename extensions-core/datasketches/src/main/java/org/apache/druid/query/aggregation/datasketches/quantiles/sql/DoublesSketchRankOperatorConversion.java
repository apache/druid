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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToRankPostAggregator;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

public class DoublesSketchRankOperatorConversion extends DoublesSketchSingleArgBaseOperatorConversion
{
  private static final String FUNCTION_NAME = "DS_RANK";
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
      .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
      .returnType(SqlTypeName.DOUBLE)
      .build();

  public DoublesSketchRankOperatorConversion()
  {
    super(SQL_FUNCTION, FUNCTION_NAME);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Override
  public PostAggregator makePostAgg(String name, PostAggregator field, float arg)
  {
    return new DoublesSketchToRankPostAggregator(
        name,
        field,
        arg
    );
  }
}
