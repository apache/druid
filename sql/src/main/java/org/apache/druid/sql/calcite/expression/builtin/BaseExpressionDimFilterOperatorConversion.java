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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.sql.SqlOperator;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SimpleExtraction;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;

public abstract class BaseExpressionDimFilterOperatorConversion extends DirectOperatorConversion
{
  public BaseExpressionDimFilterOperatorConversion(
      SqlOperator operator,
      String druidFunctionName
  )
  {
    super(operator, druidFunctionName);
  }

  protected static DimFilter toExpressionFilter(
      PlannerContext plannerContext,
      String druidFunctionName,
      List<DruidExpression> druidExpressions
  )
  {
    final String filterExpr = DruidExpression.functionCall(druidFunctionName, druidExpressions);

    return new ExpressionDimFilter(
        filterExpr,
        plannerContext.getExprMacroTable()
    );
  }

  protected static SelectorDimFilter newSelectorDimFilter(
      SimpleExtraction simpleExtraction,
      String value
  )
  {
    return new SelectorDimFilter(
        simpleExtraction.getColumn(),
        value,
        simpleExtraction.getExtractionFn(),
        null
    );
  }
}
