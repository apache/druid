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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.TimeUnits;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Arrays;
import java.util.stream.Collectors;

public class CeilOperatorConversion implements SqlOperatorConversion
{
  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.CEIL;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final RexNode arg = call.getOperands().get(0);
    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        arg
    );
    if (druidExpression == null) {
      return null;
    } else if (call.getOperands().size() == 1) {
      // CEIL(expr)
      return druidExpression.map(
          simpleExtraction -> null,
          expression -> StringUtils.format("ceil(%s)", expression)
      );
    } else if (call.getOperands().size() == 2) {
      // CEIL(expr TO timeUnit)
      final RexLiteral flag = (RexLiteral) call.getOperands().get(1);
      final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
      final PeriodGranularity granularity = TimeUnits.toQueryGranularity(timeUnit, plannerContext.getTimeZone());
      if (granularity == null) {
        return null;
      }

      // Unlike FLOOR(expr TO timeUnit) there is no built-in extractionFn that can behave like timestamp_ceil.
      // So there is no simple extraction for this operator.
      return DruidExpression.fromFunctionCall(
          "timestamp_ceil",
          Arrays.asList(
              druidExpression.getExpression(),
              DruidExpression.stringLiteral(granularity.getPeriod().toString()),
              DruidExpression.numberLiteral(
                  granularity.getOrigin() == null ? null : granularity.getOrigin().getMillis()
              ),
              DruidExpression.stringLiteral(granularity.getTimeZone().toString())
          ).stream().map(DruidExpression::fromExpression).collect(Collectors.toList())
      );
    } else {
      // WTF? CEIL with 3 arguments?
      return null;
    }
  }
}
