/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.extraction.BucketExtractionFn;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class FloorExtractionOperator implements SqlExtractionOperator
{
  public static SimpleExtraction applyTimestampFloor(
      final String column,
      final Granularity queryGranularity
  )
  {
    if (column == null || queryGranularity == null) {
      return null;
    }

    return SimpleExtraction.of(column, ExtractionFns.fromQueryGranularity(queryGranularity));
  }

  @Override
  public SqlFunction calciteFunction()
  {
    return SqlStdOperatorTable.FLOOR;
  }

  @Override
  public String convert(
      final DruidOperatorTable operatorTable,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexNode expression
  )
  {
    final RexCall call = (RexCall) expression;
    final RexNode arg = call.getOperands().get(0);
    final String column = Expressions.toDruidColumn(
        operatorTable,
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        arg
    );
    if (column == null) {
      return null;
    } else if (call.getOperands().size() == 1) {
      // FLOOR(expr)
      return virtualColumnRegistry.register(
          expression,
          SimpleExtraction.of(column, new BucketExtractionFn(1.0, 0.0))
      ).getOutputName();
    } else if (call.getOperands().size() == 2) {
      // FLOOR(expr TO timeUnit)
      final RexLiteral flag = (RexLiteral) call.getOperands().get(1);
      final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
      final Granularity granularity = TimeUnits.toQueryGranularity(timeUnit, plannerContext.getTimeZone());
      if (granularity == null) {
        return null;
      }

      return virtualColumnRegistry.register(
          expression,
          SimpleExtraction.of(column, ExtractionFns.fromQueryGranularity(granularity))
      ).getOutputName();
    } else {
      // WTF? FLOOR with 3 arguments?
      return null;
    }
  }
}
