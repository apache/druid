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
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class FloorExpressionConversion extends AbstractExpressionConversion
{
  private static final FloorExpressionConversion INSTANCE = new FloorExpressionConversion();

  private FloorExpressionConversion()
  {
    super(SqlKind.FLOOR);
  }

  public static FloorExpressionConversion instance()
  {
    return INSTANCE;
  }

  public static RowExtraction applyTimestampFloor(
      final RowExtraction rex,
      final Granularity queryGranularity
  )
  {
    if (rex == null || queryGranularity == null) {
      return null;
    }

    return RowExtraction.of(
        rex.getColumn(),
        ExtractionFns.compose(
            ExtractionFns.fromQueryGranularity(queryGranularity),
            rex.getExtractionFn()
        )
    );
  }

  @Override
  public RowExtraction convert(
      final ExpressionConverter converter,
      final PlannerContext plannerContext,
      final List<String> rowOrder,
      final RexNode expression
  )
  {
    final RexCall call = (RexCall) expression;
    final RexNode arg = call.getOperands().get(0);

    final RowExtraction rex = converter.convert(plannerContext, rowOrder, arg);
    if (rex == null) {
      return null;
    } else if (call.getOperands().size() == 1) {
      // FLOOR(expr)
      return RowExtraction.of(
          rex.getColumn(),
          ExtractionFns.compose(new BucketExtractionFn(1.0, 0.0), rex.getExtractionFn())
      );
    } else if (call.getOperands().size() == 2) {
      // FLOOR(expr TO timeUnit)
      final RexLiteral flag = (RexLiteral) call.getOperands().get(1);
      final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
      return applyTimestampFloor(rex, TimeUnits.toQueryGranularity(timeUnit, plannerContext.getTimeZone()));
    } else {
      // WTF? FLOOR with 3 arguments?
      return null;
    }
  }
}
