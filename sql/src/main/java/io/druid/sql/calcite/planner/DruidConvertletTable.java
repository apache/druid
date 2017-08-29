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

package io.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.ISE;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Map;

public class DruidConvertletTable implements SqlRexConvertletTable
{
  private static final SqlRexConvertlet BYPASS_CONVERTLET = new SqlRexConvertlet()
  {
    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call)
    {
      return StandardConvertletTable.INSTANCE.convertCall(cx, call);
    }
  };

  private final PlannerContext plannerContext;
  private final Map<SqlOperator, SqlRexConvertlet> table;

  public DruidConvertletTable(final PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;

    final SqlRexConvertlet currentTimestampAndFriendsConvertlet = new CurrentTimestampAndFriendsConvertlet();
    this.table = ImmutableMap.<SqlOperator, SqlRexConvertlet>builder()
        .put(SqlStdOperatorTable.CURRENT_TIMESTAMP, currentTimestampAndFriendsConvertlet)
        .put(SqlStdOperatorTable.CURRENT_TIME, currentTimestampAndFriendsConvertlet)
        .put(SqlStdOperatorTable.CURRENT_DATE, currentTimestampAndFriendsConvertlet)
        .put(SqlStdOperatorTable.LOCALTIMESTAMP, currentTimestampAndFriendsConvertlet)
        .put(SqlStdOperatorTable.LOCALTIME, currentTimestampAndFriendsConvertlet)
        .build();
  }

  @Override
  public SqlRexConvertlet get(SqlCall call)
  {
    if (call.getKind() == SqlKind.EXTRACT && call.getOperandList().get(1).getKind() != SqlKind.LITERAL) {
      // Avoid using the standard convertlet for EXTRACT(TIMEUNIT FROM col), since we want to handle it directly
      // in ExtractOperationConversion.
      return BYPASS_CONVERTLET;
    } else {
      final SqlRexConvertlet convertlet = table.get(call.getOperator());
      return convertlet != null ? convertlet : StandardConvertletTable.INSTANCE.get(call);
    }
  }

  private class CurrentTimestampAndFriendsConvertlet implements SqlRexConvertlet
  {
    @Override
    public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
    {
      final SqlOperator operator = call.getOperator();
      if (operator == SqlStdOperatorTable.CURRENT_TIMESTAMP || operator == SqlStdOperatorTable.LOCALTIMESTAMP) {
        return cx.getRexBuilder().makeTimestampLiteral(
            Calcites.jodaToCalciteCalendarLiteral(plannerContext.getLocalNow(), plannerContext.getTimeZone()),
            RelDataType.PRECISION_NOT_SPECIFIED
        );
      } else if (operator == SqlStdOperatorTable.CURRENT_TIME || operator == SqlStdOperatorTable.LOCALTIME) {
        return cx.getRexBuilder().makeTimeLiteral(
            Calcites.jodaToCalciteCalendarLiteral(plannerContext.getLocalNow(), plannerContext.getTimeZone()),
            RelDataType.PRECISION_NOT_SPECIFIED
        );
      } else if (operator == SqlStdOperatorTable.CURRENT_DATE) {
        return cx.getRexBuilder().makeDateLiteral(
            Calcites.jodaToCalciteCalendarLiteral(
                plannerContext.getLocalNow().hourOfDay().roundFloorCopy(),
                plannerContext.getTimeZone()
            )
        );
      } else {
        throw new ISE("WTF?! Should not have got here, operator was: %s", operator);
      }
    }
  }
}
