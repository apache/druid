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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidConvertletTable implements SqlRexConvertletTable
{
  // Apply a convertlet that doesn't do anything other than a "dumb" call translation.
  private static final SqlRexConvertlet BYPASS_CONVERTLET = StandardConvertletTable.INSTANCE::convertCall;

  private static final List<SqlOperator> CURRENT_TIME_CONVERTLET_OPERATORS =
      ImmutableList.<SqlOperator>builder()
          .add(SqlStdOperatorTable.CURRENT_TIMESTAMP)
          .add(SqlStdOperatorTable.CURRENT_TIME)
          .add(SqlStdOperatorTable.CURRENT_DATE)
          .add(SqlStdOperatorTable.LOCALTIMESTAMP)
          .add(SqlStdOperatorTable.LOCALTIME)
          .build();

  // Operators we don't have standard conversions for, but which can be converted into ones that do by
  // Calcite's StandardConvertletTable.
  private static final List<SqlOperator> STANDARD_CONVERTLET_OPERATORS =
      ImmutableList.<SqlOperator>builder()
          .add(SqlStdOperatorTable.ROW)
          .add(SqlStdOperatorTable.NOT_IN)
          .add(SqlStdOperatorTable.NOT_LIKE)
          .add(SqlStdOperatorTable.BETWEEN)
          .add(SqlStdOperatorTable.NOT_BETWEEN)
          .add(SqlStdOperatorTable.SYMMETRIC_BETWEEN)
          .add(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN)
          .add(SqlStdOperatorTable.ITEM)
          .add(SqlStdOperatorTable.TIMESTAMP_ADD)
          .add(SqlStdOperatorTable.TIMESTAMP_DIFF)
          .add(SqlStdOperatorTable.UNION)
          .add(SqlStdOperatorTable.UNION_ALL)
          .add(OracleSqlOperatorTable.NVL)
          .build();

  private final Map<SqlOperator, SqlRexConvertlet> table;

  public DruidConvertletTable(final PlannerContext plannerContext)
  {
    this.table = createConvertletMap(plannerContext);
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

  public static List<SqlOperator> knownOperators()
  {
    final ArrayList<SqlOperator> retVal = new ArrayList<>(
        CURRENT_TIME_CONVERTLET_OPERATORS.size() + STANDARD_CONVERTLET_OPERATORS.size()
    );

    retVal.addAll(CURRENT_TIME_CONVERTLET_OPERATORS);
    retVal.addAll(STANDARD_CONVERTLET_OPERATORS);

    return retVal;
  }

  private static Map<SqlOperator, SqlRexConvertlet> createConvertletMap(final PlannerContext plannerContext)
  {
    final SqlRexConvertlet currentTimestampAndFriends = new CurrentTimestampAndFriendsConvertlet(plannerContext);
    final Map<SqlOperator, SqlRexConvertlet> table = new HashMap<>();

    for (SqlOperator operator : CURRENT_TIME_CONVERTLET_OPERATORS) {
      table.put(operator, currentTimestampAndFriends);
    }

    return table;
  }

  private static class CurrentTimestampAndFriendsConvertlet implements SqlRexConvertlet
  {
    private final PlannerContext plannerContext;

    public CurrentTimestampAndFriendsConvertlet(final PlannerContext plannerContext)
    {
      this.plannerContext = plannerContext;
    }

    @Override
    public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
    {
      final SqlOperator operator = call.getOperator();
      if (operator.equals(SqlStdOperatorTable.CURRENT_TIMESTAMP)
          || operator.equals(SqlStdOperatorTable.LOCALTIMESTAMP)) {
        return cx.getRexBuilder().makeTimestampLiteral(
            Calcites.jodaToCalciteTimestampString(plannerContext.getLocalNow(), plannerContext.getTimeZone()),
            RelDataType.PRECISION_NOT_SPECIFIED
        );
      } else if (operator.equals(SqlStdOperatorTable.CURRENT_TIME) || operator.equals(SqlStdOperatorTable.LOCALTIME)) {
        return cx.getRexBuilder().makeTimeLiteral(
            Calcites.jodaToCalciteTimeString(plannerContext.getLocalNow(), plannerContext.getTimeZone()),
            RelDataType.PRECISION_NOT_SPECIFIED
        );
      } else if (operator.equals(SqlStdOperatorTable.CURRENT_DATE)) {
        return cx.getRexBuilder().makeDateLiteral(
            Calcites.jodaToCalciteDateString(
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
