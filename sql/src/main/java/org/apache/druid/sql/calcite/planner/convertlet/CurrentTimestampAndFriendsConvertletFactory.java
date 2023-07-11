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

package org.apache.druid.sql.calcite.planner.convertlet;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;

public class CurrentTimestampAndFriendsConvertletFactory implements DruidConvertletFactory
{
  public static final CurrentTimestampAndFriendsConvertletFactory INSTANCE =
      new CurrentTimestampAndFriendsConvertletFactory();

  /**
   * Use instead of {@link SqlStdOperatorTable#CURRENT_TIMESTAMP} to get the proper default precision.
   */
  private static final SqlFunction CURRENT_TIMESTAMP =
      new CurrentTimestampSqlFunction("CURRENT_TIMESTAMP", SqlTypeName.TIMESTAMP);

  /**
   * Use instead of {@link SqlStdOperatorTable#LOCALTIMESTAMP} to get the proper default precision.
   */
  private static final SqlFunction LOCALTIMESTAMP =
      new CurrentTimestampSqlFunction("LOCALTIMESTAMP", SqlTypeName.TIMESTAMP);

  private static final List<SqlOperator> SQL_OPERATORS =
      ImmutableList.<SqlOperator>builder()
                   .add(CURRENT_TIMESTAMP)
                   .add(SqlStdOperatorTable.CURRENT_TIME)
                   .add(SqlStdOperatorTable.CURRENT_DATE)
                   .add(LOCALTIMESTAMP)
                   .add(SqlStdOperatorTable.LOCALTIME)
                   .build();

  private CurrentTimestampAndFriendsConvertletFactory()
  {
    // Singleton.
  }

  @Override
  public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
  {
    return new CurrentTimestampAndFriendsConvertlet(plannerContext);
  }

  @Override
  public List<SqlOperator> operators()
  {
    return SQL_OPERATORS;
  }

  private static class CurrentTimestampAndFriendsConvertlet implements SqlRexConvertlet
  {
    private final PlannerContext plannerContext;

    private CurrentTimestampAndFriendsConvertlet(PlannerContext plannerContext)
    {
      this.plannerContext = plannerContext;
    }

    @Override
    public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
    {
      final SqlOperator operator = call.getOperator();
      if (CURRENT_TIMESTAMP.equals(operator) || LOCALTIMESTAMP.equals(operator)) {
        int precision = DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION;

        if (call.operandCount() > 0) {
          // Call is CURRENT_TIMESTAMP(precision) or LOCALTIMESTAMP(precision)
          final SqlLiteral precisionLiteral = call.operand(0);
          precision = precisionLiteral.intValue(true);
        }

        return Calcites.jodaToCalciteTimestampLiteral(
            cx.getRexBuilder(),
            plannerContext.getLocalNow(),
            plannerContext.getTimeZone(),
            precision
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
        throw new ISE("Should not have got here, operator was: %s", operator);
      }
    }
  }

  /**
   * Similar to {@link SqlAbstractTimeFunction}, but default precision is
   * {@link DruidTypeSystem#DEFAULT_TIMESTAMP_PRECISION} instead of 0.
   */
  private static class CurrentTimestampSqlFunction extends SqlAbstractTimeFunction
  {
    private final SqlTypeName typeName;

    public CurrentTimestampSqlFunction(final String name, final SqlTypeName typeName)
    {
      super(name, typeName);
      this.typeName = typeName;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding)
    {
      if (opBinding.getOperandCount() == 0) {
        return opBinding.getTypeFactory().createSqlType(typeName, DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION);
      } else {
        return super.inferReturnType(opBinding);
      }
    }
  }
}
