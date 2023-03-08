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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Util;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.builtin.NestedDataOperatorConversions;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidConvertletTable implements SqlRexConvertletTable
{
  public static final SqlFunction COALESCE_FUNCTION = OperatorConversions.operatorBuilder("COALESCE")
                                                                          .sqlKind(SqlKind.COALESCE)
                                                                          .functionCategory(SqlFunctionCategory.SYSTEM)
                                                                          .operandTypeChecker(OperandTypes.SAME_VARIADIC)
                                                                          .returnTypeInference(
                                                                              ReturnTypes.cascade(
                                                                                  ReturnTypes.LEAST_RESTRICTIVE,
                                                                                  SqlTypeTransforms.FORCE_NULLABLE
                                                                              )
                                                                          )
                                                                          .build();

  public static final SqlFunction NVL_FUNCTION = OperatorConversions.operatorBuilder("NVL")
                                                                    .sqlKind(SqlKind.NVL)
                                                                    .functionCategory(SqlFunctionCategory.SYSTEM)
                                                                    .operandTypeChecker(OperandTypes.SAME_VARIADIC)
                                                                    .returnTypeInference(
                                                                        ReturnTypes.cascade(
                                                                            ReturnTypes.LEAST_RESTRICTIVE,
                                                                            SqlTypeTransforms.FORCE_NULLABLE
                                                                        )
                                                                    )
                                                                    .build();

  // Apply a convertlet that doesn't do anything other than a "dumb" call translation.
  private static final SqlRexConvertlet BYPASS_CONVERTLET = StandardConvertletTable.INSTANCE::convertCall;

  private static final List<DruidConvertletFactory> DRUID_CONVERTLET_FACTORIES =
      ImmutableList.<DruidConvertletFactory>builder()
                   .add(CurrentTimestampAndFriendsConvertletFactory.INSTANCE)
                   .add(TimeInIntervalConvertletFactory.INSTANCE)
                   .add(NestedDataOperatorConversions.DRUID_JSON_VALUE_CONVERTLET_FACTORY_INSTANCE)
                   .add(CoalesceToNvlConvertletFactory.INSTANCE)
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
                   .add(SqlStdOperatorTable.NULLIF)
                   .build();

  private final Map<SqlOperator, SqlRexConvertlet> table;

  public DruidConvertletTable(final PlannerContext plannerContext)
  {
    this.table = createConvertletMap(plannerContext);
  }

  @Override
  public SqlRexConvertlet get(SqlCall call)
  {
    if (shouldBypassStandardConvertlets(call)) {
      return BYPASS_CONVERTLET;
    } else {
      final SqlRexConvertlet convertlet = table.get(call.getOperator());
      return convertlet != null ? convertlet : StandardConvertletTable.INSTANCE.get(call);
    }
  }

  public static List<SqlOperator> knownOperators()
  {
    final ArrayList<SqlOperator> retVal = new ArrayList<>(STANDARD_CONVERTLET_OPERATORS);

    for (final DruidConvertletFactory convertletFactory : DRUID_CONVERTLET_FACTORIES) {
      retVal.addAll(convertletFactory.operators());
    }

    return retVal;
  }

  private static Map<SqlOperator, SqlRexConvertlet> createConvertletMap(final PlannerContext plannerContext)
  {
    final Map<SqlOperator, SqlRexConvertlet> table = new HashMap<>();

    for (DruidConvertletFactory convertletFactory : DRUID_CONVERTLET_FACTORIES) {
      final SqlRexConvertlet convertlet = convertletFactory.createConvertlet(plannerContext);

      for (final SqlOperator operator : convertletFactory.operators()) {
        table.put(operator, convertlet);
      }
    }

    return table;
  }

  /**
   * Returns true if we should intercept a convertlet and use {@link #BYPASS_CONVERTLET} to handle a function directly
   * with an operator conversion
   */
  private static boolean shouldBypassStandardConvertlets(SqlCall call)
  {
    if (call.getKind() == SqlKind.EXTRACT && call.getOperandList().get(1).getKind() != SqlKind.LITERAL) {
      // Avoid using the standard convertlet for EXTRACT(TIMEUNIT FROM col), since we want to handle it directly
      // in ExtractOperationConversion.
      return true;
    }

    // avoid using standard convertlet for nvl to push to direct operator conversion
    return NVL_FUNCTION.equals(call.getOperator());
  }

  public static final class CoalesceToNvlConvertletFactory implements DruidConvertletFactory
  {
    private static final CoalesceToNvlConvertletFactory INSTANCE = new CoalesceToNvlConvertletFactory();

    @Override
    public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
    {
      return (cx, call) -> {
        if (call.operandCount() == 1) {
          // No CASE needed
          return cx.convertExpression(call.operand(0));
        }
        // coalesce, if 2 argument, rewrite as NVL, else pass through to standard converlet
        if (call.operandCount() == 2) {
          SqlNode rewrite = NVL_FUNCTION.createCall(SqlParserPos.ZERO, call.operand(0), call.operand(1));
          return cx.convertExpression(rewrite);
        }

        SqlParserPos pos = call.getParserPosition();

        SqlNodeList whenList = new SqlNodeList(pos);
        SqlNodeList thenList = new SqlNodeList(pos);

        // todo: optimize when know operand is not null.

        for (SqlNode operand : Util.skipLast(call.getOperandList())) {
          whenList.add(SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, operand));
          thenList.add(SqlNode.clone(operand));
        }
        SqlNode elseExpr = Util.last(call.getOperandList());
        return cx.convertExpression(SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr));
      };
    }

    @Override
    public List<SqlOperator> operators()
    {
      return ImmutableList.of(COALESCE_FUNCTION);
    }
  }
}
