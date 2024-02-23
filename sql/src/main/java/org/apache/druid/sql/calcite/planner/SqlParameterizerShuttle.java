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

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Replaces all {@link SqlDynamicParam} encountered in an {@link SqlNode} tree
 * with a {@link SqlLiteral} if a value binding exists for the parameter, if
 * possible. This is used in tandem with {@link RelParameterizerShuttle}.
 * <p>
 * It is preferable that all parameters are placed here to pick up as many
 * optimizations as possible, but the facilities to convert jdbc types to
 * {@link SqlLiteral} are a bit less rich here than exist for converting a
 * {@link org.apache.calcite.rex.RexDynamicParam} to
 * {@link org.apache.calcite.rex.RexLiteral}, which is why
 * {@link SqlParameterizerShuttle} and {@link RelParameterizerShuttle}
 * both exist.
 * <p>
 * As it turns out, most parameters will be replaced in this shuttle.
 * The one exception are DATE types expressed as integers. For reasons
 * known only to Calcite, the {@code RexBuilder.clean()} method, used by
 * {@code RelParameterizerShuttle}, handles integer values for dates,
 * but the {@code SqlTypeName.createLiteral()} method used here does
 * not. As a result, DATE parameters will be left as parameters to be
 * filled in later. Fortunately, this does not affect optimizations as
 * there are no rules that optimize based on the value of a DATE.
 */
public class SqlParameterizerShuttle extends SqlShuttle
{
  private final PlannerContext plannerContext;

  public SqlParameterizerShuttle(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public SqlNode visit(SqlDynamicParam param)
  {
    if (plannerContext.getParameters().size() <= param.getIndex()) {
      throw unbound(param);
    }
    TypedValue paramBinding = plannerContext.getParameters().get(param.getIndex());
    if (paramBinding == null) {
      throw unbound(param);
    }
    if (paramBinding.value == null) {
      return SqlLiteral.createNull(param.getParserPosition());
    }
    SqlTypeName typeName = SqlTypeName.getNameForJdbcType(paramBinding.type.typeId);
    if (SqlTypeName.APPROX_TYPES.contains(typeName)) {
      return SqlLiteral.createApproxNumeric(paramBinding.value.toString(), param.getParserPosition());
    }
    if (SqlTypeName.TIMESTAMP.equals(typeName) && paramBinding.value instanceof Long) {
      return SqlLiteral.createTimestamp(
          TimestampString.fromMillisSinceEpoch((Long) paramBinding.value),
          DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
          param.getParserPosition()
      );
    }

    if (typeName == SqlTypeName.ARRAY) {
      return createArrayLiteral(paramBinding.value, param.getIndex());
    }
    try {
      // This throws ClassCastException for a DATE parameter given as
      // an Integer. The parameter is left in place and is replaced
      // properly later by RelParameterizerShuttle.
      return typeName.createLiteral(paramBinding.value, param.getParserPosition());
    }
    catch (ClassCastException ignored) {
      // suppress
      return param;
    }
  }

  private static DruidException unbound(SqlDynamicParam param)
  {
    return InvalidSqlInput.exception("No value bound for parameter (position [%s])", param.getIndex() + 1);
  }

  /**
   * Convert an ARRAY parameter to the equivalent of the ARRAY[a, b, ...]
   * syntax. This is not well-supported in the present version of Calcite,
   * so we have to do a bit of roll-our-own code to create the required
   * structure. Supports a limited set of member types. Does not attempt
   * to enforce that all elements have the same type.
   */
  private SqlNode createArrayLiteral(Object value, int posn)
  {
    List<?> list;
    if (value instanceof List) {
      list = (List<?>) value;
    } else {
      list = Arrays.asList((Object[]) value);
    }
    List<SqlNode> args = new ArrayList<>(list.size());
    for (int i = 0, listSize = list.size(); i < listSize; i++) {
      Object element = list.get(i);
      if (element == null) {
        throw InvalidSqlInput.exception("parameter [%d] is an array, with an illegal null at index [%d]", posn + 1, i);
      }
      SqlNode node;
      if (element instanceof String) {
        node = SqlLiteral.createCharString((String) element, SqlParserPos.ZERO);
      } else if (element instanceof Integer || element instanceof Long) {
        // No direct way to create a literal from an Integer or Long, have
        // to parse a string, sadly.
        node = SqlLiteral.createExactNumeric(element.toString(), SqlParserPos.ZERO);
      } else if (element instanceof Boolean) {
        node = SqlLiteral.createBoolean((Boolean) value, SqlParserPos.ZERO);
      } else {
        throw InvalidSqlInput.exception(
            "parameter [%d] is an array, with an illegal value of type [%s] at index [%d]",
            posn + 1,
            value.getClass(),
            i
        );
      }
      args.add(node);
    }
    return SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(
        SqlParserPos.ZERO,
        args
    );
  }
}
