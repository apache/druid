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

package org.apache.druid.sql.calcite.aggregation;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Aggregations
{
  private Aggregations()
  {
    // No instantiation.
  }

  /**
   * Get Druid expressions that correspond to "simple" aggregator inputs. This is used by standard sum/min/max
   * aggregators, which have the following properties:
   *
   * 1) They can take direct field accesses or expressions as inputs.
   * 2) They cannot implicitly cast strings to numbers when using a direct field access.
   *
   * Unlike {@link #toDruidExpressionForNumericAggregator}, this does not reject string literals, because it is also
   * used by aggregators that accept strings, such as COUNT and SINGLE_VALUE.
   *
   * @param plannerContext SQL planner context
   * @param call           aggregate call object
   * @param inputAccessor  gives access to input fields and schema
   *
   * @return list of expressions corresponding to aggregator arguments, or null if any cannot be translated
   */
  @Nullable
  public static List<DruidExpression> getArgumentsForSimpleAggregator(
      final PlannerContext plannerContext,
      final AggregateCall call,
      final InputAccessor inputAccessor
  )
  {
    final List<DruidExpression> args = call
        .getArgList()
        .stream()
        .map(i -> inputAccessor.getField(i))
        .map(rexNode -> toDruidExpressionWithStringColumnCast(plannerContext, inputAccessor.getInputRowSignature(), rexNode))
        .collect(Collectors.toList());

    if (args.stream().noneMatch(Objects::isNull)) {
      return args;
    } else {
      return null;
    }
  }

  /**
   * Translate a Calcite {@link RexNode} to a Druid expression for the aggregators that require numeric type inputs.
   * The returned expression can keep an explicit cast from strings to numbers when the column consumed by
   * the expression is the string type.
   *
   * Consider using {@link Expressions#toDruidExpression(PlannerContext, RowSignature, RexNode)} for projections
   * or the aggregators that don't require numeric inputs.
   *
   * String literals are converted to numbers the same way as string columns, but since their value is known at
   * planning time, a string literal that cannot be parsed as a number is rejected with an {@link InvalidSqlInput}
   * error instead of silently producing nulls at runtime.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @return DruidExpression referring to fields in rowOrder, or null if not possible to translate
   */
  public static DruidExpression toDruidExpressionForNumericAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    validateStringLiteralIsNumeric(rexNode);
    return toDruidExpressionWithStringColumnCast(plannerContext, rowSignature, rexNode);
  }

  @Nullable
  private static DruidExpression toDruidExpressionWithStringColumnCast(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final DruidExpression druidExpression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    if (druidExpression == null) {
      return null;
    }

    if (druidExpression.isSimpleExtraction() &&
        (!druidExpression.isDirectColumnAccess()
         || rowSignature.getColumnType(druidExpression.getDirectColumn()).map(type -> type.is(ValueType.STRING)).orElse(false))) {
      // Aggregators are unable to implicitly cast strings to numbers.
      // So remove the simple extraction, which forces the expression to be used instead of the direct column access.
      return druidExpression.map(simpleExtraction -> null, Function.identity());
    } else {
      return druidExpression;
    }
  }

  /**
   * Throws an {@link InvalidSqlInput} error if the given node is a non-null string literal that cannot be parsed as a
   * number. See {@link Calcites#isNumericString(String)}.
   */
  private static void validateStringLiteralIsNumeric(final RexNode rexNode)
  {
    if (!(rexNode instanceof RexLiteral) || !SqlTypeFamily.CHARACTER.contains(rexNode.getType())) {
      return;
    }

    final String value = ((RexLiteral) rexNode).getValueAs(String.class);
    if (value != null && !Calcites.isNumericString(value)) {
      throw Calcites.nonNumericStringLiteralError("Aggregator input", value);
    }
  }
}
