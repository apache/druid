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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;

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
   * @param plannerContext SQL planner context
   * @param rowSignature   input row signature
   * @param call           aggregate call object
   * @param project        project that should be applied before aggregation; may be null
   *
   * @return list of expressions corresponding to aggregator arguments, or null if any cannot be translated
   */
  @Nullable
  public static List<DruidExpression> getArgumentsForSimpleAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final AggregateCall call,
      @Nullable final Project project
  )
  {
    final List<DruidExpression> args = call
        .getArgList()
        .stream()
        .map(i -> Expressions.fromFieldAccess(rowSignature, project, i))
        .map(rexNode -> toDruidExpressionForSimpleAggregator(plannerContext, rowSignature, rexNode))
        .collect(Collectors.toList());

    if (args.stream().noneMatch(Objects::isNull)) {
      return args;
    } else {
      return null;
    }
  }

  private static DruidExpression toDruidExpressionForSimpleAggregator(
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
         || rowSignature.getColumnType(druidExpression.getDirectColumn()).orElse(null) == ValueType.STRING)) {
      // Aggregators are unable to implicitly cast strings to numbers.
      // So remove the simple extraction, which forces the expression to be used instead of the direct column access.
      return druidExpression.map(simpleExtraction -> null, Function.identity());
    } else {
      return druidExpression;
    }
  }
}
