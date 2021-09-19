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

package org.apache.druid.sql.calcite.aggregation.builtin;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StringSqlAggregator implements SqlAggregator
{
  private static final String NAME = "STRING_AGG";
  private static final SqlAggFunction FUNCTION = new StringAggFunction();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    final List<DruidExpression> arguments = aggregateCall
        .getArgList()
        .stream()
        .map(i -> Expressions.fromFieldAccess(rowSignature, project, i))
        .map(rexNode -> Expressions.toDruidExpression(plannerContext, rowSignature, rexNode))
        .collect(Collectors.toList());

    if (arguments.stream().anyMatch(Objects::isNull)) {
      return null;
    }

    RexNode separatorNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(1)
    );
    if (!separatorNode.isA(SqlKind.LITERAL)) {
      // separator must be a literal
      return null;
    }
    String separator = RexLiteral.stringValue(separatorNode);

    if (separator == null) {
      // separator must not be null
      return null;
    }

    Integer maxSizeBytes = null;
    if (arguments.size() > 2) {
      RexNode maxBytes = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );
      if (!maxBytes.isA(SqlKind.LITERAL)) {
        // maxBytes must be a literal
        return null;
      }
      maxSizeBytes = ((Number) RexLiteral.value(maxBytes)).intValue();
    }
    final DruidExpression arg = arguments.get(0);
    final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

    final String initialvalue = "[]";
    final ValueType elementType = ValueType.STRING;
    final String fieldName;
    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      VirtualColumn vc = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(plannerContext, arg, elementType);
      fieldName = vc.getOutputName();
    }

    final String finalizer = StringUtils.format("if(array_length(o) == 0, null, array_to_string(o, '%s'))", separator);
    final NotDimFilter dimFilter = new NotDimFilter(new SelectorDimFilter(fieldName, null, null));
    if (aggregateCall.isDistinct()) {
      return Aggregation.create(
          // string_agg ignores nulls
          new FilteredAggregatorFactory(
              new ExpressionLambdaAggregatorFactory(
                  name,
                  ImmutableSet.of(fieldName),
                  null,
                  initialvalue,
                  null,
                  true,
                  StringUtils.format("array_set_add(\"__acc\", \"%s\")", fieldName),
                  StringUtils.format("array_set_add_all(\"__acc\", \"%s\")", name),
                  null,
                  finalizer,
                  maxSizeBytes != null ? new HumanReadableBytes(maxSizeBytes) : null,
                  macroTable
              ),
              dimFilter
          )
      );
    } else {
      return Aggregation.create(
          // string_agg ignores nulls
          new FilteredAggregatorFactory(
              new ExpressionLambdaAggregatorFactory(
                  name,
                  ImmutableSet.of(fieldName),
                  null,
                  initialvalue,
                  null,
                  true,
                  StringUtils.format("array_append(\"__acc\", \"%s\")", fieldName),
                  StringUtils.format("array_concat(\"__acc\", \"%s\")", name),
                  null,
                  finalizer,
                  maxSizeBytes != null ? new HumanReadableBytes(maxSizeBytes) : null,
                  macroTable
              ),
              dimFilter
          )
      );
    }
  }

  private static class StringAggFunction extends SqlAggFunction
  {
    StringAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          opBinding ->
              Calcites.createSqlTypeWithNullability(opBinding.getTypeFactory(), SqlTypeName.VARCHAR, true),
          InferTypes.ANY_NULLABLE,
          OperandTypes.or(
              OperandTypes.and(
                  OperandTypes.sequence(
                      StringUtils.format("'%s'(expr, separator)", NAME),
                      OperandTypes.ANY,
                      OperandTypes.STRING
                  ),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING)
              ),
              OperandTypes.and(
                  OperandTypes.sequence(
                      StringUtils.format("'%s'(expr, separator, maxSizeBytes)", NAME),
                      OperandTypes.ANY,
                      OperandTypes.STRING,
                      OperandTypes.POSITIVE_INTEGER_LITERAL
                  ),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC)
              )
          ),
          SqlFunctionCategory.STRING,
          false,
          false,
          Optionality.IGNORED
      );
    }
  }
}
