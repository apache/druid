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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class ArraySqlAggregator implements SqlAggregator
{
  private static final String NAME = "ARRAY_AGG";
  private static final SqlAggFunction FUNCTION = new ArrayAggFunction();

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
    final List<RexNode> arguments = aggregateCall
        .getArgList()
        .stream()
        .map(i -> Expressions.fromFieldAccess(rowSignature, project, i))
        .collect(Collectors.toList());

    Integer maxSizeBytes = null;
    if (arguments.size() > 1) {
      RexNode maxBytes = arguments.get(1);
      if (!maxBytes.isA(SqlKind.LITERAL)) {
        // maxBytes must be a literal
        return null;
      }
      maxSizeBytes = ((Number) RexLiteral.value(maxBytes)).intValue();
    }
    final DruidExpression arg = Expressions.toDruidExpression(plannerContext, rowSignature, arguments.get(0));
    if (arg == null) {
      // can't translate argument
      return null;
    }
    final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

    final String fieldName;
    final String initialvalue;
    final ColumnType druidType = Calcites.getValueTypeForRelDataTypeFull(aggregateCall.getType());
    final ColumnType elementType;
    if (druidType == null || !druidType.isArray()) {
      initialvalue = "[]";
      elementType = ColumnType.STRING;
    } else {
      initialvalue = ExpressionType.fromColumnTypeStrict(druidType).asTypeString() + "[]";
      elementType = (ColumnType) druidType.getElementType();
    }
    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      fieldName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(arg, elementType);
    }

    if (aggregateCall.isDistinct()) {
      return Aggregation.create(
          new ExpressionLambdaAggregatorFactory(
              name,
              ImmutableSet.of(fieldName),
              null,
              initialvalue,
              null,
              true,
              true,
              false,
              StringUtils.format("array_set_add(\"__acc\", \"%s\")", fieldName),
              StringUtils.format("array_set_add_all(\"__acc\", \"%s\")", name),
              null,
              null,
              maxSizeBytes != null ? new HumanReadableBytes(maxSizeBytes) : null,
              macroTable
          )
      );
    } else {
      return Aggregation.create(
          new ExpressionLambdaAggregatorFactory(
              name,
              ImmutableSet.of(fieldName),
              null,
              initialvalue,
              null,
              true,
              true,
              false,
              StringUtils.format("array_append(\"__acc\", \"%s\")", fieldName),
              StringUtils.format("array_concat(\"__acc\", \"%s\")", name),
              null,
              null,
              maxSizeBytes != null ? new HumanReadableBytes(maxSizeBytes) : null,
              macroTable
          )
      );
    }
  }

  static class ArrayAggReturnTypeInference implements SqlReturnTypeInference
  {
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding sqlOperatorBinding)
    {
      RelDataType type = sqlOperatorBinding.getOperandType(0);
      if (type instanceof RowSignatures.ComplexSqlType) {
        throw new UnsupportedSQLQueryException("Cannot use ARRAY_AGG on complex inputs %s", type);
      }
      return sqlOperatorBinding.getTypeFactory().createArrayType(
          type,
          -1
      );
    }
  }

  private static class ArrayAggFunction extends SqlAggFunction
  {
    private static final ArrayAggReturnTypeInference RETURN_TYPE_INFERENCE = new ArrayAggReturnTypeInference();

    ArrayAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          RETURN_TYPE_INFERENCE,
          InferTypes.ANY_NULLABLE,
          OperandTypes.or(
            OperandTypes.ANY,
            OperandTypes.and(
                OperandTypes.sequence(StringUtils.format("'%s'(expr, maxSizeBytes)", NAME), OperandTypes.ANY, OperandTypes.POSITIVE_INTEGER_LITERAL),
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
            )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.IGNORED
      );
    }
  }
}
