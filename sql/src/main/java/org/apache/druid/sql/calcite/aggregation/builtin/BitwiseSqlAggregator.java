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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BitwiseSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction XOR_FUNCTION = new BitwiseXorSqlAggFunction();

  public enum Op
  {
    AND {
      @Override
      SqlAggFunction getCalciteFunction()
      {
        return SqlStdOperatorTable.BIT_AND;
      }

      @Override
      String getDruidFunction()
      {
        return "bitwiseAnd";
      }
    },
    OR {
      @Override
      SqlAggFunction getCalciteFunction()
      {
        return SqlStdOperatorTable.BIT_OR;
      }

      @Override
      String getDruidFunction()
      {
        return "bitwiseOr";
      }
    },
    XOR {
      @Override
      SqlAggFunction getCalciteFunction()
      {
        // newer versions of calcite have this built-in so someday we can drop this...
        return XOR_FUNCTION;
      }

      @Override
      String getDruidFunction()
      {
        return "bitwiseXor";
      }
    };

    abstract SqlAggFunction getCalciteFunction();
    abstract String getDruidFunction();
  };

  private final Op op;

  public BitwiseSqlAggregator(Op op)
  {
    this.op = op;
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return op.getCalciteFunction();
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

    final DruidExpression arg = arguments.get(0);
    final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

    final String fieldName;
    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      fieldName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(arg, ColumnType.LONG);
    }

    return Aggregation.create(
        new FilteredAggregatorFactory(
            new ExpressionLambdaAggregatorFactory(
                name,
                ImmutableSet.of(fieldName),
                null,
                "0",
                null,
                null,
                false,
                false,
                StringUtils.format("%s(\"__acc\", \"%s\")", op.getDruidFunction(), fieldName),
                null,
                null,
                null,
                null,
                macroTable
            ),
            new NotDimFilter(new SelectorDimFilter(fieldName, null, null))
        )
    );
  }

  private static class BitwiseXorSqlAggFunction extends SqlAggFunction
  {
    BitwiseXorSqlAggFunction()
    {
      super(
          "BIT_XOR",
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          InferTypes.ANY_NULLABLE,
          OperandTypes.EXACT_NUMERIC,
          SqlFunctionCategory.NUMERIC,
          false,
          false,
          Optionality.IGNORED
      );
    }
  }
}
