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

package org.apache.druid.sql.calcite.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class ExpressionTestHelper
{
  private static final PlannerContext PLANNER_CONTEXT = PlannerContext.create(
      CalciteTests.createOperatorTable(),
      CalciteTests.createExprMacroTable(),
      new PlannerConfig(),
      ImmutableMap.of(),
      ImmutableList.of(),
      CalciteTests.REGULAR_USER_AUTH_RESULT
  );

  private final RowSignature rowSignature;
  private final Map<String, Object> bindings;
  private final RelDataTypeFactory typeFactory;
  private final RexBuilder rexBuilder;
  private final RelDataType relDataType;

  ExpressionTestHelper(RowSignature rowSignature, Map<String, Object> bindings)
  {
    this.rowSignature = rowSignature;
    this.bindings = bindings;

    this.typeFactory = new JavaTypeFactoryImpl();
    this.rexBuilder = new RexBuilder(typeFactory);
    this.relDataType = RowSignatures.toRelDataType(rowSignature, typeFactory);
  }

  RelDataType createSqlType(SqlTypeName sqlTypeName)
  {
    return typeFactory.createSqlType(sqlTypeName);
  }

  RexNode makeInputRef(String columnName)
  {
    int columnNumber = rowSignature.indexOf(columnName);
    return rexBuilder.makeInputRef(relDataType.getFieldList().get(columnNumber).getType(), columnNumber);
  }

  RexNode getConstantNull()
  {
    return rexBuilder.constantNull();
  }

  RexLiteral makeFlag(Enum flag)
  {
    return rexBuilder.makeFlag(flag);
  }

  RexLiteral makeNullLiteral(SqlTypeName sqlTypeName)
  {
    return rexBuilder.makeNullLiteral(createSqlType(sqlTypeName));
  }

  RexLiteral makeLiteral(String s)
  {
    return rexBuilder.makeLiteral(s);
  }

  RexNode makeLiteral(DateTime timestamp)
  {
    return rexBuilder.makeTimestampLiteral(Calcites.jodaToCalciteTimestampString(timestamp, DateTimeZone.UTC), 0);
  }

  RexNode makeLiteral(Integer integer)
  {
    return rexBuilder.makeLiteral(new BigDecimal(integer), createSqlType(SqlTypeName.INTEGER), true);
  }

  RexNode makeLiteral(Long bigint)
  {
    return rexBuilder.makeLiteral(new BigDecimal(bigint), createSqlType(SqlTypeName.BIGINT), true);
  }

  RexNode makeLiteral(BigDecimal bigDecimal)
  {
    return rexBuilder.makeExactLiteral(bigDecimal);
  }

  RexNode makeLiteral(BigDecimal v, SqlIntervalQualifier intervalQualifier)
  {
    return rexBuilder.makeIntervalLiteral(v, intervalQualifier);
  }

  RexNode makeLiteral(Double d)
  {
    return rexBuilder.makeLiteral(d, createSqlType(SqlTypeName.DOUBLE), true);
  }

  RexNode makeCall(SqlOperator op, RexNode... exprs)
  {
    return rexBuilder.makeCall(op, exprs);
  }

  RexNode makeAbstractCast(RelDataType type, RexNode exp)
  {
    return rexBuilder.makeAbstractCast(type, exp);
  }

  /**
   * @return Representation of variable that is bound in an expression. Intended use is as one of
   * the args to {@link #buildExpectedExpression(String, Object...)}.
   */
  Variable makeVariable(String name)
  {
    return new Variable(name);
  }

  private static class Variable
  {
    private final String name;

    Variable(String name)
    {
      this.name = name;
    }

    @Override
    public String toString()
    {
      return "\"" + name + "\"";
    }
  }

  DruidExpression buildExpectedExpression(String functionName, Object... args)
  {
    String noDelimiter = "";
    String argsString = Arrays.stream(args)
                              .map(ExpressionTestHelper::quoteIfNeeded)
                              .collect(Collectors.joining(","));
    List<String> elements = Arrays.asList(functionName, "(", argsString, ")");
    return DruidExpression.fromExpression(String.join(noDelimiter, elements));
  }

  private static String quoteIfNeeded(@Nullable Object arg)
  {
    if (arg == null) {
      return "null";
    } else if (arg instanceof String) {
      return "'" + arg + "'";
    } else if (arg instanceof Boolean) {
      return (Boolean) arg ? "1" : "0";
    } else {
      return arg.toString();
    }
  }

  void testExpression(
      final SqlTypeName sqlTypeName,
      final SqlOperator op,
      final List<RexNode> exprs,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    RelDataType returnType = createSqlType(sqlTypeName);
    testExpression(rexBuilder.makeCall(returnType, op, exprs), expectedExpression, expectedResult);
  }

  void testExpression(
      final SqlOperator op,
      final RexNode expr,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testExpression(op, Collections.singletonList(expr), expectedExpression, expectedResult);
  }

  void testExpression(
      final SqlOperator op,
      final List<? extends RexNode> exprs,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testExpression(rexBuilder.makeCall(op, exprs), expectedExpression, expectedResult);
  }

  void testExpression(
      final RexNode rexNode,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    DruidExpression expression = Expressions.toDruidExpression(PLANNER_CONTEXT, rowSignature, rexNode);
    Assert.assertEquals("Expression for: " + rexNode, expectedExpression, expression);

    ExprEval<?> result = Parser.parse(expression.getExpression(), PLANNER_CONTEXT.getExprMacroTable())
                               .eval(Parser.withMap(bindings));

    Assert.assertEquals("Result for: " + rexNode, expectedResult, result.value());
  }

  void testFilter(
      final SqlOperator op,
      final List<? extends RexNode> exprs,
      final List<VirtualColumn> expectedVirtualColumns,
      final DimFilter expectedFilter,
      final boolean expectedResult
  )
  {
    final RexNode rexNode = rexBuilder.makeCall(op, exprs);
    final VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(rowSignature);

    final DimFilter filter = Expressions.toFilter(PLANNER_CONTEXT, rowSignature, virtualColumnRegistry, rexNode);
    Assert.assertEquals("Filter for: " + rexNode, expectedFilter, filter);

    final List<VirtualColumn> virtualColumns =
        filter.getRequiredColumns()
              .stream()
              .map(virtualColumnRegistry::getVirtualColumn)
              .filter(Objects::nonNull)
              .sorted(Comparator.comparing(VirtualColumn::getOutputName))
              .collect(Collectors.toList());

    Assert.assertEquals(
        "Virtual columns for: " + rexNode,
        expectedVirtualColumns.stream()
                              .sorted(Comparator.comparing(VirtualColumn::getOutputName))
                              .collect(Collectors.toList()),
        virtualColumns
    );

    final ValueMatcher matcher = expectedFilter.toFilter().makeMatcher(
        RowBasedColumnSelectorFactory.create(
            RowAdapters.standardRow(),
            () -> new MapBasedRow(0L, bindings),
            rowSignature,
            false
        )
    );

    Assert.assertEquals("Result for: " + rexNode, expectedResult, matcher.matches());
  }
}
