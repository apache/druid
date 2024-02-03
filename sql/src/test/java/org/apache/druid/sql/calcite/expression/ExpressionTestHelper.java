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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.VirtualizedColumnSelectorFactory;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.ExpressionParserImpl;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerToolbox;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.easymock.EasyMock;
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

public class ExpressionTestHelper
{
  private static final JoinableFactoryWrapper JOINABLE_FACTORY_WRAPPER = CalciteTests.createJoinableFactoryWrapper();
  private static final PlannerToolbox PLANNER_TOOLBOX = new PlannerToolbox(
      CalciteTests.createOperatorTable(),
      CalciteTests.createExprMacroTable(),
      CalciteTests.getJsonMapper(),
      new PlannerConfig(),
      new DruidSchemaCatalog(
          EasyMock.createMock(SchemaPlus.class),
          ImmutableMap.of(
              "druid", new NamedDruidSchema(EasyMock.createMock(DruidSchema.class), "druid"),
              NamedViewSchema.NAME, new NamedViewSchema(EasyMock.createMock(ViewSchema.class))
          )
      ),
      JOINABLE_FACTORY_WRAPPER,
      CatalogResolver.NULL_RESOLVER,
      "druid",
      new CalciteRulesManager(ImmutableSet.of()),
      CalciteTests.TEST_AUTHORIZER_MAPPER,
      AuthConfig.newBuilder().build()
  );
  public static final PlannerContext PLANNER_CONTEXT = PlannerContext.create(
      PLANNER_TOOLBOX,
      "SELECT 1", // The actual query isn't important for this test
      null, /* Don't need engine */
      Collections.emptyMap(),
      null
  );

  private final RowSignature rowSignature;
  private final Map<String, Object> bindings;
  private final Expr.ObjectBinding expressionBindings;
  private final RelDataTypeFactory typeFactory;
  private final RexBuilder rexBuilder;
  private final RelDataType relDataType;

  ExpressionTestHelper(RowSignature rowSignature, Map<String, Object> bindings)
  {
    this.rowSignature = rowSignature;
    this.bindings = bindings;
    this.expressionBindings = new Expr.ObjectBinding()
    {
      @Nullable
      @Override
      public Object get(String name)
      {
        return bindings.get(name);
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return rowSignature.getType(name);
      }
    };
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
    return Calcites.jodaToCalciteTimestampLiteral(
        rexBuilder,
        timestamp,
        DateTimeZone.UTC,
        DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION
    );
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
    return CalciteTestBase.makeExpression(String.join(noDelimiter, elements));
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

  /**
   * @deprecated use {@link #testExpression(SqlOperator, RexNode, DruidExpression, Object)} instead which does a
   * deep comparison of {@link DruidExpression} instead of just comparing the output of
   * {@link DruidExpression#getExpression()}
   */
  @Deprecated
  void testExpressionString(
      final SqlOperator op,
      final List<? extends RexNode> exprs,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testExpression(rexBuilder.makeCall(op, exprs), expectedExpression, expectedResult, false);
  }

  void testExpression(
      final RexNode rexNode,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testExpression(rexNode, expectedExpression, expectedResult, true);
  }

  void testExpression(
      final RexNode rexNode,
      final DruidExpression expectedExpression,
      final Object expectedResult,
      final boolean deepCompare
  )
  {
    DruidExpression expression = Expressions.toDruidExpression(PLANNER_CONTEXT, rowSignature, rexNode);
    Assert.assertNotNull(expression);
    if (deepCompare) {
      Assert.assertEquals("Expression for: " + rexNode, expectedExpression, expression);
    } else {
      Assert.assertEquals("Expression for: " + rexNode, expectedExpression.getExpression(), expression.getExpression());
    }

    ExprEval<?> result = PLANNER_CONTEXT.parseExpression(expression.getExpression())
                                        
                                        .eval(expressionBindings);

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
    final VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        rowSignature,
        new ExpressionParserImpl(PLANNER_TOOLBOX.exprMacroTable()),
        false
    );

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
        new VirtualizedColumnSelectorFactory(
            RowBasedColumnSelectorFactory.create(
                RowAdapters.standardRow(),
                () -> new MapBasedRow(0L, bindings),
                rowSignature,
                false,
                false
            ),
            VirtualColumns.create(virtualColumns)
        )
    );

    Assert.assertEquals("Result for: " + rexNode, expectedResult, matcher.matches(false));
  }
}
