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
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
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
import org.apache.druid.sql.calcite.schema.ConstantDruidSchemaCatalogProvider;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ExpressionTestHelper
{
  private static final JoinableFactoryWrapper JOINABLE_FACTORY_WRAPPER = CalciteTests.createJoinableFactoryWrapper();
  private static final PlannerToolbox PLANNER_TOOLBOX = new PlannerToolbox(
      CalciteTests.createOperatorTable(),
      CalciteTests.createExprMacroTable(),
      CalciteTests.getJsonMapper(),
      new PlannerConfig(),
      new ConstantDruidSchemaCatalogProvider(
          new DruidSchemaCatalog(
              EasyMock.createMock(SchemaPlus.class),
              ImmutableMap.of(
                  "druid", new NamedDruidSchema(EasyMock.createMock(DruidSchema.class), "druid"),
                  NamedViewSchema.NAME, new NamedViewSchema(EasyMock.createMock(ViewSchema.class))
              )
          )
      ),
      JOINABLE_FACTORY_WRAPPER,
      CatalogResolver.NULL_RESOLVER,
      "druid",
      new CalciteRulesManager(ImmutableSet.of()),
      CalciteTests.TEST_AUTHORIZER_MAPPER,
      AuthConfig.newBuilder().build(),
      NoopPolicyEnforcer.instance(),
      new DruidHookDispatcher()
  );
  public static final PlannerContext PLANNER_CONTEXT = PlannerContext.create(
      PLANNER_TOOLBOX,
      "SELECT 1", // The actual query isn't important for this test
      null, /* Don't need SQL node */
      null, /* Don't need engine */
      null, /* Don't need authentication result */
      Collections.emptySet(),
      Collections.emptyMap(),
      null
  );

  public static final PlannerContext PLANNER_CONTEXT_WITH_EXTRACTION_FNS = PlannerContext.create(
      PLANNER_TOOLBOX,
      "SELECT 1", // The actual query isn't important for this test
      null, /* Don't need SQL node */
      null, /* Don't need engine */
      null, /* Don't need authentication result */
      Collections.emptySet(),
      Collections.singletonMap(PlannerContext.CTX_SQL_USE_EXTRACTION_FNS, true),
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
   * the args to {@link #expectedFunctionCall(ColumnType, String, Object...)}.
   */
  Variable makeVariable(String name)
  {
    return new Variable(name);
  }

  /**
   * @return Representation of a null literal of the given type, which may itself be null for an untyped null literal.
   * Intended use is as one of the args to {@link #expectedFunctionCall(ColumnType, String, Object...)}, where a plain
   * null arg is typed as STRING.
   */
  NullLiteral makeTypedNullLiteral(@Nullable ColumnType type)
  {
    return new NullLiteral(type);
  }

  private static class NullLiteral
  {
    @Nullable
    private final ColumnType type;

    NullLiteral(@Nullable ColumnType type)
    {
      this.type = type;
    }
  }

  private static class Variable
  {
    private final String name;

    Variable(String name)
    {
      this.name = name;
    }
  }

  /**
   * Builds the expected {@link DruidExpression} of a function call. Each of the {@code args} is converted to an
   * argument expression: {@link #makeVariable(String)} becomes a column of the type in the row signature (it is an
   * error if the name is not in the row signature), a {@link DruidExpression} is used as is, and {@link String},
   * {@link Boolean}, {@link Long}, {@link Integer} and {@link Double} become literals. A plain null becomes a null
   * literal typed as STRING; use {@link #makeTypedNullLiteral(ColumnType)} for a null of any other type.
   */
  DruidExpression expectedFunctionCall(ColumnType type, String functionName, Object... args)
  {
    return DruidExpression.ofFunctionCall(
        type,
        functionName,
        Arrays.stream(args).map(this::toArgumentExpression).collect(Collectors.toList())
    );
  }

  private DruidExpression toArgumentExpression(@Nullable Object arg)
  {
    if (arg == null) {
      return DruidExpression.ofLiteral(ColumnType.STRING, DruidExpression.nullLiteral());
    } else if (arg instanceof NullLiteral) {
      return DruidExpression.ofLiteral(((NullLiteral) arg).type, DruidExpression.nullLiteral());
    } else if (arg instanceof DruidExpression) {
      return (DruidExpression) arg;
    } else if (arg instanceof Variable) {
      final String name = ((Variable) arg).name;
      final ColumnType columnType = rowSignature.getColumnType(name).orElseThrow(
          () -> new IllegalArgumentException("Variable [" + name + "] is not in the row signature")
      );
      return DruidExpression.ofColumn(columnType, name);
    } else if (arg instanceof String) {
      return DruidExpression.ofStringLiteral((String) arg);
    } else if (arg instanceof Boolean) {
      return DruidExpression.ofLiteral(ColumnType.LONG, (Boolean) arg ? "1" : "0");
    } else if (arg instanceof Long || arg instanceof Integer) {
      return DruidExpression.ofLiteral(ColumnType.LONG, arg.toString());
    } else if (arg instanceof Double) {
      return DruidExpression.ofLiteral(ColumnType.DOUBLE, arg.toString());
    }
    throw new IllegalArgumentException("Unsupported argument type: " + arg.getClass());
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
    testExpression(PLANNER_CONTEXT, rexNode, expectedExpression, expectedResult);
  }

  /**
   * Same as {@link #testExpression(RexNode, DruidExpression, Object)}, but plans with
   * {@link #PLANNER_CONTEXT_WITH_EXTRACTION_FNS}, i.e. with sqlUseExtractionFns enabled.
   */
  void testExpressionWithExtractionFns(
      final RexNode rexNode,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testExpression(PLANNER_CONTEXT_WITH_EXTRACTION_FNS, rexNode, expectedExpression, expectedResult);
  }

  private void testExpression(
      final PlannerContext plannerContext,
      final RexNode rexNode,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    DruidExpression expression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    Assertions.assertNotNull(expression);
    Assertions.assertEquals(expectedExpression, expression, "Expression for: " + rexNode);

    ExprEval<?> result = plannerContext.parseExpression(expression.getExpression())
                                        .eval(expressionBindings);

    Assertions.assertEquals(expectedResult, result.value(), "Result for: " + rexNode);
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
        false,
        Set.of()
    );

    final DimFilter filter = Expressions.toFilter(PLANNER_CONTEXT, rowSignature, virtualColumnRegistry, rexNode);
    Assertions.assertEquals(expectedFilter, filter, "Filter for: " + rexNode);

    final List<VirtualColumn> virtualColumns =
        filter.getRequiredColumns()
              .stream()
              .map(virtualColumnRegistry::getVirtualColumn)
              .filter(Objects::nonNull)
              .sorted(Comparator.comparing(VirtualColumn::getOutputName))
              .collect(Collectors.toList());

    Assertions.assertEquals(
        expectedVirtualColumns.stream()
                              .sorted(Comparator.comparing(VirtualColumn::getOutputName))
                              .collect(Collectors.toList()),
        virtualColumns,
        "Virtual columns for: " + rexNode
    );

    final ValueMatcher matcher = expectedFilter.toFilter().makeMatcher(
        new VirtualizedColumnSelectorFactory(
            RowBasedColumnSelectorFactory.create(
                RowAdapters.standardRow(),
                () -> new MapBasedRow(0L, bindings),
                rowSignature,
                false
            ),
            VirtualColumns.create(virtualColumns)
        )
    );

    Assertions.assertEquals(expectedResult, matcher.matches(false), "Result for: " + rexNode);
  }
}
