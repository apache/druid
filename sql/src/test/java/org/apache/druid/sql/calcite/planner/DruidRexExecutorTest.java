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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringOperatorConversions;
import org.apache.druid.sql.calcite.expression.builtin.TimeParseOperatorConversion;
import org.apache.druid.sql.calcite.schema.ConstantDruidSchemaCatalogProvider;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class DruidRexExecutorTest extends InitializedNullHandlingTest
{
  private static final SqlOperator OPERATOR = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase("hyper_unique"))
      .operandTypes(SqlTypeFamily.ANY)
      .requiredOperandCount(0)
      .returnTypeInference(
          opBinding -> RowSignatures.makeComplexType(
              opBinding.getTypeFactory(),
              ColumnType.ofComplex("hyperUnique"),
              true
          )
      )
      .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
      .build();

  private static final PlannerToolbox PLANNER_TOOLBOX = new PlannerToolbox(
      new DruidOperatorTable(
          Collections.emptySet(),
          ImmutableSet.of(new DirectOperatorConversion(OPERATOR, "hyper_unique"))
      ),
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
      CalciteTests.createJoinableFactoryWrapper(),
      CatalogResolver.NULL_RESOLVER,
      "druid",
      new CalciteRulesManager(ImmutableSet.of()),
      CalciteTests.TEST_AUTHORIZER_MAPPER,
      AuthConfig.newBuilder().build(),
      NoopPolicyEnforcer.instance(),
      new DruidHookDispatcher()
  );
  private static final PlannerContext PLANNER_CONTEXT = PlannerContext.create(
      PLANNER_TOOLBOX,
      "SELECT 1", // The actual query isn't important for this test
      null, /* Don't need a SQL node */
      null, /* Don't need an engine */
      null, /* Don't need an authentication result */
      Collections.emptySet(),
      Collections.emptyMap(),
      null
  );

  private final RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);

  @Test
  public void testLongsReduced()
  {
    RexNode call = rexBuilder.makeCall(
        SqlStdOperatorTable.MULTIPLY,
        rexBuilder.makeLiteral(
            new BigDecimal(10L),
            typeFactory.createSqlType(SqlTypeName.BIGINT), true
        ),
        rexBuilder.makeLiteral(
            new BigDecimal(3L),
            typeFactory.createSqlType(SqlTypeName.BIGINT), true
        )
    );

    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    List<RexNode> reduced = new ArrayList<>();
    rexy.reduce(rexBuilder, ImmutableList.of(call), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(SqlKind.LITERAL, reduced.get(0).getKind());
    Assertions.assertEquals(new BigDecimal(30L), ((RexLiteral) reduced.get(0)).getValue());
  }

  @Test
  public void testCastDateReduced()
  {
    // CAST('2010-01-01' AS DATE)
    RexNode call = rexBuilder.makeCall(
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DATE),
        SqlStdOperatorTable.CAST,
        Collections.singletonList(rexBuilder.makeLiteral("2010-01-01"))
    );

    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    List<RexNode> reduced = new ArrayList<>();
    rexy.reduce(rexBuilder, ImmutableList.of(call), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(SqlKind.LITERAL, reduced.get(0).getKind());
    Assertions.assertEquals(
        rexBuilder.makeDateLiteral(
            Calcites.jodaToCalciteDateString(
                DateTimes.of("2010-01-01"),
                DateTimeZone.UTC
            )
        ),
        reduced.get(0)
    );
  }

  @Test
  public void testTimeParseReduced()
  {
    // TIME_PARSE('2010-01-01T02:03:04Z')
    RexNode call = rexBuilder.makeCall(
        new TimeParseOperatorConversion().calciteOperator(),
        rexBuilder.makeLiteral("2010-01-01T02:03:04Z")
    );

    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    List<RexNode> reduced = new ArrayList<>();
    rexy.reduce(rexBuilder, ImmutableList.of(call), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(SqlKind.LITERAL, reduced.get(0).getKind());
    Assertions.assertEquals(
        Calcites.jodaToCalciteTimestampLiteral(
            rexBuilder,
            DateTimes.of("2010-01-01T02:03:04Z"),
            DateTimeZone.UTC,
            DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION
        ),
        reduced.get(0)
    );
  }

  @Test
  public void testTimeParseUnparseableReduced()
  {
    // TIME_PARSE('not a timestamp')
    RexNode call = rexBuilder.makeCall(
        new TimeParseOperatorConversion().calciteOperator(),
        rexBuilder.makeLiteral("not a timestamp")
    );

    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    List<RexNode> reduced = new ArrayList<>();
    rexy.reduce(rexBuilder, ImmutableList.of(call), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(SqlKind.LITERAL, reduced.get(0).getKind());
    Assertions.assertTrue(RexLiteral.isNullLiteral(reduced.get(0)));
  }

  @Test
  public void testComplexNotReduced()
  {
    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    RexNode call = rexBuilder.makeCall(OPERATOR);
    List<RexNode> reduced = new ArrayList<>();
    rexy.reduce(rexBuilder, ImmutableList.of(call), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(SqlKind.OTHER_FUNCTION, reduced.get(0).getKind());
    Assertions.assertEquals(
        CalciteTestBase.makeExpression(ColumnType.ofComplex("hyperUnique"), "hyper_unique()"),
        Expressions.toDruidExpression(
            PLANNER_CONTEXT,
            RowSignature.builder().build(),
            reduced.get(0)
        )
    );
  }

  @Test
  public void testArrayOfDoublesReduction()
  {
    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    List<RexNode> reduced = new ArrayList<>();
    BasicSqlType basicSqlType = new BasicSqlType(DruidTypeSystem.INSTANCE, SqlTypeName.DECIMAL, 19, 10);
    ArraySqlType arraySqlType = new ArraySqlType(basicSqlType, false);
    List<BigDecimal> elements = ImmutableList.of(BigDecimal.valueOf(50.12), BigDecimal.valueOf(12.1));
    RexNode literal = rexBuilder.makeLiteral(elements, arraySqlType, true);
    rexy.reduce(rexBuilder, ImmutableList.of(literal), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(
        DruidExpression.ofExpression(
            ColumnType.DOUBLE_ARRAY,
            DruidExpression.functionCall("array"),
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.DOUBLE, "50.12"),
                DruidExpression.ofLiteral(ColumnType.DOUBLE, "12.1")
            )
        ),
        Expressions.toDruidExpression(
            PLANNER_CONTEXT,
            RowSignature.empty(),
            reduced.get(0)
        )
    );
  }

  @Test
  public void testArrayOfLongsReduction()
  {
    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    List<RexNode> reduced = new ArrayList<>();
    BasicSqlType basicSqlType = new BasicSqlType(DruidTypeSystem.INSTANCE, SqlTypeName.INTEGER);
    ArraySqlType arraySqlType = new ArraySqlType(basicSqlType, false);
    List<BigDecimal> elements = ImmutableList.of(BigDecimal.valueOf(50), BigDecimal.valueOf(12));
    RexNode literal = rexBuilder.makeLiteral(elements, arraySqlType, true);
    rexy.reduce(rexBuilder, ImmutableList.of(literal), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(
        DruidExpression.ofExpression(
            ColumnType.LONG_ARRAY,
            DruidExpression.functionCall("array"),
            ImmutableList.of(
                DruidExpression.ofLiteral(ColumnType.LONG, "50"),
                DruidExpression.ofLiteral(ColumnType.LONG, "12")
            )
        ),
        Expressions.toDruidExpression(
            PLANNER_CONTEXT,
            RowSignature.empty(),
            reduced.get(0)
        )
    );
  }

  @Test
  public void testArrayOfStringLiteralsReduction()
  {
    for (final SqlTypeName typeName : SqlTypeName.CHAR_TYPES) {
      assertLiteralArrayReduction(typeName, Arrays.asList("", "a'b", "back\\slash", "中文", null));
      assertLiteralArrayReduction(typeName, Collections.singletonList(null));
    }
  }

  @Test
  public void testStringArrayExpressionUsesEvaluator()
  {
    final RexNode array = rexBuilder.makeCall(
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, rexBuilder.makeLiteral("a"), rexBuilder.makeLiteral("b"))
    );
    // CONCAT requires evaluation rather than literal-array reuse.
    Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array));
    final RexCall reduced = reduceArrayWithEvaluator(array);
    Assertions.assertEquals(1, reduced.getOperands().size());
    Assertions.assertEquals("ab", RexLiteral.stringValue(reduced.getOperands().get(0)));
    Assertions.assertEquals(SqlTypeName.CHAR, reduced.getOperands().get(0).getType().getSqlTypeName());
    Assertions.assertEquals(2, reduced.getOperands().get(0).getType().getPrecision());
  }

  @Test
  public void testIntegerLiteralArraysReduction()
  {
    for (final SqlTypeName typeName : SqlTypeName.INT_TYPES) {
      assertLiteralArrayReduction(
          typeName,
          Arrays.asList(BigDecimal.valueOf(-1), BigDecimal.ZERO, null, BigDecimal.TEN)
      );
    }
  }

  @Test
  public void testLongLiteralArrayBoundaries()
  {
    for (final List<BigDecimal> values : Arrays.<List<BigDecimal>>asList(
        Arrays.asList(BigDecimal.valueOf(Long.MIN_VALUE), null, BigDecimal.valueOf(Long.MAX_VALUE)),
        Collections.singletonList(null)
    )) {
      assertLiteralArrayReduction(SqlTypeName.BIGINT, values);
    }
  }

  @Test
  public void testIntegerArrayExpressionUsesEvaluator()
  {
    final RexNode array = rexBuilder.makeCall(
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rexBuilder.makeBigintLiteral(BigDecimal.ONE),
                           rexBuilder.makeBigintLiteral(BigDecimal.TEN))
    );
    // Addition requires evaluation rather than literal-array reuse.
    Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array));
    final RexCall reduced = reduceArrayWithEvaluator(array);
    Assertions.assertEquals(1, reduced.getOperands().size());
    Assertions.assertEquals(BigDecimal.valueOf(11), RexLiteral.value(reduced.getOperands().get(0)));
    Assertions.assertEquals(SqlTypeName.BIGINT, reduced.getOperands().get(0).getType().getSqlTypeName());
  }

  @Test
  public void testLiteralArrayReductionRejectsUnsupportedTypes()
  {
    for (final SqlTypeName typeName : Arrays.asList(
        SqlTypeName.DECIMAL,
        SqlTypeName.DOUBLE,
        SqlTypeName.BOOLEAN,
        SqlTypeName.DATE
    )) {
      final RelDataType elementType = typeFactory.createSqlType(typeName);
      final RexNode array = rexBuilder.makeCall(
          typeFactory.createArrayType(elementType, -1), SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
          ImmutableList.of(rexBuilder.makeNullLiteral(elementType))
      );
      // Unsupported component types retain evaluator fallback, even when their only element is null.
      Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array), typeName.toString());
    }
    // A scalar literal is not an array constructor.
    Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, rexBuilder.makeLiteral("a")));
    // A non-array call is not eligible for this fast path.
    Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(
        rexBuilder,
        rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, rexBuilder.makeLiteral("a"), rexBuilder.makeLiteral("b"))
    ));
  }

  @Test
  public void testLiteralArrayReductionRejectsCastsAndMismatchedOperands()
  {
    final RelDataType stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 30);
    final RelDataType arrayType = typeFactory.createArrayType(stringType, -1);
    for (final RexNode operand : ImmutableList.of(
        rexBuilder.makeAbstractCast(stringType, rexBuilder.makeLiteral("a"), false),
        rexBuilder.makeBigintLiteral(BigDecimal.ONE)
    )) {
      final RexNode array = rexBuilder.makeCall(
          arrayType,
          SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
          ImmutableList.of(operand)
      );
      // CAST calls and integer operands in a string array cannot be reused as matching string literals.
      Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array));
    }
  }

  @Test
  public void testLiteralArrayReductionRejectsDifferentElementTypes()
  {
    final RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 3);
    final RelDataType arrayType = typeFactory.createArrayType(charType, -1);
    final RexNode array = rexBuilder.makeCall(
        arrayType,
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        ImmutableList.of(rexBuilder.makeLiteral("a"), rexBuilder.makeLiteral("abc"))
    );
    // CHAR(1) does not match CHAR(3); the evaluator must perform the required padding.
    Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array));
    final RexCall reduced = reduceArrayWithEvaluator(array);
    Assertions.assertEquals(2, reduced.getOperands().size());
    Assertions.assertEquals("a  ", RexLiteral.stringValue(reduced.getOperands().get(0)));
    Assertions.assertEquals("abc", RexLiteral.stringValue(reduced.getOperands().get(1)));
    for (final RexNode operand : reduced.getOperands()) {
      Assertions.assertEquals(SqlTypeName.CHAR, operand.getType().getSqlTypeName());
      Assertions.assertEquals(3, operand.getType().getPrecision());
    }
  }

  @Test
  public void testLiteralArrayReductionRejectsMixedIntegerTypes()
  {
    final RelDataType arrayType = typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1);
    final RexNode array = rexBuilder.makeCall(
        arrayType,
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        ImmutableList.of(
            rexBuilder.makeLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.INTEGER), false),
            rexBuilder.makeBigintLiteral(BigDecimal.TEN)
        )
    );
    // INTEGER and BIGINT operands require normalization to the BIGINT component type.
    Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array));
    final RexCall reduced = reduceArrayWithEvaluator(array);
    Assertions.assertEquals(2, reduced.getOperands().size());
    Assertions.assertEquals(BigDecimal.ONE, RexLiteral.value(reduced.getOperands().get(0)));
    Assertions.assertEquals(BigDecimal.TEN, RexLiteral.value(reduced.getOperands().get(1)));
    for (final RexNode operand : reduced.getOperands()) {
      Assertions.assertEquals(SqlTypeName.BIGINT, operand.getType().getSqlTypeName());
    }
  }

  @Test
  public void testLiteralArrayReductionRejectsDifferentCharsets()
  {
    assertCharacterTypeMismatch(StandardCharsets.ISO_8859_1, "primary");
  }

  @Test
  public void testLiteralArrayReductionRejectsDifferentCollations()
  {
    assertCharacterTypeMismatch(StandardCharsets.UTF_8, "tertiary");
  }

  private void assertCharacterTypeMismatch(final Charset operandCharset, final String operandStrength)
  {
    final RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 1);
    final RelDataType componentType = typeFactory.createTypeWithCharsetAndCollation(
        charType,
        StandardCharsets.UTF_8,
        new SqlCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US, StandardCharsets.UTF_8, "primary")
    );
    final RelDataType operandType = typeFactory.createTypeWithCharsetAndCollation(
        charType,
        operandCharset,
        new SqlCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US, operandCharset, operandStrength)
    );
    final RexNode operand = rexBuilder.makeLiteral("a", operandType, false);
    Assertions.assertInstanceOf(RexLiteral.class, operand);
    Assertions.assertEquals(operandType, operand.getType());
    Assertions.assertNotEquals(componentType, operand.getType());
    final RelDataType arrayType = typeFactory.createArrayType(componentType, -1);
    final RexNode array = rexBuilder.makeCall(
        arrayType,
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        ImmutableList.of(operand)
    );
    // Matching CHAR lengths are insufficient when the charset or collation differs.
    Assertions.assertNull(DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array));
    final RexCall reduced = reduceArrayWithEvaluator(array);
    Assertions.assertEquals(1, reduced.getOperands().size());
    final RexNode reducedOperand = reduced.getOperands().get(0);
    Assertions.assertEquals("a", RexLiteral.stringValue(reducedOperand));
    Assertions.assertEquals(componentType, reducedOperand.getType());
  }

  private void assertLiteralArrayReduction(final SqlTypeName typeName, final List<?> values)
  {
    final RelDataType elementType = SqlTypeName.CHAR_TYPES.contains(typeName)
                                    ? typeFactory.createSqlType(typeName, 30)
                                    : typeFactory.createSqlType(typeName);
    final RelDataType arrayType = typeFactory.createArrayType(
        typeFactory.createTypeWithNullability(elementType, true), -1
    );
    final List<RexNode> operands = new ArrayList<>();
    for (final Object value : values) {
      final RexNode operand = value == null
                              ? rexBuilder.makeNullLiteral(elementType)
                              : rexBuilder.makeLiteral(value, elementType, false);
      Assertions.assertInstanceOf(RexLiteral.class, operand);
      operands.add(operand);
    }
    final RexNode array = rexBuilder.makeCall(arrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands);
    final RexNode reduced = DruidRexExecutor.tryReduceLiteralArray(rexBuilder, array);
    if (typeName == SqlTypeName.VARCHAR && values.stream().anyMatch(value -> value != null)) {
      // makeLiteral(..., false) creates CHAR literals here, which do not match the VARCHAR component type.
      Assertions.assertNull(reduced);
    } else {
      // Matching literals, including typed nulls, reuse the exact array despite nullability differences.
      Assertions.assertSame(array, reduced);
    }
  }

  private RexCall reduceArrayWithEvaluator(final RexNode array)
  {
    final List<RexNode> reduced = new ArrayList<>();
    new DruidRexExecutor(PLANNER_CONTEXT).reduce(rexBuilder, ImmutableList.of(array), reduced);
    Assertions.assertEquals(1, reduced.size());
    final RexCall reducedArray = Assertions.assertInstanceOf(RexCall.class, reduced.get(0));
    Assertions.assertEquals(SqlKind.ARRAY_VALUE_CONSTRUCTOR, reducedArray.getKind());
    return reducedArray;
  }

  @Test
  public void testMultiValueStringNotReduced()
  {
    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    RexNode call = rexBuilder.makeCall(
        MultiValueStringOperatorConversions.StringToMultiString.SQL_FUNCTION,
        rexBuilder.makeLiteral("a,b,c"),
        rexBuilder.makeLiteral(",")
    );
    List<RexNode> reduced = new ArrayList<>();
    rexy.reduce(rexBuilder, ImmutableList.of(call), reduced);
    Assertions.assertEquals(1, reduced.size());
    Assertions.assertEquals(SqlKind.OTHER_FUNCTION, reduced.get(0).getKind());
    Assertions.assertEquals(
        DruidExpression.ofExpression(
            ColumnType.STRING,
            DruidExpression.functionCall("string_to_array"),
            ImmutableList.of(
                DruidExpression.ofStringLiteral("a,b,c"),
                DruidExpression.ofStringLiteral(",")
            )
        ),
        Expressions.toDruidExpression(
            PLANNER_CONTEXT,
            RowSignature.builder().build(),
            reduced.get(0)
        )
    );
  }
}
