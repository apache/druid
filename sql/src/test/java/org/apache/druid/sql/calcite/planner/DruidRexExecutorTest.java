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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringOperatorConversions;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DruidRexExecutorTest extends InitializedNullHandlingTest
{
  private static final SqlOperator OPERATOR = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase("hyper_unique"))
      .operandTypes(SqlTypeFamily.ANY)
      .requiredOperands(0)
      .returnTypeInference(
          opBinding -> RowSignatures.makeComplexType(
              opBinding.getTypeFactory(),
              ColumnType.ofComplex("hyperUnique"),
              true
          )
      )
      .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
      .build();

  private static final PlannerContext PLANNER_CONTEXT = PlannerContext.create(
      "SELECT 1", // The actual query isn't important for this test
      new DruidOperatorTable(
          Collections.emptySet(),
          ImmutableSet.of(new DirectOperatorConversion(OPERATOR, "hyper_unique"))
      ),
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
      null /* Don't need an engine */,
      Collections.emptyMap(),
      Collections.emptySet()
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
    Assert.assertEquals(1, reduced.size());
    Assert.assertEquals(SqlKind.LITERAL, reduced.get(0).getKind());
    Assert.assertEquals(new BigDecimal(30L), ((RexLiteral) reduced.get(0)).getValue());
  }

  @Test
  public void testComplexNotReduced()
  {
    DruidRexExecutor rexy = new DruidRexExecutor(PLANNER_CONTEXT);
    RexNode call = rexBuilder.makeCall(OPERATOR);
    List<RexNode> reduced = new ArrayList<>();
    rexy.reduce(rexBuilder, ImmutableList.of(call), reduced);
    Assert.assertEquals(1, reduced.size());
    Assert.assertEquals(SqlKind.OTHER_FUNCTION, reduced.get(0).getKind());
    Assert.assertEquals(
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
    BasicSqlType basicSqlType = new BasicSqlType(DruidTypeSystem.INSTANCE, SqlTypeName.DECIMAL);
    ArraySqlType arraySqlType = new ArraySqlType(basicSqlType, false);
    List<BigDecimal> elements = ImmutableList.of(BigDecimal.valueOf(50.12), BigDecimal.valueOf(12.1));
    RexNode literal = rexBuilder.makeLiteral(elements, arraySqlType, true);
    rexy.reduce(rexBuilder, ImmutableList.of(literal), reduced);
    Assert.assertEquals(1, reduced.size());
    Assert.assertEquals(
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
    Assert.assertEquals(1, reduced.size());
    Assert.assertEquals(
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
    Assert.assertEquals(1, reduced.size());
    Assert.assertEquals(SqlKind.OTHER_FUNCTION, reduced.get(0).getKind());
    Assert.assertEquals(
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
