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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class InformationSchemaTest extends BaseCalciteQueryTest
{
  private InformationSchema informationSchema;
  private SqlTestFramework qf;

  @Before
  public void setUp()
  {
    qf = queryFramework();
    DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
        CalciteTests.INJECTOR,
        qf.conglomerate(),
        qf.walker(),
        new PlannerConfig(),
        null,
        new NoopDruidSchemaManager(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CatalogResolver.NULL_RESOLVER
    );

    informationSchema = new InformationSchema(
        rootSchema,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        qf.operatorTable()
    );
  }

  @Test
  public void testGetTableNamesMap()
  {
    Assert.assertEquals(
        ImmutableSet.of("SCHEMATA", "TABLES", "COLUMNS", "ROUTINES"),
        informationSchema.getTableNames()
    );
  }

  @Test
  public void testScanRoutinesTable()
  {
    DruidOperatorTable operatorTable = qf.operatorTable();
    InformationSchema.RoutinesTable routinesTable = new InformationSchema.RoutinesTable(operatorTable);
    DataContext dataContext = createDataContext();

    List<Object[]> rows = routinesTable.scan(dataContext).toList();

    Assert.assertTrue("There should at least be 1 built-in function that gets statically loaded by default",
                      rows.size() > 0);
    RelDataType rowType = routinesTable.getRowType(new JavaTypeFactoryImpl());
    Assert.assertEquals(6, rowType.getFieldCount());

    for (Object[] row : rows) {
      Assert.assertEquals(rowType.getFieldCount(), row.length);
      Assert.assertEquals("druid", row[0]);
      Assert.assertEquals("INFORMATION_SCHEMA", row[1]);
      Assert.assertNotNull(row[2]);
      Assert.assertNotNull(row[3]);
      String isAggregator = row[4].toString();
      Assert.assertTrue(isAggregator.contains("YES") || isAggregator.contains("NO"));
      // nothing to validate for signatures as it may be not be present if operandTypeChecker is not defined.
    }
  }

  @Test
  public void testScanRoutinesTableWithCustomOperators()
  {
    Set<SqlOperatorConversion> sqlOperatorConversions = customOperatorsToOperatorConversions();
    final List<SqlOperator> sqlOperators = sqlOperatorConversions.stream()
                                                           .map(SqlOperatorConversion::calciteOperator)
                                                           .collect(Collectors.toList());

    DruidOperatorTable mockOperatorTable = Mockito.mock(DruidOperatorTable.class);
    Mockito.when(mockOperatorTable.getOperatorList()).thenReturn(sqlOperators);

    InformationSchema.RoutinesTable routinesTable = new InformationSchema.RoutinesTable(mockOperatorTable);
    DataContext dataContext = createDataContext();

    List<Object[]> rows = routinesTable.scan(dataContext).toList();

    Assert.assertNotNull(rows);
    Assert.assertEquals("There should be exactly 2 rows; any non-function syntax operator should get filtered out",
                        2, rows.size());
    Object[] expectedRow1 = {"druid", "INFORMATION_SCHEMA", "FOO", "FUNCTION", "NO", "'FOO([<ANY>])'"};
    Assert.assertTrue(rows.stream().anyMatch(row -> Arrays.equals(row, expectedRow1)));

    Object[] expectedRow2 = {"druid", "INFORMATION_SCHEMA", "BAR", "FUNCTION", "NO", "'BAR(<INTEGER>, <INTEGER>)'"};
    Assert.assertTrue(rows.stream().anyMatch(row -> Arrays.equals(row, expectedRow2)));
  }

  @Test
  public void testScanRoutinesTableWithAnEmptyOperatorTable()
  {
    DruidOperatorTable mockOperatorTable = Mockito.mock(DruidOperatorTable.class);

    // This should never happen. Adding a test case, so we don't hit a NPE and instead return gracefully.
    List<SqlOperator> emptyOperatorList = new ArrayList<>();
    Mockito.when(mockOperatorTable.getOperatorList()).thenReturn(emptyOperatorList);

    InformationSchema.RoutinesTable routinesTable = new InformationSchema.RoutinesTable(mockOperatorTable);
    DataContext dataContext = createDataContext();

    List<Object[]> rows = routinesTable.scan(dataContext).toList();

    Assert.assertNotNull(rows);
    Assert.assertEquals(0, rows.size());
  }

  private static Set<SqlOperatorConversion> customOperatorsToOperatorConversions()
  {
    final SqlOperator operator1 = OperatorConversions
        .operatorBuilder("FOO")
        .operandTypes(SqlTypeFamily.ANY)
        .requiredOperandCount(0)
        .returnTypeInference(
            opBinding -> RowSignatures.makeComplexType(
                opBinding.getTypeFactory(),
                ColumnType.ofComplex("fooComplex"),
                true
            )
        )
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();


    final SqlOperator operator2 = OperatorConversions
        .operatorBuilder("BAR")
        .operandTypes(SqlTypeFamily.NUMERIC)
        .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
        .requiredOperandCount(2)
        .returnTypeInference(
            opBinding -> RowSignatures.makeComplexType(
                opBinding.getTypeFactory(),
                ColumnType.ofComplex("barComplex"),
                true
            )
        )
        .functionCategory(SqlFunctionCategory.NUMERIC)
        .build();

    final Set<SqlOperatorConversion> extractionOperators = new HashSet<>();
    extractionOperators.add(new DirectOperatorConversion(operator1, "foo_fn"));
    extractionOperators.add(new DirectOperatorConversion(operator2, "bar_fn"));

    // Make a few non-function syntax operators
    extractionOperators.add(new DirectOperatorConversion(createNonFunctionOperator("not_a_fn_1"), "not_a_fn_1"));
    extractionOperators.add(new DirectOperatorConversion(createNonFunctionOperator("not_a_fn_2"), "not_a_fn_2"));
    return extractionOperators;
  }

  /**
   * A test factory to create non-function SQL synatx operators.
   */
  private static SqlOperator createNonFunctionOperator(String operatorName)
  {
    return new SqlOperator(operatorName, SqlKind.PLUS_PREFIX, 0, false, null,
                           InferTypes.RETURN_TYPE, OperandTypes.VARIADIC) {
      @Override
      public SqlSyntax getSyntax()
      {
        return SqlSyntax.POSTFIX;
      }

      @Override
      public boolean isDynamicFunction()
      {
        return true;
      }
    };
  }

  private DataContext createDataContext()
  {
    return new DataContext()
    {
      @Override
      public SchemaPlus getRootSchema()
      {
        return null;
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return null;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        return null;
      }

      @Override
      public Object get(String authorizerName)
      {
        return CalciteTests.SUPER_USER_AUTH_RESULT;
      }
    };
  }
}
