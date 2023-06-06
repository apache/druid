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
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperator;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class InformationSchemaTest extends BaseCalciteQueryTest
{
  private InformationSchema informationSchema;
  private SqlTestFramework qf;
  @Before
  public void setUp() throws Exception
  {
    qf = queryFramework();
    DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
        CalciteTests.INJECTOR,
        qf.conglomerate(),
        qf.walker(),
        new PlannerConfig(),
        null,
        new NoopDruidSchemaManager(),
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );

    informationSchema = new InformationSchema(
        rootSchema,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        qf.operatorTable()
    );
  }

  @Test
  public void testGetRoutinesMap()
  {
    Assert.assertEquals(
        ImmutableSet.of("SCHEMATA", "TABLES", "COLUMNS", "ROUTINES"),
        informationSchema.getTableNames()
    );
  }

  @Test
  public void testGetOperators()
  {
    DruidOperatorTable operatorTable = qf.operatorTable();
    InformationSchema.RoutinesTable routinesTable = new InformationSchema.RoutinesTable(operatorTable);
    DataContext dataContext = createDataContext();

    List<Object[]> rows = routinesTable.scan(dataContext).toList();

    List<SqlOperator> operatorList = operatorTable.getOperatorList();

    // todo: clean this up and perhaps check all rows?
    Object[] row0 = rows.get(0);
    Assert.assertEquals("druid", row0[0].toString());
    Assert.assertEquals("INFORMATION_SCHEMA", row0[1].toString());
    Assert.assertEquals(operatorList.get(0).getName(), row0[2].toString());
    Assert.assertEquals(operatorList.get(0).getSyntax().toString(), row0[3].toString());
    Assert.assertEquals(operatorList.get(0).isAggregator() ? "YES" : "NO", row0[4].toString());
    Assert.assertEquals(operatorList.get(0).getAllowedSignatures(), row0[5].toString());
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
