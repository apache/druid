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

package org.apache.druid.sql.avatica;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DruidStatementTest extends CalciteTestBase
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder.newFolder());
    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    SchemaPlus rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        operatorTable,
        macroTable,
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME
    );
    this.sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(plannerFactory);
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testSignature()
  {
    final String sql = "SELECT * FROM druid.foo";
    final DruidStatement statement = new DruidStatement("", 0, null, sqlLifecycleFactory.factorize(), () -> {
    }).prepare(sql, -1, AllowAllAuthenticator.ALLOW_ALL_RESULT);

    // Check signature.
    final Meta.Signature signature = statement.getSignature();
    Assert.assertEquals(Meta.CursorFactory.ARRAY, signature.cursorFactory);
    Assert.assertEquals(Meta.StatementType.SELECT, signature.statementType);
    Assert.assertEquals(sql, signature.sql);
    Assert.assertEquals(
        Lists.newArrayList(
            Lists.newArrayList("__time", "TIMESTAMP", "java.lang.Long"),
            Lists.newArrayList("cnt", "BIGINT", "java.lang.Long"),
            Lists.newArrayList("dim1", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("dim2", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("dim3", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("m1", "FLOAT", "java.lang.Float"),
            Lists.newArrayList("m2", "DOUBLE", "java.lang.Double"),
            Lists.newArrayList("unique_dim1", "OTHER", "java.lang.Object")
        ),
        Lists.transform(
            signature.columns,
            new Function<ColumnMetaData, List<String>>()
            {
              @Override
              public List<String> apply(final ColumnMetaData columnMetaData)
              {
                return Lists.newArrayList(
                    columnMetaData.label,
                    columnMetaData.type.name,
                    columnMetaData.type.rep.clazz.getName()
                );
              }
            }
        )
    );
  }

  @Test
  public void testSelectAllInFirstFrame()
  {
    final String sql = "SELECT __time, cnt, dim1, dim2, m1 FROM druid.foo";
    final DruidStatement statement = new DruidStatement("", 0, null, sqlLifecycleFactory.factorize(), () -> {
    }).prepare(sql, -1, AllowAllAuthenticator.ALLOW_ALL_RESULT);

    // First frame, ask for all rows.
    Meta.Frame frame = statement.execute(Collections.emptyList()).nextFrame(DruidStatement.START_OFFSET, 6);
    Assert.assertEquals(
        Meta.Frame.create(
            0,
            true,
            Lists.newArrayList(
                new Object[]{DateTimes.of("2000-01-01").getMillis(), 1L, "", "a", 1.0f},
                new Object[]{
                    DateTimes.of("2000-01-02").getMillis(),
                    1L,
                    "10.1",
                    NullHandling.defaultStringValue(),
                    2.0f
                },
                new Object[]{DateTimes.of("2000-01-03").getMillis(), 1L, "2", "", 3.0f},
                new Object[]{DateTimes.of("2001-01-01").getMillis(), 1L, "1", "a", 4.0f},
                new Object[]{DateTimes.of("2001-01-02").getMillis(), 1L, "def", "abc", 5.0f},
                new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc", NullHandling.defaultStringValue(), 6.0f}
            )
        ),
        frame
    );
    Assert.assertTrue(statement.isDone());
  }

  @Test
  public void testSelectSplitOverTwoFrames()
  {
    final String sql = "SELECT __time, cnt, dim1, dim2, m1 FROM druid.foo";
    final DruidStatement statement = new DruidStatement("", 0, null, sqlLifecycleFactory.factorize(), () -> {
    }).prepare(sql, -1, AllowAllAuthenticator.ALLOW_ALL_RESULT);

    // First frame, ask for 2 rows.
    Meta.Frame frame = statement.execute(Collections.emptyList()).nextFrame(DruidStatement.START_OFFSET, 2);
    Assert.assertEquals(
        Meta.Frame.create(
            0,
            false,
            Lists.newArrayList(
                new Object[]{DateTimes.of("2000-01-01").getMillis(), 1L, "", "a", 1.0f},
                new Object[]{
                    DateTimes.of("2000-01-02").getMillis(),
                    1L,
                    "10.1",
                    NullHandling.defaultStringValue(),
                    2.0f
                }
            )
        ),
        frame
    );
    Assert.assertFalse(statement.isDone());

    // Last frame, ask for all remaining rows.
    frame = statement.nextFrame(2, 10);
    Assert.assertEquals(
        Meta.Frame.create(
            2,
            true,
            Lists.newArrayList(
                new Object[]{DateTimes.of("2000-01-03").getMillis(), 1L, "2", "", 3.0f},
                new Object[]{DateTimes.of("2001-01-01").getMillis(), 1L, "1", "a", 4.0f},
                new Object[]{DateTimes.of("2001-01-02").getMillis(), 1L, "def", "abc", 5.0f},
                new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc", NullHandling.defaultStringValue(), 6.0f}
            )
        ),
        frame
    );
    Assert.assertTrue(statement.isDone());
  }
}
