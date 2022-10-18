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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.avatica.DruidJdbcResultSet.ResultFetcherFactory;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
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
  private static String SUB_QUERY_WITH_ORDER_BY =
        "select T20.F13 as F22\n"
      + "from (SELECT DISTINCT dim1 as F13 FROM druid.foo T10) T20\n"
      + "order by T20.F13 ASC";
  private static String SELECT_FROM_FOO =
      "SELECT __time, cnt, dim1, dim2, m1 FROM druid.foo";
  private static String SELECT_STAR_FROM_FOO =
      "SELECT * FROM druid.foo";

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
  private SqlStatementFactory sqlStatementFactory;

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder.newFolder());
    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        operatorTable,
        macroTable,
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );
    this.sqlStatementFactory = CalciteTests.createSqlStatementFactory(
        CalciteTests.createMockSqlEngine(walker, conglomerate),
        plannerFactory
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  //-----------------------------------------------------------------
  // Druid JDBC Statement
  //
  // The JDBC Statement class starts "empty", then allows executing
  // one statement at a time. Executing a second automatically closes
  // the result set from the first. Each statement takes a new query.
  // Parameters are not generally used in this pattern.

  private DruidJdbcStatement jdbcStatement()
  {
    return new DruidJdbcStatement(
        "",
        0,
        Collections.emptyMap(),
        sqlStatementFactory,
        new ResultFetcherFactory(AvaticaServerConfig.DEFAULT_FETCH_TIMEOUT_MS)
    );
  }

  @Test
  public void testSubQueryWithOrderByDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SUB_QUERY_WITH_ORDER_BY,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  @Test
  public void testFetchPastEOFDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SUB_QUERY_WITH_ORDER_BY,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
      try {
        statement.nextFrame(6, 6);
        Assert.fail();
      }
      catch (Exception e) {
        // Expected: can't work with an auto-closed result set.
      }
    }
  }

  /**
   * Ensure an error is thrown if the execution step is skipped.
   */
  @Test
  public void testSkipExecuteDirect()
  {
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // Error: no call to execute;
      statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.fail();
    }
    catch (Exception e) {
      // Expected
    }
  }

  /**
   * Ensure an error is thrown if the client attempts to fetch from a
   * statement after its result set is closed.
   */
  @Test
  public void testFetchAfterResultCloseDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SUB_QUERY_WITH_ORDER_BY,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      statement.closeResultSet();
      statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.fail();
    }
    catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testSubQueryWithOrderByDirectTwice()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SUB_QUERY_WITH_ORDER_BY,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );

      // Do it again. JDBC says we can reuse statements sequentially.
      Assert.assertTrue(statement.isDone());
      statement.execute(queryPlus, -1);
      frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  private Meta.Frame subQueryWithOrderByResults()
  {
    return Meta.Frame.create(
        0,
        true,
        Lists.newArrayList(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectAllInFirstFrameDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SELECT_FROM_FOO,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
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
  }

  /**
   * Test results spread over two frames. Also checks various state-related
   * methods.
   */
  @Test
  public void testSelectSplitOverTwoFramesDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SELECT_FROM_FOO,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {

      // First frame, ask for 2 rows.
      statement.execute(queryPlus, -1);
      Assert.assertEquals(0, statement.getCurrentOffset());
      Assert.assertFalse(statement.isDone());
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());
      Assert.assertEquals(2, statement.getCurrentOffset());

      // Last frame, ask for all remaining rows.
      frame = statement.nextFrame(2, 10);
      Assert.assertEquals(
          secondFrameResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  /**
   * Verify that JDBC automatically closes the first result set when we
   * open a second for the same statement.
   */
  @Test
  public void testTwoFramesAutoCloseDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SELECT_FROM_FOO,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for 2 rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());

      // Do it again. Closes the prior result set.
      statement.execute(queryPlus, -1);
      frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());

      // Last frame, ask for all remaining rows.
      frame = statement.nextFrame(2, 10);
      Assert.assertEquals(
          secondFrameResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  /**
   * Test that closing a statement with pending results automatically
   * closes the underlying result set.
   */
  @Test
  public void testTwoFramesCloseWithResultSetDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SELECT_FROM_FOO,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for 2 rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());

      // Leave result set open; close statement.
    }
  }

  private Meta.Frame firstFrameResults()
  {
    return Meta.Frame.create(
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
    );
  }

  private Meta.Frame secondFrameResults()
  {
    return Meta.Frame.create(
        2,
        true,
        Lists.newArrayList(
            new Object[]{DateTimes.of("2000-01-03").getMillis(), 1L, "2", "", 3.0f},
            new Object[]{DateTimes.of("2001-01-01").getMillis(), 1L, "1", "a", 4.0f},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), 1L, "def", "abc", 5.0f},
            new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc", NullHandling.defaultStringValue(), 6.0f}
        )
    );
  }

  @Test
  public void testSignatureDirect()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SELECT_STAR_FROM_FOO,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcStatement statement = jdbcStatement()) {
      // Check signature.
      statement.execute(queryPlus, -1);
      verifySignature(statement.getSignature());
    }
  }

  @SuppressWarnings("unchecked")
  private void verifySignature(Meta.Signature signature)
  {
    Assert.assertEquals(Meta.CursorFactory.ARRAY, signature.cursorFactory);
    Assert.assertEquals(Meta.StatementType.SELECT, signature.statementType);
    Assert.assertEquals(SELECT_STAR_FROM_FOO, signature.sql);
    Assert.assertEquals(
        Lists.newArrayList(
            Lists.newArrayList("__time", "TIMESTAMP", "java.lang.Long"),
            Lists.newArrayList("dim1", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("dim2", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("dim3", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("cnt", "BIGINT", "java.lang.Number"),
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

  //-----------------------------------------------------------------
  // Druid JDBC Prepared Statement
  //
  // The JDBC PreparedStatement class starts with, then allows executing
  // the statement sequentially, typically with a set of parameters.

  private DruidJdbcPreparedStatement jdbcPreparedStatement(SqlQueryPlus queryPlus)
  {
    return new DruidJdbcPreparedStatement(
        "",
        0,
        sqlStatementFactory.preparedStatement(queryPlus),
        Long.MAX_VALUE,
        new ResultFetcherFactory(AvaticaServerConfig.DEFAULT_FETCH_TIMEOUT_MS)
    );
  }

  @Test
  public void testSubQueryWithOrderByPrepared()
  {
    final String sql = "select T20.F13 as F22  from (SELECT DISTINCT dim1 as F13 FROM druid.foo T10) T20 order by T20.F13 ASC";
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        sql,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {
      statement.prepare();
      // First frame, ask for all rows.
      statement.execute(Collections.emptyList());
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  @Test
  public void testSubQueryWithOrderByPreparedTwice()
  {
    final String sql = "select T20.F13 as F22  from (SELECT DISTINCT dim1 as F13 FROM druid.foo T10) T20 order by T20.F13 ASC";
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        sql,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {
      statement.prepare();
      statement.execute(Collections.emptyList());
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );

      // Do it again. JDBC says we can reuse prepared statements sequentially.
      Assert.assertTrue(statement.isDone());
      statement.execute(Collections.emptyList());
      frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  @Test
  public void testSignaturePrepared()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        SELECT_STAR_FROM_FOO,
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    try (final DruidJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {
      statement.prepare();
      verifySignature(statement.getSignature());
    }
  }

  @Test
  public void testParameters()
  {
    SqlQueryPlus queryPlus = new SqlQueryPlus(
        "SELECT COUNT(*) AS cnt FROM sys.servers WHERE servers.host = ?",
        null,
        null,
        AllowAllAuthenticator.ALLOW_ALL_RESULT
    );
    Meta.Frame expected = Meta.Frame.create(0, true, Collections.singletonList(new Object[] {1L}));
    List<TypedValue> matchingParams = Collections.singletonList(TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "dummy"));
    try (final DruidJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {

      // PreparedStatement protocol: prepare once...
      statement.prepare();

      // Execute many times. First time.
      statement.execute(matchingParams);
      Meta.Frame frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          expected,
          frame
      );

      // Again, same value.
      statement.execute(matchingParams);
      frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          expected,
          frame
      );

      // Again, no matches.
      statement.execute(
          Collections.singletonList(
              TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "foo")));
      frame = statement.nextFrame(AbstractDruidJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          Meta.Frame.create(0, true, Collections.emptyList()),
          frame
      );
    }
  }
}
