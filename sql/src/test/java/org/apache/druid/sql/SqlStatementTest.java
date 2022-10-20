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

package org.apache.druid.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.SqlPlanningException.PlanningError;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PrepareResult;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.http.SqlQuery;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlStatementTest
{
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();
  private SpecificSegmentsQuerySegmentWalker walker = null;
  private TestRequestLogger testRequestLogger;
  private ListeningExecutorService executorService;
  private SqlStatementFactory sqlStatementFactory;
  private final DefaultQueryConfig defaultQueryConfig = new DefaultQueryConfig(
      ImmutableMap.of("DEFAULT_KEY", "DEFAULT_VALUE"));

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

  @Before
  public void setUp() throws Exception
  {
    final QueryScheduler scheduler = new QueryScheduler(
        5,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new HiLoQueryLaningStrategy(40),
        new ServerConfig()
    )
    {
      @Override
      public <T> Sequence<T> run(Query<?> query, Sequence<T> resultSequence)
      {
        return super.run(
            query,
            new LazySequence<T>(() -> {
              return resultSequence;
            })
        );
      }
    };

    executorService = MoreExecutors.listeningDecorator(Execs.multiThreaded(8, "test_sql_resource_%s"));
    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder.newFolder(), scheduler);

    final PlannerConfig plannerConfig = PlannerConfig.builder().serializeComplexValues(false).build();
    final DruidSchemaCatalog rootSchema = CalciteTests.createMockRootSchema(
        conglomerate,
        walker,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();

    testRequestLogger = new TestRequestLogger();

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        operatorTable,
        macroTable,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );

    this.sqlStatementFactory = new SqlStatementFactory(
        new SqlToolbox(
            CalciteTests.createMockSqlEngine(walker, conglomerate),
            plannerFactory,
            new NoopServiceEmitter(),
            testRequestLogger,
            QueryStackTests.DEFAULT_NOOP_SCHEDULER,
            new AuthConfig(),
            defaultQueryConfig,
            new SqlLifecycleManager()
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
    executorService.shutdownNow();
    executorService.awaitTermination(2, TimeUnit.SECONDS);
  }

  HttpServletRequest request(boolean ok)
  {
    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(null).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, ok);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.replay(req);
    return req;
  }

  //-----------------------------------------------------------------
  // Direct statements: using an auth result for verification.

  private SqlQueryPlus queryPlus(final String sql, final AuthenticationResult authResult)
  {
    return SqlQueryPlus.builder(sql).auth(authResult).build();
  }

  @Test
  public void testDirectHappyPath()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    ResultSet resultSet = stmt.plan();
    assertTrue(resultSet.runnable());
    List<Object[]> results = resultSet.run().getResults().toList();
    assertEquals(1, results.size());
    assertEquals(6L, results.get(0)[0]);
    assertEquals("foo", results.get(0)[1]);
    assertSame(stmt.reporter(), resultSet.reporter());
    assertSame(stmt.resources(), resultSet.resources());
    assertSame(stmt.query(), resultSet.query());
    assertFalse(resultSet.runnable());
    resultSet.close();
    stmt.close();
  }

  @Test
  public void testDirectPlanTwice()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    stmt.plan();
    try {
      stmt.plan();
      fail();
    }
    catch (ISE e) {
      stmt.closeWithError(e);
    }
  }

  @Test
  public void testDirectExecTwice()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    ResultSet resultSet = stmt.plan();
    resultSet.run();
    try {
      resultSet.run();
      fail();
    }
    catch (ISE e) {
      stmt.closeWithError(e);
    }
  }

  @Test
  public void testDirectSyntaxError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    try {
      stmt.execute();
      fail();
    }
    catch (SqlPlanningException e) {
      // Expected
      assertEquals(PlanningError.SQL_PARSE_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testDirectValidationError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.bogus",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    try {
      stmt.execute();
      fail();
    }
    catch (SqlPlanningException e) {
      // Expected
      assertEquals(PlanningError.VALIDATION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testDirectPermissionError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "select count(*) from forbiddenDatasource",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    try {
      stmt.execute();
      fail();
    }
    catch (ForbiddenException e) {
      // Expected
    }
  }

  //-----------------------------------------------------------------
  // HTTP statements: using a servlet request for verification.

  private SqlQuery makeQuery(String sql)
  {
    return new SqlQuery(
        sql,
        null,
        false,
        false,
        false,
        null,
        null
        );
  }

  @Test
  public void testHttpHappyPath()
  {
    HttpStatement stmt = sqlStatementFactory.httpStatement(
        makeQuery("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo"),
        request(true)
        );
    List<Object[]> results = stmt.execute().getResults().toList();
    assertEquals(1, results.size());
    assertEquals(6L, results.get(0)[0]);
    assertEquals("foo", results.get(0)[1]);
  }

  @Test
  public void testHttpSyntaxError()
  {
    HttpStatement stmt = sqlStatementFactory.httpStatement(
        makeQuery("SELECT COUNT(*) AS cnt, 'foo' AS"),
        request(true)
        );
    try {
      stmt.execute();
      fail();
    }
    catch (SqlPlanningException e) {
      // Expected
      assertEquals(PlanningError.SQL_PARSE_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testHttpValidationError()
  {
    HttpStatement stmt = sqlStatementFactory.httpStatement(
        makeQuery("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.bogus"),
        request(true)
        );
    try {
      stmt.execute();
      fail();
    }
    catch (SqlPlanningException e) {
      // Expected
      assertEquals(PlanningError.VALIDATION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testHttpPermissionError()
  {
    HttpStatement stmt = sqlStatementFactory.httpStatement(
        makeQuery("select count(*) from forbiddenDatasource"),
        request(false)
        );
    try {
      stmt.execute();
      fail();
    }
    catch (ForbiddenException e) {
      // Expected
    }
  }

  //-----------------------------------------------------------------
  // Prepared statements: using a prepare/execute model.

  @Test
  public void testPreparedHappyPath()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);

    PrepareResult prepareResult = stmt.prepare();
    RelDataType rowType = prepareResult.getReturnedRowType();
    assertEquals(2, rowType.getFieldCount());
    List<RelDataTypeField> fields = rowType.getFieldList();
    assertEquals("cnt", fields.get(0).getName());
    assertEquals("BIGINT", fields.get(0).getType().toString());
    assertEquals("TheFoo", fields.get(1).getName());
    assertEquals("CHAR(3)", fields.get(1).getType().toString());

    // JDBC supports a prepare once, execute many model
    for (int i = 0; i < 3; i++) {
      List<Object[]> results = stmt
          .execute(Collections.emptyList())
          .execute()
          .getResults()
          .toList();
      assertEquals(1, results.size());
      assertEquals(6L, results.get(0)[0]);
      assertEquals("foo", results.get(0)[1]);
    }
  }

  @Test
  public void testPrepareSyntaxError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);
    try {
      stmt.prepare();
      fail();
    }
    catch (SqlPlanningException e) {
      // Expected
      assertEquals(PlanningError.SQL_PARSE_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testPrepareValidationError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.bogus",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);
    try {
      stmt.prepare();
      fail();
    }
    catch (SqlPlanningException e) {
      // Expected
      assertEquals(PlanningError.VALIDATION_ERROR.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testPreparePermissionError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "select count(*) from forbiddenDatasource",
        CalciteTests.REGULAR_USER_AUTH_RESULT);
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);
    try {
      stmt.prepare();
      fail();
    }
    catch (ForbiddenException e) {
      // Expected
    }
  }

  //-----------------------------------------------------------------
  // Generic tests.

  @Test
  public void testIgnoredQueryContextParametersAreIgnored()
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("select 1 + ?")
        .context(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, "true"))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    Map<String, Object> context = stmt.context();
    Assert.assertEquals(2, context.size());
    // should contain only query id, not bySegment since it is not valid for SQL
    Assert.assertTrue(context.containsKey(QueryContexts.CTX_SQL_QUERY_ID));
  }

  @Test
  public void testDefaultQueryContextIsApplied()
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("select 1 + ?")
        .context(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, "true"))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    Map<String, Object> context = stmt.context();
    Assert.assertEquals(2, context.size());
    // Statement should contain default query context values
    for (String defaultContextKey : defaultQueryConfig.getContext().keySet()) {
      Assert.assertTrue(context.containsKey(defaultContextKey));
    }
  }
}
