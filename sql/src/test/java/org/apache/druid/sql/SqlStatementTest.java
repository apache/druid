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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.CancelFlag;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.PrepareResult;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalogProvider;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.druid.sql.calcite.BaseCalciteQueryTest.assertResultsEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SqlStatementTest
{
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static SpecificSegmentsQuerySegmentWalker walker;
  private static Closer resourceCloser;
  @TempDir
  public static File temporaryFolder;
  private ListeningExecutorService executorService;
  private final DefaultQueryConfig defaultQueryConfig = new DefaultQueryConfig(
      ImmutableMap.of("DEFAULT_KEY", "DEFAULT_VALUE"));

  private PolicyEnforcer policyEnforcer;
  private SqlStatementFactory sqlStatementFactory;
  private SqlToolbox sqlToolbox;
  private StubServiceEmitter emitter;

  @BeforeAll
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);

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
            new LazySequence<>(() -> resultSequence)
        );
      }
    };

    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder, scheduler);
    resourceCloser.register(walker);
  }

  @AfterAll
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @BeforeEach
  public void setUp()
  {
    executorService = MoreExecutors.listeningDecorator(Execs.multiThreaded(8, "test_sql_resource_%s"));

    policyEnforcer = NoopPolicyEnforcer.instance();
    this.sqlStatementFactory = buildSqlStatementFactory();
  }

  @AfterEach
  public void tearDown() throws Exception
  {
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
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
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
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
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
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
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
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
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
  public void testDirectPolicyEnforcerThrowsForNoPolicy()
  {
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    sqlStatementFactory = buildSqlStatementFactory();
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    ResultSet resultSet = stmt.plan();
    DruidException e = Assertions.assertThrows(DruidException.class, () -> resultSet.run());

    Assertions.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assertions.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assertions.assertEquals("Failed security validation with dataSource [foo]", e.getMessage());
  }

  @Test
  public void testDirectPolicyEnforcerValidatesWithPolicy()
  {
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    sqlStatementFactory = buildSqlStatementFactory();
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt FROM druid.restrictedDatasource_m1_is_6",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    ResultSet resultSet = stmt.plan();
    List<Object[]> results = resultSet.run().getResults().toList();

    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{1L});
    assertResultsEquals("SELECT COUNT(*) AS cnt FROM druid.restrictedDatasource_m1_is_6", expectedResults, results);
  }

  @Test
  public void testDirectValidationError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.bogus",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    try {
      stmt.execute();
      fail();
    }
    catch (DruidException e) {
      BaseCalciteQueryTest.assertDruidException(
          e,
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageContains("Object 'bogus' not found within 'druid'")
      );
    }
  }

  @Test
  public void testDirectPermissionError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "select count(*) from forbiddenDatasource",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
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

  /**
   * Creates a {@link SqlQueryPlus} with auth result {@link CalciteTests#REGULAR_USER_AUTH_RESULT}, which matches
   * the auth result used by {@link #request(boolean)}.
   */
  private SqlQueryPlus makeQuery(String sql)
  {
    return SqlQueryPlus.builder(sql).auth(CalciteTests.REGULAR_USER_AUTH_RESULT).build();
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
    catch (DruidException e) {
      BaseCalciteQueryTest.assertDruidException(
          e,
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageContains("Object 'bogus' not found within 'druid'")
      );
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

  @Test
  public void testHttpPolicyEnforcerThrowsForNoPolicy() throws Exception
  {
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    sqlStatementFactory = buildSqlStatementFactory();
    HttpStatement stmt = sqlStatementFactory.httpStatement(
        makeQuery("SELECT COUNT(*) AS cnt FROM druid.foo"),
        request(true)
    );
    ResultSet resultSet = stmt.plan();
    DruidException e = Assertions.assertThrows(DruidException.class, () -> resultSet.run());

    Assertions.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assertions.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assertions.assertEquals("Failed security validation with dataSource [foo]", e.getMessage());
  }

  @Test
  public void testHttpPolicyEnforcerValidatesWithPolicy()
  {
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    sqlStatementFactory = buildSqlStatementFactory();
    HttpStatement stmt = sqlStatementFactory.httpStatement(
        makeQuery("SELECT COUNT(*) AS cnt FROM druid.restrictedDatasource_m1_is_6"),
        request(true)
    );
    List<Object[]> results = stmt.plan().run().getResults().toList();

    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{1L});
    assertResultsEquals("SELECT COUNT(*) AS cnt FROM druid.restrictedDatasource_m1_is_6", expectedResults, results);
  }

  //-----------------------------------------------------------------
  // Prepared statements: using a prepare/execute model.

  @Test
  public void testPreparedHappyPath()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
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
          .execute(Collections.emptyList(), null)
          .execute()
          .getResults()
          .toList();
      assertEquals(1, results.size());
      assertEquals(6L, results.get(0)[0]);
      assertEquals("foo", results.get(0)[1]);
    }
  }

  @Test
  public void testPrepareValidationError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.bogus",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);
    try {
      stmt.prepare();
      fail();
    }
    catch (DruidException e) {
      BaseCalciteQueryTest.assertDruidException(
          e,
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageContains("Object 'bogus' not found within 'druid'")
      );
    }
  }

  @Test
  public void testPreparePermissionError()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "select count(*) from forbiddenDatasource",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);
    try {
      stmt.prepare();
      fail();
    }
    catch (ForbiddenException e) {
      // Expected
    }
  }

  @Test
  public void testPreparePolicyEnforcerThrowsForNoPolicy()
  {
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    sqlStatementFactory = buildSqlStatementFactory();
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);
    DruidException e = Assertions.assertThrows(DruidException.class, () -> stmt.execute(Collections.emptyList(), null).execute());

    Assertions.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assertions.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assertions.assertEquals("Failed security validation with dataSource [foo]", e.getMessage());
  }

  @Test
  public void testPreparePolicyEnforcerValidatesWithPolicy()
  {
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    sqlStatementFactory = buildSqlStatementFactory();
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT COUNT(*) AS cnt FROM druid.restrictedDatasource_m1_is_6",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlReq);
    List<Object[]> results = stmt.execute(Collections.emptyList(), null).execute().getResults().toList();

    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{1L});
    assertResultsEquals("SELECT COUNT(*) AS cnt FROM druid.restrictedDatasource_m1_is_6", expectedResults, results);
  }

  //-----------------------------------------------------------------
  // Generic tests.

  @Test
  public void testIgnoredQueryContextParametersAreIgnored()
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("select 1 + ?")
        .queryContext(QueryContext.of(QueryContextParameters.BY_SEGMENT, true))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();
    DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    final QueryContext context = stmt.context();
    // should contain only query id, not bySegment since it is not valid for SQL
    Assertions.assertEquals(
        Collections.singleton(QueryContextParameters.SQL_QUERY_ID.getName()),
        context.asMap().keySet()
    );
  }

  @Test
  public void testUserProvidedSqlQueryIdIsKept()
  {
    final SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("select 1")
        .queryContext(QueryContext.of(QueryContextParameters.SQL_QUERY_ID, "user-sql-query-id"))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();
    final DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    Assertions.assertEquals("user-sql-query-id", stmt.sqlQueryId());
    Assertions.assertEquals("user-sql-query-id", stmt.context().get(QueryContextParameters.SQL_QUERY_ID));
  }

  @Test
  public void testNullSqlQueryIdIsReplaced()
  {
    final Map<String, Object> userContext = new HashMap<>();
    userContext.put(QueryContextParameters.SQL_QUERY_ID.getName(), null);
    final SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("select 1")
        .queryContext(QueryContext.of(userContext))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();
    final DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    Assertions.assertNotNull(stmt.sqlQueryId());
    Assertions.assertEquals(stmt.sqlQueryId(), stmt.context().get(QueryContextParameters.SQL_QUERY_ID));
  }

  private SqlStatementFactory buildSqlStatementFactory()
  {
    // Set authorizeTableVisibility(false) so reads from unauthorized tables are "Forbidden" rather than "Not Found".
    final PlannerConfig plannerConfig = PlannerConfig.builder().authorizeTableVisibility(false).build();
    final DruidSchemaCatalogProvider rootSchemaProvider = CalciteTests.createMockRootSchemaProvider(
        conglomerate,
        walker,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();

    TestRequestLogger testRequestLogger = new TestRequestLogger();
    final JoinableFactoryWrapper joinableFactoryWrapper = CalciteTests.createJoinableFactoryWrapper();

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchemaProvider,
        operatorTable,
        macroTable,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        joinableFactoryWrapper,
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        policyEnforcer,
        new DruidHookDispatcher()
    );

    this.emitter = new StubServiceEmitter();
    this.sqlToolbox = new SqlToolbox(
        CalciteTests.createMockSqlEngine(walker, conglomerate),
        plannerFactory,
        emitter,
        testRequestLogger,
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new SqlLifecycleManager()
    );
    return new SqlStatementFactory(sqlToolbox);
  }

  /**
   * Planning that exceeds {@code maxPlanningTimeMs} fails with a {@link QueryTimeoutException}. Planning is simulated
   * as a CPU-bound loop that honours the cancel flag (as Calcite does), so the watchdog trips it and we time out.
   */
  @Test
  @Timeout(30)
  public void testPlanningTimeout()
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo")
        .queryContext(QueryContext.of(ImmutableMap.of(QueryContextParameters.MAX_PLANNING_TIME_MS.getName(), 100)))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();

    // A DirectStatement whose planning step blocks until the query is cancelled, mimicking a pathological query.
    DirectStatement stmt = new DirectStatement(sqlToolbox, sqlReq, null)
    {
      @Override
      protected PlannerResult createPlan(DruidPlanner planner)
      {
        final CancelFlag cancelFlag = planner.getPlannerContext().getCancelFlag();
        while (!cancelFlag.isCancelRequested() && !Thread.currentThread().isInterrupted()) {
          // spin until the watchdog aborts us
        }
        // Calcite throws when it observes a tripped cancel flag; emulate that.
        throw new RuntimeException("Preparation aborted");
      }
    };

    try {
      QueryTimeoutException e = Assertions.assertThrows(QueryTimeoutException.class, stmt::plan);
      Assertions.assertTrue(
          e.getMessage().contains("exceeded the configured maximum planning time"),
          "Unexpected message: " + e.getMessage()
      );
    }
    finally {
      stmt.close();
    }
  }

  /**
   * The budget covers planner construction: if {@link DirectStatement#createPlanner()} alone exhausts
   * {@code maxPlanningTimeMs}, planning fails with a {@link QueryTimeoutException} before any further work.
   */
  @Test
  @Timeout(30)
  public void testPlanningTimeoutDuringPlannerConstruction()
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo")
        .queryContext(QueryContext.of(ImmutableMap.of(QueryContextParameters.MAX_PLANNING_TIME_MS.getName(), 50)))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();

    // Simulate an expensive planner/schema construction that by itself exceeds the budget.
    DirectStatement stmt = new DirectStatement(sqlToolbox, sqlReq, null)
    {
      @Override
      protected DruidPlanner createPlanner()
      {
        try {
          Thread.sleep(300);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return super.createPlanner();
      }
    };

    try {
      QueryTimeoutException e = Assertions.assertThrows(QueryTimeoutException.class, stmt::plan);
      Assertions.assertTrue(
          e.getMessage().contains("exceeded the configured maximum planning time"),
          "Unexpected message: " + e.getMessage()
      );
    }
    finally {
      stmt.close();
    }
  }

  private static final String PLANNING_TIMEOUT_METRIC = "sqlQuery/planningTimeout/count";

  /**
   * A statement whose planning blocks until the planning watchdog aborts it, mimicking a pathological query.
   */
  private DirectStatement newSpinningStatement(final int maxPlanningTimeMs)
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo")
        .queryContext(
            QueryContext.of(ImmutableMap.of(QueryContextParameters.MAX_PLANNING_TIME_MS.getName(), maxPlanningTimeMs))
        )
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();

    return new DirectStatement(sqlToolbox, sqlReq, null)
    {
      @Override
      protected PlannerResult createPlan(DruidPlanner planner)
      {
        final CancelFlag cancelFlag = planner.getPlannerContext().getCancelFlag();
        while (!cancelFlag.isCancelRequested() && !Thread.currentThread().isInterrupted()) {
          // spin until the watchdog aborts us
        }
        throw new RuntimeException("Preparation aborted");
      }
    };
  }

  /**
   * A query aborted because planning timed out emits {@code sqlQuery/planningTimeout/count}, alongside
   * {@code sqlQuery/time}, with the same dimensions.
   */
  @Test
  @Timeout(30)
  public void testPlanningTimeoutEmitsMetric()
  {
    final DirectStatement stmt = newSpinningStatement(100);

    Assertions.assertThrows(QueryTimeoutException.class, stmt::plan);
    stmt.close();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(PLANNING_TIMEOUT_METRIC);
    Assertions.assertEquals(1, events.size());
    final ServiceMetricEvent event = events.get(0);
    Assertions.assertEquals(1, event.getValue().intValue());
    Assertions.assertEquals("false", event.getUserDims().get("success"));
    Assertions.assertEquals("504", String.valueOf(event.getUserDims().get("statusCode")));
    Assertions.assertNotNull(event.getUserDims().get("engine"));

    // The id, nativeQueryIds and dataSource dimensions are not asserted: they only exist once the planner context
    // and authorization are available, and whether the timeout fires before or after that depends on timing.
    final List<ServiceMetricEvent> timeEvents = emitter.getMetricEvents("sqlQuery/time");
    Assertions.assertEquals(1, timeEvents.size());
    Assertions.assertEquals("false", timeEvents.get(0).getUserDims().get("success"));
    Assertions.assertEquals("504", String.valueOf(timeEvents.get(0).getUserDims().get("statusCode")));
  }

  /**
   * The timeout is also reported when planner construction alone exhausts the budget, which fails before the
   * watchdog is armed.
   */
  @Test
  @Timeout(30)
  public void testPlanningTimeoutDuringPlannerConstructionEmitsMetric()
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo")
        .queryContext(QueryContext.of(ImmutableMap.of(QueryContextParameters.MAX_PLANNING_TIME_MS.getName(), 50)))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();

    final DirectStatement stmt = new DirectStatement(sqlToolbox, sqlReq, null)
    {
      @Override
      protected DruidPlanner createPlanner()
      {
        try {
          Thread.sleep(300);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return super.createPlanner();
      }
    };

    Assertions.assertThrows(QueryTimeoutException.class, stmt::plan);
    stmt.close();

    Assertions.assertEquals(1, emitter.getMetricEvents(PLANNING_TIMEOUT_METRIC).size());
  }

  /**
   * On the HTTP path the result pusher reports the failure again, converted to a different exception. The metric must
   * not depend on the type of the exception that happens to be reported last.
   */
  @Test
  @Timeout(30)
  public void testPlanningTimeoutMetricIsEmittedWhenAnotherFailureIsReportedLater()
  {
    final DirectStatement stmt = newSpinningStatement(100);

    Assertions.assertThrows(QueryTimeoutException.class, stmt::plan);
    stmt.reporter().failed(new RuntimeException("converted"));
    stmt.close();

    Assertions.assertEquals(1, emitter.getMetricEvents(PLANNING_TIMEOUT_METRIC).size());
  }

  @Test
  public void testNoPlanningTimeoutMetricWhenPlanningSucceeds()
  {
    SqlQueryPlus sqlReq = SqlQueryPlus
        .builder("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo")
        .queryContext(QueryContext.of(ImmutableMap.of(QueryContextParameters.MAX_PLANNING_TIME_MS.getName(), 60_000)))
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();

    final DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    stmt.plan();
    stmt.close();

    Assertions.assertEquals(0, emitter.getMetricEvents(PLANNING_TIMEOUT_METRIC).size());
    final List<ServiceMetricEvent> timeEvents = emitter.getMetricEvents("sqlQuery/time");
    Assertions.assertEquals(1, timeEvents.size());
    Assertions.assertEquals("true", timeEvents.get(0).getUserDims().get("success"));
  }

  /**
   * Only a planning timeout counts: a query that fails to plan for any other reason does not.
   */
  @Test
  public void testNoPlanningTimeoutMetricWhenPlanningFailsForAnotherReason()
  {
    SqlQueryPlus sqlReq = queryPlus(
        "SELECT * FROM druid.does_not_exist",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    final DirectStatement stmt = sqlStatementFactory.directStatement(sqlReq);
    Assertions.assertThrows(DruidException.class, stmt::plan);
    stmt.close();

    Assertions.assertEquals(0, emitter.getMetricEvents(PLANNING_TIMEOUT_METRIC).size());
    final List<ServiceMetricEvent> timeEvents = emitter.getMetricEvents("sqlQuery/time");
    Assertions.assertEquals(1, timeEvents.size());
    Assertions.assertEquals("false", timeEvents.get(0).getUserDims().get("success"));
  }
}
