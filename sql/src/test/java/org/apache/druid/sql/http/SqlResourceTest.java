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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.exception.AllowedRegexErrorResponseTransformStrategy;
import org.apache.druid.common.exception.ErrorResponseTransformStrategy;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlPlanningException.PlanningError;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlResourceTest extends CalciteTestBase
{
  public static final DruidNode DUMMY_DRUID_NODE = new DruidNode("dummy", "dummy", false, 1, null, true, false);
  public static final ResponseContextConfig TEST_RESPONSE_CONTEXT_CONFIG = ResponseContextConfig.newConfig(false);

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final String DUMMY_SQL_QUERY_ID = "dummy";
  // Timeout to allow (rapid) debugging, while not blocking tests with errors.
  private static final int WAIT_TIMEOUT_SECS = 60;
  private static final Consumer<DirectStatement> NULL_ACTION = s -> {
  };

  private static final List<String> EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS =
      Arrays.asList("__time", "dim1", "dim2", "dim3", "cnt", "m1", "m2", "unique_dim1", "EXPR$8");

  private static final List<String> EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS =
      Arrays.asList("LONG", "STRING", "STRING", "STRING", "LONG", "FLOAT", "DOUBLE", "COMPLEX<hyperUnique>", "STRING");

  private static final List<String> EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS =
      Arrays.asList("TIMESTAMP", "VARCHAR", "VARCHAR", "VARCHAR", "BIGINT", "FLOAT", "DOUBLE", "OTHER", "VARCHAR");

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();
  private SpecificSegmentsQuerySegmentWalker walker;
  private TestRequestLogger testRequestLogger;
  private SqlResource resource;
  private MockHttpServletRequest req;
  private ListeningExecutorService executorService;
  private SqlLifecycleManager lifecycleManager;
  private NativeSqlEngine engine;
  private SqlStatementFactory sqlStatementFactory;
  private StubServiceEmitter stubServiceEmitter;

  private CountDownLatch lifecycleAddLatch;
  private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> validateAndAuthorizeLatchSupplier = new SettableSupplier<>();
  private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> planLatchSupplier = new SettableSupplier<>();
  private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> executeLatchSupplier = new SettableSupplier<>();
  private final SettableSupplier<Function<Sequence<Object[]>, Sequence<Object[]>>> sequenceMapFnSupplier = new SettableSupplier<>();
  private final SettableSupplier<ResponseContext> responseContextSupplier = new SettableSupplier<>();
  private Consumer<DirectStatement> onExecute = NULL_ACTION;

  private boolean sleep;

  @Before
  public void setUp() throws Exception
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
            new LazySequence<T>(() -> {
              if (sleep) {
                try {
                  // pretend to be a query that is waiting on results
                  Thread.sleep(500);
                }
                catch (InterruptedException ignored) {
                }
              }
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

    req = request();

    testRequestLogger = new TestRequestLogger();

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        operatorTable,
        macroTable,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper()
    );

    lifecycleManager = new SqlLifecycleManager()
    {
      @Override
      public void add(String sqlQueryId, Cancelable lifecycle)
      {
        super.add(sqlQueryId, lifecycle);
        if (lifecycleAddLatch != null) {
          lifecycleAddLatch.countDown();
        }
      }
    };
    stubServiceEmitter = new StubServiceEmitter("test", "test");
    final AuthConfig authConfig = new AuthConfig();
    final DefaultQueryConfig defaultQueryConfig = new DefaultQueryConfig(ImmutableMap.of());
    engine = CalciteTests.createMockSqlEngine(walker, conglomerate);
    final SqlToolbox sqlToolbox = new SqlToolbox(
        engine,
        plannerFactory,
        stubServiceEmitter,
        testRequestLogger,
        scheduler,
        authConfig,
        defaultQueryConfig,
        lifecycleManager
    );
    sqlStatementFactory = new SqlStatementFactory(null)
    {
      @Override
      public HttpStatement httpStatement(
          final SqlQuery sqlQuery,
          final HttpServletRequest req
      )
      {
        TestHttpStatement stmt = new TestHttpStatement(
            sqlToolbox,
            sqlQuery,
            req,
            validateAndAuthorizeLatchSupplier,
            planLatchSupplier,
            executeLatchSupplier,
            sequenceMapFnSupplier,
            responseContextSupplier,
            onExecute
        );
        onExecute = NULL_ACTION;
        return stmt;
      }

      @Override
      public DirectStatement directStatement(SqlQueryPlus sqlRequest)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public PreparedStatement preparedStatement(SqlQueryPlus sqlRequest)
      {
        throw new UnsupportedOperationException();
      }
    };
    resource = new SqlResource(
        JSON_MAPPER,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        sqlStatementFactory,
        lifecycleManager,
        new ServerConfig(),
        TEST_RESPONSE_CONTEXT_CONFIG,
        DUMMY_DRUID_NODE
    );
  }

  MockHttpServletRequest request()
  {
    return makeExpectedReq(CalciteTests.REGULAR_USER_AUTH_RESULT);
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
    executorService.shutdownNow();
    executorService.awaitTermination(2, TimeUnit.SECONDS);
    resourceCloser.close();
  }

  @Test
  public void testUnauthorized()
  {
    try {
      postForResponse(
          createSimpleQueryWithId("id", "select count(*) from forbiddenDatasource"),
          request()
      );
      Assert.fail("doPost did not throw ForbiddenException for an unauthorized query");
    }
    catch (ForbiddenException e) {
      // expected
    }
    Assert.assertEquals(1, testRequestLogger.getSqlQueryLogs().size());
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testCountStar() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        createSimpleQueryWithId("id", "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo")
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6, "TheFoo", "foo")
        ),
        rows
    );
    checkSqlRequestLog(true);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testCountStarWithMissingIntervalsContext() throws Exception
  {
    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo",
        null,
        false,
        false,
        false,
        // We set uncoveredIntervalsLimit more for the funzies than anything.  The underlying setup of the test doesn't
        // actually look at it or operate with it.  Instead, we set the supplier of the ResponseContext to mock what
        // we would expect from the normal query pipeline
        ImmutableMap.of(BaseQuery.SQL_QUERY_ID, "id", "uncoveredIntervalsLimit", 1),
        null
    );

    final ResponseContext mockRespContext = ResponseContext.createEmpty();
    mockRespContext.put(ResponseContext.Keys.instance().keyOf("uncoveredIntervals"), "2030-01-01/78149827981274-01-01");
    mockRespContext.put(ResponseContext.Keys.instance().keyOf("uncoveredIntervalsOverflowed"), "true");
    responseContextSupplier.set(mockRespContext);

    final MockHttpServletResponse response = postForResponse(sqlQuery, makeRegularUserReq());

    Map responseContext = JSON_MAPPER.readValue(
        Iterables.getOnlyElement(response.headers.get("X-Druid-Response-Context")),
        Map.class
    );
    Assert.assertEquals(
        ImmutableMap.of(
            "uncoveredIntervals", "2030-01-01/78149827981274-01-01",
            "uncoveredIntervalsOverflowed", "true"
        ),
        responseContext
    );

    Object results = JSON_MAPPER.readValue(response.baos.toByteArray(), Object.class);

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6, "TheFoo", "foo")
        ),
        results
    );
    checkSqlRequestLog(true);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testSqlLifecycleMetrics() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        createSimpleQueryWithId("id", "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo")
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6, "TheFoo", "foo")
        ),
        rows
    );
    checkSqlRequestLog(true);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
    stubServiceEmitter.verifyEmitted("sqlQuery/time", 1);
    stubServiceEmitter.verifyValue("sqlQuery/bytes", 27L);
    stubServiceEmitter.verifyEmitted("sqlQuery/planningTimeMs", 1);
  }


  @Test
  public void testCountStarExtendedCharacters() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        createSimpleQueryWithId(
            "id",
            "SELECT COUNT(*) AS cnt FROM druid.lotsocolumns WHERE dimMultivalEnumerated = 'ㅑ ㅓ ㅕ ㅗ ㅛ ㅜ ㅠ ㅡ ㅣ'"
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 1)
        ),
        rows
    );
    checkSqlRequestLog(true);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testTimestampsInResponse() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            null,
            null
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "2000-01-01T00:00:00.000Z", "t2", "2000-01-01T00:00:00.000Z")
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponseWithParameterizedLimit() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT ?",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            null,
            ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "2000-01-01T00:00:00.000Z", "t2", "2000-01-01T00:00:00.000Z")
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponseLosAngelesTimeZone() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            ImmutableMap.of(PlannerContext.CTX_SQL_TIME_ZONE, "America/Los_Angeles"),
            null
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "1999-12-31T16:00:00.000-08:00", "t2", "1999-12-31T00:00:00.000-08:00")
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponseWithNulls() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT MAX(__time) as t1, MAX(__time) FILTER(WHERE dim1 = 'non_existing') as t2 FROM druid.foo",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            null,
            null
        )
    ).rhs;

    Assert.assertEquals(
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            ImmutableMap.of("t1", "2001-01-03T00:00:00.000Z", "t2", "-292275055-05-16T16:47:04.192Z")
            // t2 represents Long.MIN converted to a timestamp
        ) :
        ImmutableList.of(
            Maps.transformValues(
                ImmutableMap.of("t1", "2001-01-03T00:00:00.000Z", "t2", ""),
                (val) -> "".equals(val) ? null : val
            )
        ),
        rows
    );
  }

  @Test
  public void testFieldAliasingSelect() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo LIMIT 1",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            null,
            null
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("x", "a", "y", "a")
        ),
        rows
    );
  }

  @Test
  public void testFieldAliasingGroupBy() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo GROUP BY dim2",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            null,
            null
        )
    ).rhs;

    Assert.assertEquals(
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            ImmutableMap.of("x", "", "y", ""),
            ImmutableMap.of("x", "a", "y", "a"),
            ImmutableMap.of("x", "abc", "y", "abc")
        ) :
        ImmutableList.of(
            // x and y both should be null instead of empty string
            Maps.transformValues(ImmutableMap.of("x", "", "y", ""), (val) -> null),
            ImmutableMap.of("x", "", "y", ""),
            ImmutableMap.of("x", "a", "y", "a"),
            ImmutableMap.of("x", "abc", "y", "abc")
        ),
        rows
    );
  }

  @Test
  public void testArrayResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;

    Assert.assertEquals(
        ImmutableList.of(
            Arrays.asList(
                "2000-01-01T00:00:00.000Z",
                "",
                "a",
                "[\"a\",\"b\"]",
                1,
                1.0,
                1.0,
                "org.apache.druid.hll.VersionOneHyperLogLogCollector",
                nullStr
            ),
            Arrays.asList(
                "2000-01-02T00:00:00.000Z",
                "10.1",
                nullStr,
                "[\"b\",\"c\"]",
                1,
                2.0,
                2.0,
                "org.apache.druid.hll.VersionOneHyperLogLogCollector",
                nullStr
            )
        ),
        doPost(
            new SqlQuery(query, ResultFormat.ARRAY, false, false, false, null, null),
            new TypeReference<List<List<Object>>>()
            {
            }
        ).rhs
    );
  }

  @Test
  public void testArrayResultFormatWithErrorAfterSecondRow() throws Exception
  {
    sequenceMapFnSupplier.set(errorAfterSecondRowMapFn());

    final String query = "SELECT cnt FROM foo";
    final Pair<QueryException, String> response =
        doPostRaw(new SqlQuery(query, ResultFormat.ARRAY, false, false, false, null, null), req);

    // Truncated response: missing final ]
    Assert.assertNull(response.lhs);
    Assert.assertEquals("[[1],[1]", response.rhs);
  }

  @Test
  public void testObjectResultFormatWithErrorAfterFirstRow() throws Exception
  {
    sequenceMapFnSupplier.set(errorAfterSecondRowMapFn());

    final String query = "SELECT cnt FROM foo";
    final Pair<QueryException, String> response =
        doPostRaw(new SqlQuery(query, ResultFormat.OBJECT, false, false, false, null, null), req);

    // Truncated response: missing final ]
    Assert.assertNull(response.lhs);
    Assert.assertEquals("[{\"cnt\":1},{\"cnt\":1}", response.rhs);
  }

  @Test
  public void testArrayLinesResultFormatWithErrorAfterFirstRow() throws Exception
  {
    sequenceMapFnSupplier.set(errorAfterSecondRowMapFn());

    final String query = "SELECT cnt FROM foo";
    final Pair<QueryException, String> response =
        doPostRaw(new SqlQuery(query, ResultFormat.ARRAYLINES, false, false, false, null, null), req);

    // Truncated response: missing final LFLF
    Assert.assertNull(response.lhs);
    Assert.assertEquals("[1]\n[1]", response.rhs);
  }

  @Test
  public void testObjectLinesResultFormatWithErrorAfterFirstRow() throws Exception
  {
    sequenceMapFnSupplier.set(errorAfterSecondRowMapFn());

    final String query = "SELECT cnt FROM foo";
    final Pair<QueryException, String> response =
        doPostRaw(new SqlQuery(query, ResultFormat.OBJECTLINES, false, false, false, null, null), req);

    // Truncated response: missing final LFLF
    Assert.assertNull(response.lhs);
    Assert.assertEquals("{\"cnt\":1}\n{\"cnt\":1}", response.rhs);
  }

  @Test
  public void testCsvResultFormatWithErrorAfterFirstRow() throws Exception
  {
    sequenceMapFnSupplier.set(errorAfterSecondRowMapFn());

    final String query = "SELECT cnt FROM foo";
    final Pair<QueryException, String> response =
        doPostRaw(new SqlQuery(query, ResultFormat.CSV, false, false, false, null, null), req);

    // Truncated response: missing final LFLF
    Assert.assertNull(response.lhs);
    Assert.assertEquals("1\n1\n", response.rhs);
  }

  @Test
  public void testArrayResultFormatWithHeader() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;

    final String hllStr = "org.apache.druid.hll.VersionOneHyperLogLogCollector";
    List[] expectedQueryResults = new List[]{
        Arrays.asList("2000-01-01T00:00:00.000Z", "", "a", "[\"a\",\"b\"]", 1, 1.0, 1.0, hllStr, nullStr),
        Arrays.asList("2000-01-02T00:00:00.000Z", "10.1", nullStr, "[\"b\",\"c\"]", 1, 2.0, 2.0, hllStr, nullStr)
    };

    MockHttpServletResponse response = postForResponse(
        new SqlQuery(query, ResultFormat.ARRAY, true, true, true, null, null),
        req.mimic()
    );

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("yes", response.getHeader("X-Druid-SQL-Header-Included"));
    Assert.assertEquals(
        new ArrayList<Object>()
        {
          {
            add(EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS);
            add(EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS);
            add(EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS);
            addAll(Arrays.asList(expectedQueryResults));
          }
        },
        JSON_MAPPER.readValue(response.baos.toByteArray(), Object.class)
    );

    MockHttpServletResponse responseNoSqlTypesHeader = postForResponse(
        new SqlQuery(query, ResultFormat.ARRAY, true, true, false, null, null),
        req.mimic()
    );

    Assert.assertEquals(200, responseNoSqlTypesHeader.getStatus());
    Assert.assertEquals("yes", responseNoSqlTypesHeader.getHeader("X-Druid-SQL-Header-Included"));
    Assert.assertEquals(
        new ArrayList<Object>()
        {
          {
            add(EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS);
            add(EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS);
            addAll(Arrays.asList(expectedQueryResults));
          }
        },
        JSON_MAPPER.readValue(responseNoSqlTypesHeader.baos.toByteArray(), Object.class)
    );

    MockHttpServletResponse responseNoTypesHeader = postForResponse(
        new SqlQuery(query, ResultFormat.ARRAY, true, false, true, null, null),
        req.mimic()
    );

    Assert.assertEquals(200, responseNoTypesHeader.getStatus());
    Assert.assertEquals("yes", responseNoTypesHeader.getHeader("X-Druid-SQL-Header-Included"));
    Assert.assertEquals(
        new ArrayList<Object>()
        {
          {
            add(EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS);
            add(EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS);
            addAll(Arrays.asList(expectedQueryResults));
          }
        },
        JSON_MAPPER.readValue(responseNoTypesHeader.baos.toByteArray(), Object.class)
    );

    MockHttpServletResponse responseNoTypes = postForResponse(
        new SqlQuery(query, ResultFormat.ARRAY, true, false, false, null, null),
        req.mimic()
    );

    Assert.assertEquals(200, responseNoTypes.getStatus());
    Assert.assertEquals("yes", responseNoTypes.getHeader("X-Druid-SQL-Header-Included"));
    Assert.assertEquals(
        new ArrayList<Object>()
        {
          {
            add(EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS);
            addAll(Arrays.asList(expectedQueryResults));
          }
        },
        JSON_MAPPER.readValue(responseNoTypes.baos.toByteArray(), Object.class)
    );

    MockHttpServletResponse responseNoHeader = postForResponse(
        new SqlQuery(query, ResultFormat.ARRAY, false, false, false, null, null),
        req.mimic()
    );

    Assert.assertEquals(200, responseNoHeader.getStatus());
    Assert.assertNull(responseNoHeader.getHeader("X-Druid-SQL-Header-Included"));
    Assert.assertEquals(
        Arrays.asList(expectedQueryResults),
        JSON_MAPPER.readValue(responseNoHeader.baos.toByteArray(), Object.class)
    );

  }

  @Test
  public void testArrayResultFormatWithHeader_nullColumnType() throws Exception
  {
    // Test a query that returns null header for some of the columns
    final String query = "SELECT (1, 2) FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1";

    MockHttpServletResponse response = postForResponse(
        new SqlQuery(query, ResultFormat.ARRAY, true, true, true, null, null),
        req
    );

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("yes", response.getHeader("X-Druid-SQL-Header-Included"));

    Assert.assertEquals(
        ImmutableList.of(
            Collections.singletonList("EXPR$0"),
            Collections.singletonList(null),
            Collections.singletonList("ROW"),
            Collections.singletonList(
                Arrays.asList(
                    1,
                    2
                )
            )
        ),
        JSON_MAPPER.readValue(response.baos.toByteArray(), Object.class)
    );
  }

  @Test
  public void testArrayLinesResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final Pair<QueryException, String> pair = doPostRaw(
        new SqlQuery(query, ResultFormat.ARRAYLINES, false, false, false, null, null)
    );
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(4, lines.size());
    Assert.assertEquals(
        Arrays.asList(
            "2000-01-01T00:00:00.000Z",
            "",
            "a",
            "[\"a\",\"b\"]",
            1,
            1.0,
            1.0,
            "org.apache.druid.hll.VersionOneHyperLogLogCollector",
            nullStr
        ),
        JSON_MAPPER.readValue(lines.get(0), List.class)
    );
    Assert.assertEquals(
        Arrays.asList(
            "2000-01-02T00:00:00.000Z",
            "10.1",
            nullStr,
            "[\"b\",\"c\"]",
            1,
            2.0,
            2.0,
            "org.apache.druid.hll.VersionOneHyperLogLogCollector",
            nullStr
        ),
        JSON_MAPPER.readValue(lines.get(1), List.class)
    );
    Assert.assertEquals("", lines.get(2));
    Assert.assertEquals("", lines.get(3));
  }

  @Test
  public void testArrayLinesResultFormatWithHeader() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final Pair<QueryException, String> pair = doPostRaw(
        new SqlQuery(query, ResultFormat.ARRAYLINES, true, true, true, null, null)
    );
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(7, lines.size());
    Assert.assertEquals(EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS, JSON_MAPPER.readValue(lines.get(0), List.class));
    Assert.assertEquals(EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS, JSON_MAPPER.readValue(lines.get(1), List.class));
    Assert.assertEquals(EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS, JSON_MAPPER.readValue(lines.get(2), List.class));
    Assert.assertEquals(
        Arrays.asList(
            "2000-01-01T00:00:00.000Z",
            "",
            "a",
            "[\"a\",\"b\"]",
            1,
            1.0,
            1.0,
            "org.apache.druid.hll.VersionOneHyperLogLogCollector",
            nullStr
        ),
        JSON_MAPPER.readValue(lines.get(3), List.class)
    );
    Assert.assertEquals(
        Arrays.asList(
            "2000-01-02T00:00:00.000Z",
            "10.1",
            nullStr,
            "[\"b\",\"c\"]",
            1,
            2.0,
            2.0,
            "org.apache.druid.hll.VersionOneHyperLogLogCollector",
            nullStr
        ),
        JSON_MAPPER.readValue(lines.get(4), List.class)
    );
    Assert.assertEquals("", lines.get(5));
    Assert.assertEquals("", lines.get(6));
  }

  @Test
  public void testArrayLinesResultFormatWithHeader_nullColumnType() throws Exception
  {
    final String query = "SELECT (1, 2) FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1";
    final Pair<QueryException, String> pair = doPostRaw(
        new SqlQuery(query, ResultFormat.ARRAYLINES, true, true, true, null, null)
    );
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(6, lines.size());
    Assert.assertEquals(Collections.singletonList("EXPR$0"), JSON_MAPPER.readValue(lines.get(0), List.class));
    Assert.assertEquals(Collections.singletonList(null), JSON_MAPPER.readValue(lines.get(1), List.class));
    Assert.assertEquals(Collections.singletonList("ROW"), JSON_MAPPER.readValue(lines.get(2), List.class));
    Assert.assertEquals(
        Collections.singletonList(
            Arrays.asList(
                1,
                2
            )
        ),
        JSON_MAPPER.readValue(lines.get(3), List.class)
    );
    Assert.assertEquals("", lines.get(4));
    Assert.assertEquals("", lines.get(5));
  }

  @Test
  public void testObjectResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo  LIMIT 2";
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final Function<Map<String, Object>, Map<String, Object>> transformer = m -> {
      return Maps.transformEntries(
          m,
          (k, v) -> "EXPR$8".equals(k) || ("dim2".equals(k) && v.toString().isEmpty()) ? nullStr : v
      );
    };

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-01T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "")
                .put("dim2", "a")
                .put("dim3", "[\"a\",\"b\"]")
                .put("m1", 1.0)
                .put("m2", 1.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build(),
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-02T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "10.1")
                .put("dim2", "")
                .put("dim3", "[\"b\",\"c\"]")
                .put("m1", 2.0)
                .put("m2", 2.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build()
        ).stream().map(transformer).collect(Collectors.toList()),
        doPost(
            new SqlQuery(query, ResultFormat.OBJECT, false, false, false, null, null),
            new TypeReference<List<Map<String, Object>>>()
            {
            }
        ).rhs
    );
  }

  @Test
  public void testObjectLinesResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final Pair<QueryException, String> pair = doPostRaw(
        new SqlQuery(query, ResultFormat.OBJECTLINES, false, false, false, null, null)
    );
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final Function<Map<String, Object>, Map<String, Object>> transformer = m -> {
      return Maps.transformEntries(
          m,
          (k, v) -> "EXPR$8".equals(k) || ("dim2".equals(k) && v.toString().isEmpty()) ? nullStr : v
      );
    };
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(4, lines.size());
    Assert.assertEquals(
        transformer.apply(
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-01T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "")
                .put("dim2", "a")
                .put("dim3", "[\"a\",\"b\"]")
                .put("m1", 1.0)
                .put("m2", 1.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build()
        ),
        JSON_MAPPER.readValue(lines.get(0), Object.class)
    );
    Assert.assertEquals(
        transformer.apply(
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-02T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "10.1")
                .put("dim2", "")
                .put("dim3", "[\"b\",\"c\"]")
                .put("m1", 2.0)
                .put("m2", 2.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build()
        ),
        JSON_MAPPER.readValue(lines.get(1), Object.class)
    );
    Assert.assertEquals("", lines.get(2));
    Assert.assertEquals("", lines.get(3));
  }

  @Test
  public void testObjectLinesResultFormatWithMinimalHeader() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final Pair<QueryException, String> pair =
        doPostRaw(new SqlQuery(query, ResultFormat.OBJECTLINES, true, false, false, null, null));
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final Function<Map<String, Object>, Map<String, Object>> transformer = m -> Maps.transformEntries(
        m,
        (k, v) -> "EXPR$8".equals(k) || ("dim2".equals(k) && v.toString().isEmpty()) ? nullStr : v
    );
    final List<String> lines = Splitter.on('\n').splitToList(response);

    final Map<String, Object> expectedHeader = new HashMap<>();
    for (final String column : EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS) {
      expectedHeader.put(column, null);
    }

    Assert.assertEquals(5, lines.size());
    Assert.assertEquals(expectedHeader, JSON_MAPPER.readValue(lines.get(0), Object.class));
    Assert.assertEquals(
        transformer.apply(
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-01T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "")
                .put("dim2", "a")
                .put("dim3", "[\"a\",\"b\"]")
                .put("m1", 1.0)
                .put("m2", 1.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build()
        ),
        JSON_MAPPER.readValue(lines.get(1), Object.class)
    );
    Assert.assertEquals(
        transformer.apply(
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-02T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "10.1")
                .put("dim2", "")
                .put("dim3", "[\"b\",\"c\"]")
                .put("m1", 2.0)
                .put("m2", 2.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build()
        ),
        JSON_MAPPER.readValue(lines.get(2), Object.class)
    );
    Assert.assertEquals("", lines.get(3));
    Assert.assertEquals("", lines.get(4));
  }

  @Test
  public void testObjectLinesResultFormatWithFullHeader() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final Pair<QueryException, String> pair =
        doPostRaw(new SqlQuery(query, ResultFormat.OBJECTLINES, true, true, true, null, null));
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final Function<Map<String, Object>, Map<String, Object>> transformer = m -> Maps.transformEntries(
        m,
        (k, v) -> "EXPR$8".equals(k) || ("dim2".equals(k) && v.toString().isEmpty()) ? nullStr : v
    );
    final List<String> lines = Splitter.on('\n').splitToList(response);

    final Map<String, Object> expectedHeader = new HashMap<>();
    for (int i = 0; i < EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS.size(); i++) {
      expectedHeader.put(
          EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS.get(i),
          ImmutableMap.of(
              ObjectWriter.TYPE_HEADER_NAME, EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS.get(i),
              ObjectWriter.SQL_TYPE_HEADER_NAME, EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS.get(i)
          )
      );
    }

    Assert.assertEquals(5, lines.size());
    Assert.assertEquals(expectedHeader, JSON_MAPPER.readValue(lines.get(0), Object.class));
    Assert.assertEquals(
        transformer.apply(
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-01T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "")
                .put("dim2", "a")
                .put("dim3", "[\"a\",\"b\"]")
                .put("m1", 1.0)
                .put("m2", 1.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build()
        ),
        JSON_MAPPER.readValue(lines.get(1), Object.class)
    );
    Assert.assertEquals(
        transformer.apply(
            ImmutableMap
                .<String, Object>builder()
                .put("__time", "2000-01-02T00:00:00.000Z")
                .put("cnt", 1)
                .put("dim1", "10.1")
                .put("dim2", "")
                .put("dim3", "[\"b\",\"c\"]")
                .put("m1", 2.0)
                .put("m2", 2.0)
                .put("unique_dim1", "org.apache.druid.hll.VersionOneHyperLogLogCollector")
                .put("EXPR$8", "")
                .build()
        ),
        JSON_MAPPER.readValue(lines.get(2), Object.class)
    );
    Assert.assertEquals("", lines.get(3));
    Assert.assertEquals("", lines.get(4));
  }

  @Test
  public void testObjectLinesResultFormatWithFullHeader_nullColumnType() throws Exception
  {
    final String query = "SELECT (1, 2) FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1";
    final Pair<QueryException, String> pair =
        doPostRaw(new SqlQuery(query, ResultFormat.OBJECTLINES, true, true, true, null, null));
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    final Map<String, String> typeMap = new HashMap<>();
    typeMap.put(ObjectWriter.TYPE_HEADER_NAME, null);
    typeMap.put(ObjectWriter.SQL_TYPE_HEADER_NAME, "ROW");
    final Map<String, Object> expectedHeader = ImmutableMap.of("EXPR$0", typeMap);

    Assert.assertEquals(4, lines.size());
    Assert.assertEquals(expectedHeader, JSON_MAPPER.readValue(lines.get(0), Object.class));
    Assert.assertEquals(
        ImmutableMap
            .<String, Object>builder()
            .put("EXPR$0", Arrays.asList(1, 2))
            .build(),
        JSON_MAPPER.readValue(lines.get(1), Object.class)
    );

    Assert.assertEquals("", lines.get(2));
    Assert.assertEquals("", lines.get(3));
  }

  @Test
  public void testCsvResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final Pair<QueryException, String> pair = doPostRaw(
        new SqlQuery(query, ResultFormat.CSV, false, false, false, null, null)
    );
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(
        ImmutableList.of(
            "2000-01-01T00:00:00.000Z,,a,\"[\"\"a\"\",\"\"b\"\"]\",1,1.0,1.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
            "2000-01-02T00:00:00.000Z,10.1,,\"[\"\"b\"\",\"\"c\"\"]\",1,2.0,2.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
            "",
            ""
        ),
        lines
    );
  }

  @Test
  public void testCsvResultFormatWithHeaders() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final Pair<QueryException, String> pair = doPostRaw(
        new SqlQuery(query, ResultFormat.CSV, true, true, true, null, null)
    );
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(
        ImmutableList.of(
            String.join(",", EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS),
            String.join(",", EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS),
            String.join(",", EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS),
            "2000-01-01T00:00:00.000Z,,a,\"[\"\"a\"\",\"\"b\"\"]\",1,1.0,1.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
            "2000-01-02T00:00:00.000Z,10.1,,\"[\"\"b\"\",\"\"c\"\"]\",1,2.0,2.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
            "",
            ""
        ),
        lines
    );
  }

  @Test
  public void testCsvResultFormatWithHeaders_nullColumnType() throws Exception
  {
    final String query = "SELECT (1, 2) FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1";
    final Pair<QueryException, String> pair = doPostRaw(
        new SqlQuery(query, ResultFormat.CSV, true, true, true, null, null)
    );
    Assert.assertNull(pair.lhs);
    final String response = pair.rhs;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(
        ImmutableList.of(
            "EXPR$0",
            "",
            "ROW"
        ),
        lines.subList(0, 3)
    );
  }

  @Test
  public void testExplainCountStar() throws Exception
  {
    Map<String, Object> queryContext = ImmutableMap.of(
        QueryContexts.CTX_SQL_QUERY_ID,
        DUMMY_SQL_QUERY_ID,
        PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN,
        "false"
    );
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "EXPLAIN PLAN FOR SELECT COUNT(*) AS cnt FROM druid.foo",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            queryContext,
            null
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.<String, Object>of(
                "PLAN",
                StringUtils.format(
                    "DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"context\":{\"sqlQueryId\":\"%s\",\"%s\":\"%s\"}}], signature=[{a0:LONG}])\n",
                    DUMMY_SQL_QUERY_ID,
                    PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN,
                    "false"
                ),
                "RESOURCES",
                "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]"
            )
        ),
        rows
    );
  }

  @Test
  public void testCannotParse() throws Exception
  {
    final QueryException exception = doPost(
        createSimpleQueryWithId("id", "FROM druid.foo")
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(PlanningError.SQL_PARSE_ERROR.getErrorCode(), exception.getErrorCode());
    Assert.assertEquals(PlanningError.SQL_PARSE_ERROR.getErrorClass(), exception.getErrorClass());
    Assert.assertTrue(exception.getMessage().contains("Encountered \"FROM\" at line 1, column 1."));
    checkSqlRequestLog(false);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testCannotValidate() throws Exception
  {
    final QueryException exception = doPost(
        createSimpleQueryWithId("id", "SELECT dim4 FROM druid.foo")
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(PlanningError.VALIDATION_ERROR.getErrorCode(), exception.getErrorCode());
    Assert.assertEquals(PlanningError.VALIDATION_ERROR.getErrorClass(), exception.getErrorClass());
    Assert.assertTrue(exception.getMessage().contains("Column 'dim4' not found in any table"));
    checkSqlRequestLog(false);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testCannotConvert() throws Exception
  {
    // SELECT + ORDER unsupported
    final QueryException exception = doPost(
        createSimpleQueryWithId("id", "SELECT dim1 FROM druid.foo ORDER BY dim1")
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals("SQL query is unsupported", exception.getErrorCode());
    Assert.assertEquals(PlanningError.UNSUPPORTED_SQL_ERROR.getErrorClass(), exception.getErrorClass());
    Assert.assertTrue(
        exception.getMessage()
                 .contains("Query not supported. " +
                           "Possible error: SQL query requires order by non-time column [dim1 ASC] that is not supported.")
    );
    checkSqlRequestLog(false);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  /**
   * This test is for {@link UnsupportedSQLQueryException} exceptions that are thrown by druid rules during query
   * planning. e.g. doing max aggregation on string type. The test checks that the API returns correct error messages
   * for such planning errors.
   */
  @Test
  public void testCannotConvert_UnsupportedSQLQueryException() throws Exception
  {
    // max(string) unsupported
    final QueryException exception = doPost(
        createSimpleQueryWithId("id", "SELECT max(dim1) FROM druid.foo")
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(PlanningError.UNSUPPORTED_SQL_ERROR.getErrorCode(), exception.getErrorCode());
    Assert.assertEquals(PlanningError.UNSUPPORTED_SQL_ERROR.getErrorClass(), exception.getErrorClass());
    Assert.assertTrue(
        exception.getMessage()
                 .contains("Query not supported. " +
                           "Possible error: Max aggregation is not supported for 'STRING' type")
    );
    checkSqlRequestLog(false);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testResourceLimitExceeded() throws Exception
  {
    final QueryException exception = doPost(
        new SqlQuery(
            "SELECT DISTINCT dim1 FROM foo",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            ImmutableMap.of(GroupByQueryConfig.CTX_KEY_BUFFER_GROUPER_MAX_SIZE, 1, BaseQuery.SQL_QUERY_ID, "id"),
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(exception.getErrorCode(), QueryException.RESOURCE_LIMIT_EXCEEDED_ERROR_CODE);
    Assert.assertEquals(exception.getErrorClass(), ResourceLimitExceededException.class.getName());
    checkSqlRequestLog(false);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  private void failOnExecute(String errorMessage)
  {
    onExecute = s -> {
      throw new QueryUnsupportedException(errorMessage);
    };
  }

  @Test
  public void testUnsupportedQueryThrowsException() throws Exception
  {
    String errorMessage = "This will be supported in Druid 9999";
    failOnExecute(errorMessage);
    final QueryException exception = doPost(
        new SqlQuery(
            "SELECT ANSWER TO LIFE",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            ImmutableMap.of(BaseQuery.SQL_QUERY_ID, "id"),
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(QueryException.QUERY_UNSUPPORTED_ERROR_CODE, exception.getErrorCode());
    Assert.assertEquals(QueryUnsupportedException.class.getName(), exception.getErrorClass());
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testErrorResponseReturnSameQueryIdWhenSetInContext()
  {
    String queryId = "id123";
    String errorMessage = "This will be supported in Druid 9999";
    failOnExecute(errorMessage);
    final MockHttpServletResponse response = postForResponse(
        new SqlQuery(
            "SELECT ANSWER TO LIFE",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            ImmutableMap.of("sqlQueryId", queryId),
            null
        ),
        req
    );
    Assert.assertNotEquals(200, response.getStatus());
    Assert.assertEquals(
        queryId,
        Iterables.getOnlyElement(response.headers.get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER))
    );
  }

  @Test
  public void testErrorResponseReturnNewQueryIdWhenNotSetInContext()
  {
    String errorMessage = "This will be supported in Druid 9999";
    failOnExecute(errorMessage);
    final MockHttpServletResponse response = postForResponse(
        new SqlQuery(
            "SELECT ANSWER TO LIFE",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            ImmutableMap.of(),
            null
        ),
        req
    );
    Assert.assertNotEquals(200, response.getStatus());
    Assert.assertFalse(
        Strings.isNullOrEmpty(Iterables.getOnlyElement(response.headers.get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER)))
    );
  }

  @Test
  public void testUnsupportedQueryThrowsExceptionWithFilterResponse() throws Exception
  {
    resource = new SqlResource(
        JSON_MAPPER,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        sqlStatementFactory,
        lifecycleManager,
        new ServerConfig()
        {
          @Override
          public boolean isShowDetailedJettyErrors()
          {
            return true;
          }

          @Override
          public ErrorResponseTransformStrategy getErrorResponseTransformStrategy()
          {
            return new AllowedRegexErrorResponseTransformStrategy(ImmutableList.of());
          }
        },
        TEST_RESPONSE_CONTEXT_CONFIG,
        DUMMY_DRUID_NODE
    );

    String errorMessage = "This will be supported in Druid 9999";
    failOnExecute(errorMessage);
    final QueryException exception = doPost(
        new SqlQuery(
            "SELECT ANSWER TO LIFE",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            ImmutableMap.of("sqlQueryId", "id"),
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertNull(exception.getMessage());
    Assert.assertNull(exception.getHost());
    Assert.assertEquals(exception.getErrorCode(), QueryException.QUERY_UNSUPPORTED_ERROR_CODE);
    Assert.assertNull(exception.getErrorClass());
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testAssertionErrorThrowsErrorWithFilterResponse() throws Exception
  {
    resource = new SqlResource(
        JSON_MAPPER,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        sqlStatementFactory,
        lifecycleManager,
        new ServerConfig()
        {
          @Override
          public boolean isShowDetailedJettyErrors()
          {
            return true;
          }

          @Override
          public ErrorResponseTransformStrategy getErrorResponseTransformStrategy()
          {
            return new AllowedRegexErrorResponseTransformStrategy(ImmutableList.of());
          }
        },
        TEST_RESPONSE_CONTEXT_CONFIG,
        DUMMY_DRUID_NODE
    );

    String errorMessage = "could not assert";
    failOnExecute(errorMessage);
    onExecute = s -> {
      throw new AssertionError(errorMessage);
    };
    final QueryException exception = doPost(
        new SqlQuery(
            "SELECT ANSWER TO LIFE",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            ImmutableMap.of("sqlQueryId", "id"),
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertNull(exception.getMessage());
    Assert.assertNull(exception.getHost());
    Assert.assertEquals("Unknown exception", exception.getErrorCode());
    Assert.assertNull(exception.getErrorClass());
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testTooManyRequests() throws Exception
  {
    sleep = true;
    final int numQueries = 3;
    final String sqlQueryId = "tooManyRequestsTest";

    List<Future<Pair<QueryException, List<Map<String, Object>>>>> futures = new ArrayList<>(numQueries);
    for (int i = 0; i < numQueries; i++) {
      futures.add(executorService.submit(() -> {
        try {
          return doPost(
              new SqlQuery(
                  "SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo",
                  null,
                  false,
                  false,
                  false,
                  ImmutableMap.of("priority", -5, BaseQuery.SQL_QUERY_ID, sqlQueryId),
                  null
              ),
              makeRegularUserReq()
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    int success = 0;
    int limited = 0;
    for (int i = 0; i < numQueries; i++) {
      Pair<QueryException, List<Map<String, Object>>> result = futures.get(i).get();
      List<Map<String, Object>> rows = result.rhs;
      if (rows != null) {
        Assert.assertEquals(ImmutableList.of(ImmutableMap.of("cnt", 6, "TheFoo", "foo")), rows);
        success++;
      } else {
        QueryException interruped = result.lhs;
        Assert.assertEquals(QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE, interruped.getErrorCode());
        Assert.assertEquals(
            QueryCapacityExceededException.makeLaneErrorMessage(HiLoQueryLaningStrategy.LOW, 2),
            interruped.getMessage()
        );
        limited++;
      }
    }
    Assert.assertEquals(2, success);
    Assert.assertEquals(1, limited);
    Assert.assertEquals(3, testRequestLogger.getSqlQueryLogs().size());
    Assert.assertTrue(lifecycleManager.getAll(sqlQueryId).isEmpty());
  }

  @Test
  public void testQueryTimeoutException() throws Exception
  {
    final String sqlQueryId = "timeoutTest";
    Map<String, Object> queryContext = ImmutableMap.of(
        QueryContexts.TIMEOUT_KEY,
        1,
        BaseQuery.SQL_QUERY_ID,
        sqlQueryId
    );
    final QueryException timeoutException = doPost(
        new SqlQuery(
            "SELECT CAST(__time AS DATE), dim1, dim2, dim3 FROM druid.foo GROUP by __time, dim1, dim2, dim3 ORDER BY dim2 DESC",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            queryContext,
            null
        )
    ).lhs;
    Assert.assertNotNull(timeoutException);
    Assert.assertEquals(timeoutException.getErrorCode(), QueryException.QUERY_TIMEOUT_ERROR_CODE);
    Assert.assertEquals(timeoutException.getErrorClass(), QueryTimeoutException.class.getName());
    Assert.assertTrue(lifecycleManager.getAll(sqlQueryId).isEmpty());
  }

  @Test
  public void testCancelBetweenValidateAndPlan() throws Exception
  {
    final String sqlQueryId = "toCancel";
    lifecycleAddLatch = new CountDownLatch(1);
    CountDownLatch validateAndAuthorizeLatch = new CountDownLatch(1);
    validateAndAuthorizeLatchSupplier.set(new NonnullPair<>(validateAndAuthorizeLatch, true));
    CountDownLatch planLatch = new CountDownLatch(1);
    planLatchSupplier.set(new NonnullPair<>(planLatch, false));
    Future<MockHttpServletResponse> future = executorService.submit(
        () -> postForResponse(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM foo"),
            makeRegularUserReq()
        )
    );
    Assert.assertTrue(validateAndAuthorizeLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    Assert.assertTrue(lifecycleAddLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    Response cancelResponse = resource.cancelQuery(sqlQueryId, makeRequestForCancel());
    planLatch.countDown();
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), cancelResponse.getStatus());

    Assert.assertTrue(lifecycleManager.getAll(sqlQueryId).isEmpty());

    MockHttpServletResponse queryResponse = future.get();
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), queryResponse.getStatus());
    QueryException exception = JSON_MAPPER.readValue(queryResponse.baos.toByteArray(), QueryException.class);
    Assert.assertEquals("Query cancelled", exception.getErrorCode());
  }

  @Test
  public void testCancelBetweenPlanAndExecute() throws Exception
  {
    final String sqlQueryId = "toCancel";
    CountDownLatch planLatch = new CountDownLatch(1);
    planLatchSupplier.set(new NonnullPair<>(planLatch, true));
    CountDownLatch execLatch = new CountDownLatch(1);
    executeLatchSupplier.set(new NonnullPair<>(execLatch, false));
    Future<MockHttpServletResponse> future = executorService.submit(
        () -> postForResponse(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM foo"),
            makeRegularUserReq()
        )
    );
    Assert.assertTrue(planLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    Response cancelResponse = resource.cancelQuery(sqlQueryId, makeRequestForCancel());
    execLatch.countDown();
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), cancelResponse.getStatus());

    Assert.assertTrue(lifecycleManager.getAll(sqlQueryId).isEmpty());

    MockHttpServletResponse queryResponse = future.get();
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), queryResponse.getStatus());
    QueryException exception = JSON_MAPPER.readValue(queryResponse.baos.toByteArray(), QueryException.class);
    Assert.assertEquals("Query cancelled", exception.getErrorCode());
  }

  @Test
  public void testCancelInvalidQuery() throws Exception
  {
    final String sqlQueryId = "validQuery";
    CountDownLatch planLatch = new CountDownLatch(1);
    planLatchSupplier.set(new NonnullPair<>(planLatch, true));
    CountDownLatch execLatch = new CountDownLatch(1);
    executeLatchSupplier.set(new NonnullPair<>(execLatch, false));
    Future<MockHttpServletResponse> future = executorService.submit(
        () -> postForResponse(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM foo"),
            makeRegularUserReq()
        )
    );
    Assert.assertTrue(planLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    Response cancelResponse = resource.cancelQuery("invalidQuery", makeRequestForCancel());
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), cancelResponse.getStatus());

    Assert.assertFalse(lifecycleManager.getAll(sqlQueryId).isEmpty());

    execLatch.countDown();
    MockHttpServletResponse queryResponse = future.get();
    Assert.assertEquals(Status.OK.getStatusCode(), queryResponse.getStatus());
  }

  @Test
  public void testCancelForbidden() throws Exception
  {
    final String sqlQueryId = "toCancel";
    CountDownLatch planLatch = new CountDownLatch(1);
    planLatchSupplier.set(new NonnullPair<>(planLatch, true));
    CountDownLatch execLatch = new CountDownLatch(1);
    executeLatchSupplier.set(new NonnullPair<>(execLatch, false));
    Future<MockHttpServletResponse> future = executorService.submit(
        () -> postForResponse(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM forbiddenDatasource"),
            makeSuperUserReq()
        )
    );
    Assert.assertTrue(planLatch.await(3, TimeUnit.SECONDS));
    Response cancelResponse = resource.cancelQuery(sqlQueryId, makeRequestForCancel());
    Assert.assertEquals(Status.FORBIDDEN.getStatusCode(), cancelResponse.getStatus());

    Assert.assertFalse(lifecycleManager.getAll(sqlQueryId).isEmpty());

    execLatch.countDown();
    MockHttpServletResponse queryResponse = future.get();
    Assert.assertEquals(Status.OK.getStatusCode(), queryResponse.getStatus());
  }

  @Test
  public void testQueryContextException() throws Exception
  {
    final String sqlQueryId = "badQueryContextTimeout";
    Map<String, Object> queryContext = ImmutableMap.of(
        QueryContexts.TIMEOUT_KEY,
        "2000'",
        BaseQuery.SQL_QUERY_ID,
        sqlQueryId
    );
    final QueryException queryContextException = doPost(
        new SqlQuery(
            "SELECT 1337",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            queryContext,
            null
        )
    ).lhs;
    Assert.assertNotNull(queryContextException);
    Assert.assertEquals(QueryException.BAD_QUERY_CONTEXT_ERROR_CODE, queryContextException.getErrorCode());
    Assert.assertEquals(BadQueryContextException.ERROR_CLASS, queryContextException.getErrorClass());
    Assert.assertTrue(queryContextException.getMessage().contains("2000'"));
    checkSqlRequestLog(false);
    Assert.assertTrue(lifecycleManager.getAll(sqlQueryId).isEmpty());
  }

  @Test
  public void testQueryContextKeyNotAllowed() throws Exception
  {
    Map<String, Object> queryContext = ImmutableMap.of(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, "all");
    final QueryException queryContextException = doPost(
        new SqlQuery(
            "SELECT 1337",
            ResultFormat.OBJECT,
            false,
            false,
            false,
            queryContext,
            null
        )
    ).lhs;
    Assert.assertNotNull(queryContextException);
    Assert.assertEquals(PlanningError.VALIDATION_ERROR.getErrorCode(), queryContextException.getErrorCode());
    MatcherAssert.assertThat(
        queryContextException.getMessage(),
        CoreMatchers.containsString(
            "Cannot execute query with context parameter [sqlInsertSegmentGranularity]")
    );
    checkSqlRequestLog(false);
  }

  @SuppressWarnings("unchecked")
  private void checkSqlRequestLog(boolean success)
  {
    Assert.assertEquals(1, testRequestLogger.getSqlQueryLogs().size());

    final Map<String, Object> stats = testRequestLogger.getSqlQueryLogs().get(0).getQueryStats().getStats();
    final Map<String, Object> queryContext = (Map<String, Object>) stats.get("context");
    Assert.assertEquals(success, stats.get("success"));
    Assert.assertEquals(CalciteTests.REGULAR_USER_AUTH_RESULT.getIdentity(), stats.get("identity"));
    Assert.assertTrue(stats.containsKey("sqlQuery/time"));
    Assert.assertTrue(stats.containsKey("sqlQuery/planningTimeMs"));
    Assert.assertTrue(queryContext.containsKey(QueryContexts.CTX_SQL_QUERY_ID));
    if (success) {
      Assert.assertTrue(stats.containsKey("sqlQuery/bytes"));
    } else {
      Assert.assertTrue(stats.containsKey("exception"));
    }
  }

  private static SqlQuery createSimpleQueryWithId(String sqlQueryId, String sql)
  {
    return new SqlQuery(sql, null, false, false, false, ImmutableMap.of(BaseQuery.SQL_QUERY_ID, sqlQueryId), null);
  }

  private Pair<QueryException, List<Map<String, Object>>> doPost(final SqlQuery query) throws Exception
  {
    return doPost(query, new TypeReference<List<Map<String, Object>>>()
    {
    });
  }

  // Returns either an error or a result, assuming the result is a JSON object.
  private <T> Pair<QueryException, T> doPost(
      final SqlQuery query,
      final TypeReference<T> typeReference
  ) throws Exception
  {
    return doPost(query, req, typeReference);
  }

  private Pair<QueryException, String> doPostRaw(final SqlQuery query) throws Exception
  {
    return doPostRaw(query, req);
  }

  private Pair<QueryException, List<Map<String, Object>>> doPost(final SqlQuery query, MockHttpServletRequest req)
      throws Exception
  {
    return doPost(query, req, new TypeReference<List<Map<String, Object>>>()
    {
    });
  }

  // Returns either an error or a result, assuming the result is a JSON object.
  @SuppressWarnings("unchecked")
  private <T> Pair<QueryException, T> doPost(
      final SqlQuery query,
      final MockHttpServletRequest req,
      final TypeReference<T> typeReference
  ) throws Exception
  {
    final Pair<QueryException, String> pair = doPostRaw(query, req);
    if (pair.rhs == null) {
      //noinspection unchecked
      return (Pair<QueryException, T>) pair;
    } else {
      return Pair.of(pair.lhs, JSON_MAPPER.readValue(pair.rhs, typeReference));
    }
  }

  // Returns either an error or a result.
  private Pair<QueryException, String> doPostRaw(final SqlQuery query, final MockHttpServletRequest req)
      throws Exception
  {
    MockHttpServletResponse response = postForResponse(query, req);

    if (response.getStatus() == 200) {
      return Pair.of(null, new String(response.baos.toByteArray(), StandardCharsets.UTF_8));
    } else {
      return Pair.of(JSON_MAPPER.readValue(response.baos.toByteArray(), QueryException.class), null);
    }
  }

  @Nonnull
  private MockHttpServletResponse postForResponse(SqlQuery query, MockHttpServletRequest req)
  {
    MockHttpServletResponse response = MockHttpServletResponse.forRequest(req);

    Assert.assertNull(resource.doPost(query, req));
    return response;
  }

  private MockHttpServletRequest makeSuperUserReq()
  {
    return makeExpectedReq(CalciteTests.SUPER_USER_AUTH_RESULT);
  }

  private MockHttpServletRequest makeRegularUserReq()
  {
    return makeExpectedReq(CalciteTests.REGULAR_USER_AUTH_RESULT);
  }

  private MockHttpServletRequest makeExpectedReq(AuthenticationResult authenticationResult)
  {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
    return req;
  }

  private MockHttpServletRequest makeRequestForCancel()
  {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, CalciteTests.REGULAR_USER_AUTH_RESULT);
    return req;
  }

  private static Function<Sequence<Object[]>, Sequence<Object[]>> errorAfterSecondRowMapFn()
  {
    return results -> {
      final AtomicLong rows = new AtomicLong();
      return results
          .flatMap(
              row -> Sequences.simple(new AbstractList<Object[]>()
              {
                @Override
                public Object[] get(int index)
                {
                  return row;
                }

                @Override
                public int size()
                {
                  return 1000;
                }
              })
          )
          .map(row -> {
            if (rows.incrementAndGet() == 3) {
              throw new ISE("Oh no!");
            } else {
              return row;
            }
          });
    };
  }

  private static class TestHttpStatement extends HttpStatement
  {
    private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> validateAndAuthorizeLatchSupplier;
    private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> planLatchSupplier;
    private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> executeLatchSupplier;
    private final SettableSupplier<Function<Sequence<Object[]>, Sequence<Object[]>>> sequenceMapFnSupplier;
    private final SettableSupplier<ResponseContext> responseContextSupplier;
    private final Consumer<DirectStatement> onExecute;

    private TestHttpStatement(
        final SqlToolbox lifecycleContext,
        final SqlQuery sqlQuery,
        final HttpServletRequest req,
        SettableSupplier<NonnullPair<CountDownLatch, Boolean>> validateAndAuthorizeLatchSupplier,
        SettableSupplier<NonnullPair<CountDownLatch, Boolean>> planLatchSupplier,
        SettableSupplier<NonnullPair<CountDownLatch, Boolean>> executeLatchSupplier,
        SettableSupplier<Function<Sequence<Object[]>, Sequence<Object[]>>> sequenceMapFnSupplier,
        SettableSupplier<ResponseContext> responseContextSupplier,
        final Consumer<DirectStatement> onAuthorize
    )
    {
      super(lifecycleContext, sqlQuery, req);
      this.validateAndAuthorizeLatchSupplier = validateAndAuthorizeLatchSupplier;
      this.planLatchSupplier = planLatchSupplier;
      this.executeLatchSupplier = executeLatchSupplier;
      this.sequenceMapFnSupplier = sequenceMapFnSupplier;
      this.responseContextSupplier = responseContextSupplier;
      this.onExecute = onAuthorize;
    }

    @Override
    protected void authorize(
        DruidPlanner planner,
        Function<Set<ResourceAction>, Access> authorizer
    )
    {
      if (validateAndAuthorizeLatchSupplier.get() != null) {
        if (validateAndAuthorizeLatchSupplier.get().rhs) {
          super.authorize(planner, authorizer);
          validateAndAuthorizeLatchSupplier.get().lhs.countDown();
        } else {
          try {
            if (!validateAndAuthorizeLatchSupplier.get().lhs.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS)) {
              throw new RuntimeException("Latch timed out");
            }
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          super.authorize(planner, authorizer);
        }
      } else {
        super.authorize(planner, authorizer);
      }
    }

    @Override
    public PlannerResult createPlan(DruidPlanner planner)
    {
      final NonnullPair<CountDownLatch, Boolean> planLatch = planLatchSupplier.get();
      if (planLatch != null) {
        if (planLatch.rhs) {
          PlannerResult result = super.createPlan(planner);
          planLatch.lhs.countDown();
          return result;
        } else {
          try {
            if (!planLatch.lhs.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS)) {
              throw new RuntimeException("Latch timed out");
            }
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return super.createPlan(planner);
        }
      } else {
        return super.createPlan(planner);
      }
    }

    @Override
    public ResultSet plan()
    {
      onExecute.accept(this);
      return super.plan();
    }

    @Override
    public ResultSet createResultSet(PlannerResult plannerResult)
    {
      return new ResultSet(plannerResult)
      {
        @Override
        public QueryResponse<Object[]> run()
        {
          final Function<Sequence<Object[]>, Sequence<Object[]>> sequenceMapFn =
              Optional.ofNullable(sequenceMapFnSupplier.get()).orElse(Function.identity());

          final NonnullPair<CountDownLatch, Boolean> executeLatch = executeLatchSupplier.get();
          if (executeLatch != null) {
            if (executeLatch.rhs) {
              final QueryResponse<Object[]> resp = super.run();
              Sequence<Object[]> sequence = sequenceMapFn.apply(resp.getResults());
              executeLatch.lhs.countDown();
              final ResponseContext respContext = resp.getResponseContext();
              respContext.merge(responseContextSupplier.get());
              return new QueryResponse<>(sequence, respContext);
            } else {
              try {
                if (!executeLatch.lhs.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS)) {
                  throw new RuntimeException("Latch timed out");
                }
              }
              catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          }

          final QueryResponse<Object[]> resp = super.run();
          Sequence<Object[]> sequence = sequenceMapFn.apply(resp.getResults());
          final ResponseContext respContext = resp.getResponseContext();
          respContext.merge(responseContextSupplier.get());
          return new QueryResponse<>(sequence, respContext);
        }
      };
    }
  }
}
