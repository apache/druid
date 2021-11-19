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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.tools.RelConversionException;
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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlPlanningException.PlanningError;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlResourceTest extends CalciteTestBase
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final String DUMMY_SQL_QUERY_ID = "dummy";

  private static final List<String> EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS =
      Arrays.asList("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1", "EXPR$8");

  private static final List<String> EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS =
      Arrays.asList("LONG", "LONG", "STRING", "STRING", "STRING", "FLOAT", "DOUBLE", "COMPLEX<hyperUnique>", "STRING");

  private static final List<String> EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS =
      Arrays.asList("TIMESTAMP", "BIGINT", "VARCHAR", "VARCHAR", "VARCHAR", "FLOAT", "DOUBLE", "OTHER", "VARCHAR");

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();
  private SpecificSegmentsQuerySegmentWalker walker = null;
  private TestRequestLogger testRequestLogger;
  private SqlResource resource;
  private HttpServletRequest req;
  private ListeningExecutorService executorService;
  private SqlLifecycleManager lifecycleManager;
  private SqlLifecycleFactory sqlLifecycleFactory;

  private CountDownLatch lifecycleAddLatch;
  private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> validateAndAuthorizeLatchSupplier = new SettableSupplier<>();
  private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> planLatchSupplier = new SettableSupplier<>();
  private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> executeLatchSupplier = new SettableSupplier<>();
  private final SettableSupplier<Function<Sequence<Object[]>, Sequence<Object[]>>> sequenceMapFnSupplier = new SettableSupplier<>();

  private boolean sleep = false;

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

    final PlannerConfig plannerConfig = new PlannerConfig()
    {
      @Override
      public boolean shouldSerializeComplexValues()
      {
        return false;
      }
    };
    final DruidSchemaCatalog rootSchema = CalciteTests.createMockRootSchema(
        conglomerate,
        walker,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(req.getRemoteAddr()).andReturn(null).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.replay(req);

    testRequestLogger = new TestRequestLogger();

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        operatorTable,
        macroTable,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME
    );

    lifecycleManager = new SqlLifecycleManager()
    {
      @Override
      public void add(String sqlQueryId, SqlLifecycle lifecycle)
      {
        super.add(sqlQueryId, lifecycle);
        if (lifecycleAddLatch != null) {
          lifecycleAddLatch.countDown();
        }
      }
    };
    final ServiceEmitter emitter = new NoopServiceEmitter();
    sqlLifecycleFactory = new SqlLifecycleFactory(
        plannerFactory,
        emitter,
        testRequestLogger,
        scheduler
    )
    {
      @Override
      public SqlLifecycle factorize()
      {
        return new TestSqlLifecycle(
            plannerFactory,
            emitter,
            testRequestLogger,
            scheduler,
            System.currentTimeMillis(),
            System.nanoTime(),
            validateAndAuthorizeLatchSupplier,
            planLatchSupplier,
            executeLatchSupplier,
            sequenceMapFnSupplier
        );
      }
    };
    resource = new SqlResource(
        JSON_MAPPER,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        sqlLifecycleFactory,
        lifecycleManager,
        new ServerConfig()
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

  @Test
  public void testUnauthorized() throws Exception
  {
    HttpServletRequest testRequest = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(testRequest.getRemoteAddr()).andReturn(null).once();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    testRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().once();
    EasyMock.replay(testRequest);

    try {
      resource.doPost(
          createSimpleQueryWithId("id", "select count(*) from forbiddenDatasource"),
          testRequest
      );
      Assert.fail("doPost did not throw ForbiddenException for an unauthorized query");
    }
    catch (ForbiddenException e) {
      // expected
    }
    Assert.assertEquals(0, testRequestLogger.getSqlQueryLogs().size());
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
                1,
                "",
                "a",
                "[\"a\",\"b\"]",
                1.0,
                1.0,
                "org.apache.druid.hll.VersionOneHyperLogLogCollector",
                nullStr
            ),
            Arrays.asList(
                "2000-01-02T00:00:00.000Z",
                1,
                "10.1",
                nullStr,
                "[\"b\",\"c\"]",
                2.0,
                2.0,
                "org.apache.druid.hll.VersionOneHyperLogLogCollector",
                nullStr
            )
        ),
        doPost(
            new SqlQuery(query, ResultFormat.ARRAY, false, false, false, null, null),
            new TypeReference<List<List<Object>>>() {}
        ).rhs
    );
  }

  @Test
  public void testArrayResultFormatWithErrorAfterFirstRow() throws Exception
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

    Assert.assertEquals(
        ImmutableList.of(
            EXPECTED_COLUMNS_FOR_RESULT_FORMAT_TESTS,
            EXPECTED_TYPES_FOR_RESULT_FORMAT_TESTS,
            EXPECTED_SQL_TYPES_FOR_RESULT_FORMAT_TESTS,
            Arrays.asList(
                "2000-01-01T00:00:00.000Z",
                1,
                "",
                "a",
                "[\"a\",\"b\"]",
                1.0,
                1.0,
                "org.apache.druid.hll.VersionOneHyperLogLogCollector",
                nullStr
            ),
            Arrays.asList(
                "2000-01-02T00:00:00.000Z",
                1,
                "10.1",
                nullStr,
                "[\"b\",\"c\"]",
                2.0,
                2.0,
                "org.apache.druid.hll.VersionOneHyperLogLogCollector",
                nullStr
            )
        ),
        doPost(
            new SqlQuery(query, ResultFormat.ARRAY, true, true, true, null, null),
            new TypeReference<List<List<Object>>>() {}
        ).rhs
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
            1,
            "",
            "a",
            "[\"a\",\"b\"]",
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
            1,
            "10.1",
            nullStr,
            "[\"b\",\"c\"]",
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
            1,
            "",
            "a",
            "[\"a\",\"b\"]",
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
            1,
            "10.1",
            nullStr,
            "[\"b\",\"c\"]",
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
            new TypeReference<List<Map<String, Object>>>() {}
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
            "2000-01-01T00:00:00.000Z,1,,a,\"[\"\"a\"\",\"\"b\"\"]\",1.0,1.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
            "2000-01-02T00:00:00.000Z,1,10.1,,\"[\"\"b\"\",\"\"c\"\"]\",2.0,2.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
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
            "2000-01-01T00:00:00.000Z,1,,a,\"[\"\"a\"\",\"\"b\"\"]\",1.0,1.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
            "2000-01-02T00:00:00.000Z,1,10.1,,\"[\"\"b\"\",\"\"c\"\"]\",2.0,2.0,org.apache.druid.hll.VersionOneHyperLogLogCollector,",
            "",
            ""
        ),
        lines
    );
  }

  @Test
  public void testExplainCountStar() throws Exception
  {
    Map<String, Object> queryContext = ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, DUMMY_SQL_QUERY_ID);
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
                    "DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"descending\":false,\"virtualColumns\":[],\"filter\":null,\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"postAggregations\":[],\"limit\":2147483647,\"context\":{\"sqlQueryId\":\"%s\"}}], signature=[{a0:LONG}])\n",
                    DUMMY_SQL_QUERY_ID
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
    Assert.assertEquals(QueryInterruptedException.UNKNOWN_EXCEPTION, exception.getErrorCode());
    Assert.assertEquals(ISE.class.getName(), exception.getErrorClass());
    Assert.assertTrue(
        exception.getMessage()
                 .contains("Cannot build plan for query: SELECT dim1 FROM druid.foo ORDER BY dim1")
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
            ImmutableMap.of("maxMergingDictionarySize", 1, "sqlQueryId", "id"),
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(exception.getErrorCode(), ResourceLimitExceededException.ERROR_CODE);
    Assert.assertEquals(exception.getErrorClass(), ResourceLimitExceededException.class.getName());
    checkSqlRequestLog(false);
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testUnsupportedQueryThrowsException() throws Exception
  {
    String errorMessage = "This will be support in Druid 9999";
    SqlQuery badQuery = EasyMock.createMock(SqlQuery.class);
    EasyMock.expect(badQuery.getQuery()).andReturn("SELECT ANSWER TO LIFE");
    EasyMock.expect(badQuery.getContext()).andReturn(ImmutableMap.of("sqlQueryId", "id"));
    EasyMock.expect(badQuery.getParameterList()).andThrow(new QueryUnsupportedException(errorMessage));
    EasyMock.replay(badQuery);
    final QueryException exception = doPost(badQuery).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(QueryUnsupportedException.ERROR_CODE, exception.getErrorCode());
    Assert.assertEquals(QueryUnsupportedException.class.getName(), exception.getErrorClass());
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testErrorResponseReturnSameQueryIdWhenSetInContext() throws Exception
  {
    String queryId = "id123";
    String errorMessage = "This will be support in Druid 9999";
    SqlQuery badQuery = EasyMock.createMock(SqlQuery.class);
    EasyMock.expect(badQuery.getQuery()).andReturn("SELECT ANSWER TO LIFE");
    EasyMock.expect(badQuery.getContext()).andReturn(ImmutableMap.of("sqlQueryId", queryId));
    EasyMock.expect(badQuery.getParameterList()).andThrow(new QueryUnsupportedException(errorMessage));
    EasyMock.replay(badQuery);
    final Response response = resource.doPost(badQuery, req);
    Assert.assertNotEquals(200, response.getStatus());
    final MultivaluedMap<String, Object> headers = response.getMetadata();
    Assert.assertTrue(headers.containsKey(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER));
    Assert.assertEquals(1, headers.get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER).size());
    Assert.assertEquals(queryId, headers.get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER).get(0));
  }

  @Test
  public void testErrorResponseReturnNewQueryIdWhenNotSetInContext() throws Exception
  {
    String errorMessage = "This will be support in Druid 9999";
    SqlQuery badQuery = EasyMock.createMock(SqlQuery.class);
    EasyMock.expect(badQuery.getQuery()).andReturn("SELECT ANSWER TO LIFE");
    EasyMock.expect(badQuery.getContext()).andReturn(ImmutableMap.of());
    EasyMock.expect(badQuery.getParameterList()).andThrow(new QueryUnsupportedException(errorMessage));
    EasyMock.replay(badQuery);
    final Response response = resource.doPost(badQuery, req);
    Assert.assertNotEquals(200, response.getStatus());
    final MultivaluedMap<String, Object> headers = response.getMetadata();
    Assert.assertTrue(headers.containsKey(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER));
    Assert.assertEquals(1, headers.get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER).size());
    Assert.assertFalse(Strings.isNullOrEmpty(headers.get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER).get(0).toString()));
  }

  @Test
  public void testUnsupportedQueryThrowsExceptionWithFilterResponse() throws Exception
  {
    resource = new SqlResource(
        JSON_MAPPER,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        sqlLifecycleFactory,
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
        }
    );

    String errorMessage = "This will be support in Druid 9999";
    SqlQuery badQuery = EasyMock.createMock(SqlQuery.class);
    EasyMock.expect(badQuery.getQuery()).andReturn("SELECT ANSWER TO LIFE");
    EasyMock.expect(badQuery.getContext()).andReturn(ImmutableMap.of("sqlQueryId", "id"));
    EasyMock.expect(badQuery.getParameterList()).andThrow(new QueryUnsupportedException(errorMessage));
    EasyMock.replay(badQuery);
    final QueryException exception = doPost(badQuery).lhs;

    Assert.assertNotNull(exception);
    Assert.assertNull(exception.getMessage());
    Assert.assertNull(exception.getHost());
    Assert.assertEquals(exception.getErrorCode(), QueryUnsupportedException.ERROR_CODE);
    Assert.assertNull(exception.getErrorClass());
    Assert.assertTrue(lifecycleManager.getAll("id").isEmpty());
  }

  @Test
  public void testAssertionErrorThrowsErrorWithFilterResponse() throws Exception
  {
    resource = new SqlResource(
        JSON_MAPPER,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        sqlLifecycleFactory,
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
        }
    );

    String errorMessage = "could not assert";
    SqlQuery badQuery = EasyMock.createMock(SqlQuery.class);
    EasyMock.expect(badQuery.getQuery()).andReturn("SELECT ANSWER TO LIFE");
    EasyMock.expect(badQuery.getContext()).andReturn(ImmutableMap.of("sqlQueryId", "id"));
    EasyMock.expect(badQuery.getParameterList()).andThrow(new Error(errorMessage));
    EasyMock.replay(badQuery);
    final QueryException exception = doPost(badQuery).lhs;

    Assert.assertNotNull(exception);
    Assert.assertNull(exception.getMessage());
    Assert.assertNull(exception.getHost());
    Assert.assertEquals(exception.getErrorCode(), QueryInterruptedException.UNKNOWN_EXCEPTION);
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
                  ImmutableMap.of("priority", -5, "sqlQueryId", sqlQueryId),
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
        Assert.assertEquals(QueryCapacityExceededException.ERROR_CODE, interruped.getErrorCode());
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
    Map<String, Object> queryContext = ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1, "sqlQueryId", sqlQueryId);
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
    Assert.assertEquals(timeoutException.getErrorCode(), QueryTimeoutException.ERROR_CODE);
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
    Future<Response> future = executorService.submit(
        () -> resource.doPost(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM foo"),
            makeRegularUserReq()
        )
    );
    Assert.assertTrue(validateAndAuthorizeLatch.await(1, TimeUnit.SECONDS));
    Assert.assertTrue(lifecycleAddLatch.await(1, TimeUnit.SECONDS));
    Response response = resource.cancelQuery(sqlQueryId, mockRequestForCancel());
    planLatch.countDown();
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());

    Assert.assertTrue(lifecycleManager.getAll(sqlQueryId).isEmpty());

    response = future.get();
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    QueryException exception = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(
        QueryInterruptedException.QUERY_CANCELLED,
        exception.getErrorCode()
    );
  }

  @Test
  public void testCancelBetweenPlanAndExecute() throws Exception
  {
    final String sqlQueryId = "toCancel";
    CountDownLatch planLatch = new CountDownLatch(1);
    planLatchSupplier.set(new NonnullPair<>(planLatch, true));
    CountDownLatch execLatch = new CountDownLatch(1);
    executeLatchSupplier.set(new NonnullPair<>(execLatch, false));
    Future<Response> future = executorService.submit(
        () -> resource.doPost(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM foo"),
            makeRegularUserReq()
        )
    );
    Assert.assertTrue(planLatch.await(1, TimeUnit.SECONDS));
    Response response = resource.cancelQuery(sqlQueryId, mockRequestForCancel());
    execLatch.countDown();
    Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());

    Assert.assertTrue(lifecycleManager.getAll(sqlQueryId).isEmpty());

    response = future.get();
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    QueryException exception = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(
        QueryInterruptedException.QUERY_CANCELLED,
        exception.getErrorCode()
    );
  }

  @Test
  public void testCancelInvalidQuery() throws Exception
  {
    final String sqlQueryId = "validQuery";
    CountDownLatch planLatch = new CountDownLatch(1);
    planLatchSupplier.set(new NonnullPair<>(planLatch, true));
    CountDownLatch execLatch = new CountDownLatch(1);
    executeLatchSupplier.set(new NonnullPair<>(execLatch, false));
    Future<Response> future = executorService.submit(
        () -> resource.doPost(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM foo"),
            makeRegularUserReq()
        )
    );
    Assert.assertTrue(planLatch.await(1, TimeUnit.SECONDS));
    Response response = resource.cancelQuery("invalidQuery", mockRequestForCancel());
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());

    Assert.assertFalse(lifecycleManager.getAll(sqlQueryId).isEmpty());

    execLatch.countDown();
    response = future.get();
    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testCancelForbidden() throws Exception
  {
    final String sqlQueryId = "toCancel";
    CountDownLatch planLatch = new CountDownLatch(1);
    planLatchSupplier.set(new NonnullPair<>(planLatch, true));
    CountDownLatch execLatch = new CountDownLatch(1);
    executeLatchSupplier.set(new NonnullPair<>(execLatch, false));
    Future<Response> future = executorService.submit(
        () -> resource.doPost(
            createSimpleQueryWithId(sqlQueryId, "SELECT DISTINCT dim1 FROM forbiddenDatasource"),
            makeSuperUserReq()
        )
    );
    Assert.assertTrue(planLatch.await(1, TimeUnit.SECONDS));
    Response response = resource.cancelQuery(sqlQueryId, mockRequestForCancel());
    Assert.assertEquals(Status.FORBIDDEN.getStatusCode(), response.getStatus());

    Assert.assertFalse(lifecycleManager.getAll(sqlQueryId).isEmpty());

    execLatch.countDown();
    response = future.get();
    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
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
    Assert.assertTrue(queryContext.containsKey(PlannerContext.CTX_SQL_QUERY_ID));
    if (success) {
      Assert.assertTrue(stats.containsKey("sqlQuery/bytes"));
    } else {
      Assert.assertTrue(stats.containsKey("exception"));
    }
  }

  private static SqlQuery createSimpleQueryWithId(String sqlQueryId, String sql)
  {
    return new SqlQuery(sql, null, false, false, false, ImmutableMap.of("sqlQueryId", sqlQueryId), null);
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

  private Pair<QueryException, List<Map<String, Object>>> doPost(final SqlQuery query, HttpServletRequest req)
      throws Exception
  {
    return doPost(query, req, new TypeReference<List<Map<String, Object>>>()
    {
    });
  }

  // Returns either an error or a result, assuming the result is a JSON object.
  private <T> Pair<QueryException, T> doPost(
      final SqlQuery query,
      final HttpServletRequest req,
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
  private Pair<QueryException, String> doPostRaw(final SqlQuery query, final HttpServletRequest req) throws Exception
  {
    final Response response = resource.doPost(query, req);
    if (response.getStatus() == 200) {
      final StreamingOutput output = (StreamingOutput) response.getEntity();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        output.write(baos);
      }
      catch (Exception ignored) {
        // Suppress errors and return the response so far. Similar to what the real web server would do, if it
        // started writing a 200 OK and then threw an exception in the middle.
      }

      return Pair.of(
          null,
          new String(baos.toByteArray(), StandardCharsets.UTF_8)
      );
    } else {
      return Pair.of(
          JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryException.class),
          null
      );
    }
  }

  private HttpServletRequest makeSuperUserReq()
  {
    return makeExpectedReq(CalciteTests.SUPER_USER_AUTH_RESULT);
  }

  private HttpServletRequest makeRegularUserReq()
  {
    return makeExpectedReq(CalciteTests.REGULAR_USER_AUTH_RESULT);
  }

  private HttpServletRequest makeExpectedReq(AuthenticationResult authenticationResult)
  {
    HttpServletRequest req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(req.getRemoteAddr()).andReturn(null).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .anyTimes();
    EasyMock.replay(req);
    return req;
  }

  private HttpServletRequest mockRequestForCancel()
  {
    HttpServletRequest req = EasyMock.createNiceMock(HttpServletRequest.class);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(req);
    return req;
  }

  private static Function<Sequence<Object[]>, Sequence<Object[]>> errorAfterSecondRowMapFn()
  {
    return results -> {
      final AtomicLong rows = new AtomicLong();
      return results.map(row -> {
        if (rows.incrementAndGet() == 3) {
          throw new ISE("Oh no!");
        } else {
          return row;
        }
      });
    };
  }

  private static class TestSqlLifecycle extends SqlLifecycle
  {
    private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> validateAndAuthorizeLatchSupplier;
    private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> planLatchSupplier;
    private final SettableSupplier<NonnullPair<CountDownLatch, Boolean>> executeLatchSupplier;
    private final SettableSupplier<Function<Sequence<Object[]>, Sequence<Object[]>>> sequenceMapFnSupplier;

    private TestSqlLifecycle(
        PlannerFactory plannerFactory,
        ServiceEmitter emitter,
        RequestLogger requestLogger,
        QueryScheduler queryScheduler,
        long startMs,
        long startNs,
        SettableSupplier<NonnullPair<CountDownLatch, Boolean>> validateAndAuthorizeLatchSupplier,
        SettableSupplier<NonnullPair<CountDownLatch, Boolean>> planLatchSupplier,
        SettableSupplier<NonnullPair<CountDownLatch, Boolean>> executeLatchSupplier,
        SettableSupplier<Function<Sequence<Object[]>, Sequence<Object[]>>> sequenceMapFnSupplier
    )
    {
      super(plannerFactory, emitter, requestLogger, queryScheduler, startMs, startNs);
      this.validateAndAuthorizeLatchSupplier = validateAndAuthorizeLatchSupplier;
      this.planLatchSupplier = planLatchSupplier;
      this.executeLatchSupplier = executeLatchSupplier;
      this.sequenceMapFnSupplier = sequenceMapFnSupplier;
    }

    @Override
    public void validateAndAuthorize(HttpServletRequest req)
    {
      if (validateAndAuthorizeLatchSupplier.get() != null) {
        if (validateAndAuthorizeLatchSupplier.get().rhs) {
          super.validateAndAuthorize(req);
          validateAndAuthorizeLatchSupplier.get().lhs.countDown();
        } else {
          try {
            if (!validateAndAuthorizeLatchSupplier.get().lhs.await(1, TimeUnit.SECONDS)) {
              throw new RuntimeException("Latch timed out");
            }
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          super.validateAndAuthorize(req);
        }
      } else {
        super.validateAndAuthorize(req);
      }
    }

    @Override
    public void plan() throws RelConversionException
    {
      if (planLatchSupplier.get() != null) {
        if (planLatchSupplier.get().rhs) {
          super.plan();
          planLatchSupplier.get().lhs.countDown();
        } else {
          try {
            if (!planLatchSupplier.get().lhs.await(1, TimeUnit.SECONDS)) {
              throw new RuntimeException("Latch timed out");
            }
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          super.plan();
        }
      } else {
        super.plan();
      }
    }

    @Override
    public Sequence<Object[]> execute()
    {
      final Function<Sequence<Object[]>, Sequence<Object[]>> sequenceMapFn =
          Optional.ofNullable(sequenceMapFnSupplier.get()).orElse(Function.identity());

      if (executeLatchSupplier.get() != null) {
        if (executeLatchSupplier.get().rhs) {
          Sequence<Object[]> sequence = sequenceMapFn.apply(super.execute());
          executeLatchSupplier.get().lhs.countDown();
          return sequence;
        } else {
          try {
            if (!executeLatchSupplier.get().lhs.await(1, TimeUnit.SECONDS)) {
              throw new RuntimeException("Latch timed out");
            }
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return sequenceMapFn.apply(super.execute());
        }
      } else {
        return sequenceMapFn.apply(super.execute());
      }
    }
  }
}
