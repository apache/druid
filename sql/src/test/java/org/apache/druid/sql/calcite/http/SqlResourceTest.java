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

package org.apache.druid.sql.calcite.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;
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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlResourceTest extends CalciteTestBase
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final String DUMMY_SQL_QUERY_ID = "dummy";

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

  @BeforeClass
  public static void setUpClass()
  {
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    conglomerate = conglomerateCloserPair.lhs;
    resourceCloser = conglomerateCloserPair.rhs;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder.newFolder());

    final PlannerConfig plannerConfig = new PlannerConfig()
    {
      @Override
      public boolean shouldSerializeComplexValues()
      {
        return false;
      }
    };
    final DruidSchema druidSchema = CalciteTests.createMockSchema(conglomerate, walker, plannerConfig);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig);
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
        druidSchema,
        systemSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        operatorTable,
        macroTable,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper()
    );

    resource = new SqlResource(
        JSON_MAPPER,
        new SqlLifecycleFactory(
            plannerFactory,
            new NoopServiceEmitter(),
            testRequestLogger
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
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
          new SqlQuery("select count(*) from forbiddenDatasource", null, false, null),
          testRequest
      );
      Assert.fail("doPost did not throw ForbiddenException for an unauthorized query");
    }
    catch (ForbiddenException e) {
      // expected
    }
    Assert.assertEquals(0, testRequestLogger.getSqlQueryLogs().size());
  }

  @Test
  public void testCountStar() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo", null, false, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6, "TheFoo", "foo")
        ),
        rows
    );
    checkSqlRequestLog(true);
  }

  @Test
  public void testTimestampsInResponse() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            ResultFormat.OBJECT,
            false,
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
  public void testTimestampsInResponseLosAngelesTimeZone() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            ResultFormat.OBJECT,
            false,
            ImmutableMap.of(PlannerContext.CTX_SQL_TIME_ZONE, "America/Los_Angeles")
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
  public void testFieldAliasingSelect() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo LIMIT 1", ResultFormat.OBJECT, false, null)
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
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo GROUP BY dim2", ResultFormat.OBJECT, false, null)
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
        doPost(new SqlQuery(query, ResultFormat.ARRAY, false, null), new TypeReference<List<List<Object>>>() {}).rhs
    );
  }

  @Test
  public void testArrayResultFormatWithHeader() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;

    Assert.assertEquals(
        ImmutableList.of(
            Arrays.asList("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1", "EXPR$8"),
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
        doPost(new SqlQuery(query, ResultFormat.ARRAY, true, null), new TypeReference<List<List<Object>>>() {}).rhs
    );
  }

  @Test
  public void testArrayLinesResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String response = doPostRaw(new SqlQuery(query, ResultFormat.ARRAYLINES, false, null)).rhs;
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
    final String response = doPostRaw(new SqlQuery(query, ResultFormat.ARRAYLINES, true, null)).rhs;
    final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(5, lines.size());
    Assert.assertEquals(
        Arrays.asList("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1", "EXPR$8"),
        JSON_MAPPER.readValue(lines.get(0), List.class)
    );
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
        JSON_MAPPER.readValue(lines.get(1), List.class)
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
        JSON_MAPPER.readValue(lines.get(2), List.class)
    );
    Assert.assertEquals("", lines.get(3));
    Assert.assertEquals("", lines.get(4));
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
            new SqlQuery(query, ResultFormat.OBJECT, false, null),
            new TypeReference<List<Map<String, Object>>>() {}
        ).rhs
    );
  }

  @Test
  public void testObjectLinesResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String response = doPostRaw(new SqlQuery(query, ResultFormat.OBJECTLINES, false, null)).rhs;
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
  public void testCsvResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String response = doPostRaw(new SqlQuery(query, ResultFormat.CSV, false, null)).rhs;
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
    final String response = doPostRaw(new SqlQuery(query, ResultFormat.CSV, true, null)).rhs;
    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(
        ImmutableList.of(
            "__time,cnt,dim1,dim2,dim3,m1,m2,unique_dim1,EXPR$8",
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
        new SqlQuery("EXPLAIN PLAN FOR SELECT COUNT(*) AS cnt FROM druid.foo", ResultFormat.OBJECT, false, queryContext)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.<String, Object>of(
                "PLAN",
                StringUtils.format(
                    "DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"descending\":false,\"virtualColumns\":[],\"filter\":null,\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"postAggregations\":[],\"limit\":2147483647,\"context\":{\"skipEmptyBuckets\":true,\"sqlQueryId\":\"%s\"}}], signature=[{a0:LONG}])\n",
                    DUMMY_SQL_QUERY_ID
                )
            )
        ),
        rows
    );
  }

  @Test
  public void testCannotValidate() throws Exception
  {
    final QueryInterruptedException exception = doPost(
        new SqlQuery(
            "SELECT dim4 FROM druid.foo",
            ResultFormat.OBJECT,
            false,
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(QueryInterruptedException.UNKNOWN_EXCEPTION, exception.getErrorCode());
    Assert.assertEquals(ValidationException.class.getName(), exception.getErrorClass());
    Assert.assertTrue(exception.getMessage().contains("Column 'dim4' not found in any table"));
    checkSqlRequestLog(false);
  }

  @Test
  public void testCannotConvert() throws Exception
  {
    // SELECT + ORDER unsupported
    final QueryInterruptedException exception = doPost(
        new SqlQuery("SELECT dim1 FROM druid.foo ORDER BY dim1", ResultFormat.OBJECT, false, null)
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(QueryInterruptedException.UNKNOWN_EXCEPTION, exception.getErrorCode());
    Assert.assertEquals(ISE.class.getName(), exception.getErrorClass());
    Assert.assertTrue(
        exception.getMessage()
                 .contains("Cannot build plan for query: SELECT dim1 FROM druid.foo ORDER BY dim1")
    );
    checkSqlRequestLog(false);
  }

  @Test
  public void testResourceLimitExceeded() throws Exception
  {
    final QueryInterruptedException exception = doPost(
        new SqlQuery(
            "SELECT DISTINCT dim1 FROM foo",
            ResultFormat.OBJECT,
            false,
            ImmutableMap.of("maxMergingDictionarySize", 1)
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(exception.getErrorCode(), QueryInterruptedException.RESOURCE_LIMIT_EXCEEDED);
    Assert.assertEquals(exception.getErrorClass(), ResourceLimitExceededException.class.getName());
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
    Assert.assertTrue(queryContext.containsKey(PlannerContext.CTX_SQL_QUERY_ID));
    if (success) {
      Assert.assertTrue(stats.containsKey("sqlQuery/bytes"));
    } else {
      Assert.assertTrue(stats.containsKey("exception"));
    }
  }

  // Returns either an error or a result, assuming the result is a JSON object.
  private <T> Pair<QueryInterruptedException, T> doPost(
      final SqlQuery query,
      final TypeReference<T> typeReference
  ) throws Exception
  {
    final Pair<QueryInterruptedException, String> pair = doPostRaw(query);
    if (pair.rhs == null) {
      //noinspection unchecked
      return (Pair<QueryInterruptedException, T>) pair;
    } else {
      return Pair.of(pair.lhs, JSON_MAPPER.readValue(pair.rhs, typeReference));
    }
  }

  // Returns either an error or a result.
  private Pair<QueryInterruptedException, String> doPostRaw(final SqlQuery query) throws Exception
  {
    final Response response = resource.doPost(query, req);
    if (response.getStatus() == 200) {
      final StreamingOutput output = (StreamingOutput) response.getEntity();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      output.write(baos);
      return Pair.of(
          null,
          new String(baos.toByteArray(), StandardCharsets.UTF_8)
      );
    } else {
      return Pair.of(
          JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryInterruptedException.class),
          null
      );
    }
  }

  private Pair<QueryInterruptedException, List<Map<String, Object>>> doPost(final SqlQuery query) throws Exception
  {
    return doPost(query, new TypeReference<List<Map<String, Object>>>()
    {
    });
  }
}
