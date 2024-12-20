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

package org.apache.druid.msq.dart.controller.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.sql.DartQueryMaker;
import org.apache.druid.msq.dart.controller.sql.DartSqlClient;
import org.apache.druid.msq.dart.controller.sql.DartSqlClients;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.InvalidNullByteFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestControllerContext;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.mocks.MockAsyncContext;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Functional test of {@link DartSqlResource}, {@link DartSqlEngine}, and {@link DartQueryMaker}.
 * Other classes are mocked when possible.
 */
public class DartSqlResourceTest extends MSQTestBase
{
  private static final DruidNode SELF_NODE = new DruidNode("none", "localhost", false, 8080, -1, true, false);
  private static final String AUTHENTICATOR_NAME = "authn";
  private static final int MAX_CONTROLLERS = 1;

  /**
   * A user that is not a superuser.
   * See {@link CalciteTests#TEST_AUTHORIZER_MAPPER} for how this user is mapped.
   */
  private static final String REGULAR_USER_NAME = "regularUser";

  /**
   * A user that is not a superuser, and is different from {@link #REGULAR_USER_NAME}.
   * See {@link CalciteTests#TEST_AUTHORIZER_MAPPER} for how this user is mapped.
   */
  private static final String DIFFERENT_REGULAR_USER_NAME = "differentRegularUser";

  /**
   * Latch that cancellation tests can use to determine when a query is added to the {@link DartControllerRegistry},
   * and becomes cancelable.
   */
  private final CountDownLatch controllerRegistered = new CountDownLatch(1);

  // Objects created in setUp() below this line.

  private DartSqlResource sqlResource;
  private DartControllerRegistry controllerRegistry;
  private ExecutorService controllerExecutor;
  private AutoCloseable mockCloser;

  // Mocks below this line.

  /**
   * Mock for {@link DartSqlClients}, which is used in tests of {@link DartSqlResource#doGetRunningQueries}.
   */
  @Mock
  private DartSqlClients dartSqlClients;

  /**
   * Mock for {@link DartSqlClient}, which is used in tests of {@link DartSqlResource#doGetRunningQueries}.
   */
  @Mock
  private DartSqlClient dartSqlClient;

  /**
   * Mock http request.
   */
  @Mock
  private HttpServletRequest httpServletRequest;

  /**
   * Mock for test cases that need to make two requests.
   */
  @Mock
  private HttpServletRequest httpServletRequest2;

  @BeforeEach
  void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);

    final DartSqlEngine engine = new DartSqlEngine(
        queryId -> new MSQTestControllerContext(
            objectMapper,
            injector,
            null /* not used in this test */,
            workerMemoryParameters,
            loadedSegmentsMetadata,
            TaskLockType.APPEND,
            QueryContext.empty()
        ),
        controllerRegistry = new DartControllerRegistry()
        {
          @Override
          public void register(ControllerHolder holder)
          {
            super.register(holder);
            controllerRegistered.countDown();
          }
        },
        objectMapper.convertValue(ImmutableMap.of(), DartControllerConfig.class),
        controllerExecutor = Execs.multiThreaded(
            MAX_CONTROLLERS,
            StringUtils.encodeForFormat(getClass().getSimpleName() + "-controller-exec")
        )
    );

    final DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
        CalciteTests.INJECTOR,
        queryFramework().conglomerate(),
        queryFramework().walker(),
        new PlannerConfig(),
        new NoopViewManager(),
        new NoopDruidSchemaManager(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CatalogResolver.NULL_RESOLVER
    );

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        queryFramework().operatorTable(),
        queryFramework().macroTable(),
        PLANNER_CONFIG_DEFAULT,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        objectMapper,
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        new DruidHookDispatcher()
    );

    final SqlLifecycleManager lifecycleManager = new SqlLifecycleManager();
    final SqlToolbox toolbox = new SqlToolbox(
        engine,
        plannerFactory,
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new DefaultQueryConfig(ImmutableMap.of()),
        lifecycleManager
    );

    sqlResource = new DartSqlResource(
        objectMapper,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        new SqlStatementFactory(toolbox),
        controllerRegistry,
        lifecycleManager,
        dartSqlClients,
        new ServerConfig() /* currently only used for error transform strategy */,
        ResponseContextConfig.newConfig(false),
        SELF_NODE,
        new DefaultQueryConfig(ImmutableMap.of("foo", "bar"))
    );

    // Setup mocks
    Mockito.when(dartSqlClients.getAllClients()).thenReturn(Collections.singletonList(dartSqlClient));
  }

  @AfterEach
  void tearDown() throws Exception
  {
    mockCloser.close();

    // shutdown(), not shutdownNow(), to ensure controllers stop timely on their own.
    controllerExecutor.shutdown();

    if (!controllerExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
      throw new IAE("controllerExecutor.awaitTermination() timed out");
    }

    // Ensure that controllerRegistry has nothing in it at the conclusion of each test. Verifies that controllers
    // are fully cleaned up.
    Assertions.assertEquals(0, controllerRegistry.getAllHolders().size(), "controllerRegistry.getAllHolders().size()");
  }

  @Test
  public void test_getEnabled()
  {
    final Response response = sqlResource.doGetEnabled(httpServletRequest);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  /**
   * Test where a superuser calls {@link DartSqlResource#doGetRunningQueries} with selfOnly enabled.
   */
  @Test
  public void test_getRunningQueries_selfOnly_superUser()
  {
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(CalciteTests.TEST_SUPERUSER_NAME));

    final ControllerHolder holder = setUpMockRunningQuery(REGULAR_USER_NAME);

    Assertions.assertEquals(
        new GetQueriesResponse(Collections.singletonList(DartQueryInfo.fromControllerHolder(holder))),
        sqlResource.doGetRunningQueries("", httpServletRequest)
    );

    controllerRegistry.deregister(holder);
  }

  /**
   * Test where {@link #REGULAR_USER_NAME} and {@link #DIFFERENT_REGULAR_USER_NAME} issue queries, and
   * {@link #REGULAR_USER_NAME} calls {@link DartSqlResource#doGetRunningQueries} with selfOnly enabled.
   */
  @Test
  public void test_getRunningQueries_selfOnly_regularUser()
  {
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));

    final ControllerHolder holder = setUpMockRunningQuery(REGULAR_USER_NAME);
    final ControllerHolder holder2 = setUpMockRunningQuery(DIFFERENT_REGULAR_USER_NAME);

    // Regular users can see only their own queries, without authentication details.
    Assertions.assertEquals(2, controllerRegistry.getAllHolders().size());
    Assertions.assertEquals(
        new GetQueriesResponse(
            Collections.singletonList(DartQueryInfo.fromControllerHolder(holder).withoutAuthenticationResult())),
        sqlResource.doGetRunningQueries("", httpServletRequest)
    );

    controllerRegistry.deregister(holder);
    controllerRegistry.deregister(holder2);
  }

  /**
   * Test where a superuser calls {@link DartSqlResource#doGetRunningQueries} with selfOnly disabled.
   */
  @Test
  public void test_getRunningQueries_global_superUser()
  {
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(CalciteTests.TEST_SUPERUSER_NAME));

    // REGULAR_USER_NAME runs a query locally.
    final ControllerHolder localHolder = setUpMockRunningQuery(REGULAR_USER_NAME);

    // DIFFERENT_REGULAR_USER_NAME runs a query remotely.
    final DartQueryInfo remoteQueryInfo = new DartQueryInfo(
        "sid",
        "did2",
        "SELECT 2",
        "localhost:1002",
        AUTHENTICATOR_NAME,
        DIFFERENT_REGULAR_USER_NAME,
        DateTimes.of("2001"),
        ControllerHolder.State.RUNNING.toString()
    );
    Mockito.when(dartSqlClient.getRunningQueries(true))
           .thenReturn(Futures.immediateFuture(new GetQueriesResponse(Collections.singletonList(remoteQueryInfo))));

    // With selfOnly = null, the endpoint returns both queries.
    Assertions.assertEquals(
        new GetQueriesResponse(
            ImmutableList.of(
                DartQueryInfo.fromControllerHolder(localHolder),
                remoteQueryInfo
            )
        ),
        sqlResource.doGetRunningQueries(null, httpServletRequest)
    );

    controllerRegistry.deregister(localHolder);
  }

  /**
   * Test where a superuser calls {@link DartSqlResource#doGetRunningQueries} with selfOnly disabled, and where the
   * remote server has a problem.
   */
  @Test
  public void test_getRunningQueries_global_remoteError_superUser()
  {
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(CalciteTests.TEST_SUPERUSER_NAME));

    // REGULAR_USER_NAME runs a query locally.
    final ControllerHolder localHolder = setUpMockRunningQuery(REGULAR_USER_NAME);

    // Remote call fails.
    Mockito.when(dartSqlClient.getRunningQueries(true))
           .thenReturn(Futures.immediateFailedFuture(new IOException("something went wrong")));

    // We only see local queries, because the remote call failed. (The entire call doesn't fail; we see what we
    // were able to fetch.)
    Assertions.assertEquals(
        new GetQueriesResponse(ImmutableList.of(DartQueryInfo.fromControllerHolder(localHolder))),
        sqlResource.doGetRunningQueries(null, httpServletRequest)
    );

    controllerRegistry.deregister(localHolder);
  }

  /**
   * Test where {@link #REGULAR_USER_NAME} and {@link #DIFFERENT_REGULAR_USER_NAME} issue queries, and
   * {@link #REGULAR_USER_NAME} calls {@link DartSqlResource#doGetRunningQueries} with selfOnly disabled.
   */
  @Test
  public void test_getRunningQueries_global_regularUser()
  {
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));

    // REGULAR_USER_NAME runs a query locally.
    final ControllerHolder localHolder = setUpMockRunningQuery(REGULAR_USER_NAME);

    // DIFFERENT_REGULAR_USER_NAME runs a query remotely.
    final DartQueryInfo remoteQueryInfo = new DartQueryInfo(
        "sid",
        "did2",
        "SELECT 2",
        "localhost:1002",
        AUTHENTICATOR_NAME,
        DIFFERENT_REGULAR_USER_NAME,
        DateTimes.of("2000"),
        ControllerHolder.State.RUNNING.toString()
    );
    Mockito.when(dartSqlClient.getRunningQueries(true))
           .thenReturn(Futures.immediateFuture(new GetQueriesResponse(Collections.singletonList(remoteQueryInfo))));

    // The endpoint returns only the query issued by REGULAR_USER_NAME.
    Assertions.assertEquals(
        new GetQueriesResponse(
            ImmutableList.of(DartQueryInfo.fromControllerHolder(localHolder).withoutAuthenticationResult())),
        sqlResource.doGetRunningQueries(null, httpServletRequest)
    );

    controllerRegistry.deregister(localHolder);
  }

  /**
   * Test where {@link #REGULAR_USER_NAME} and {@link #DIFFERENT_REGULAR_USER_NAME} issue queries, and
   * {@link #DIFFERENT_REGULAR_USER_NAME} calls {@link DartSqlResource#doGetRunningQueries} with selfOnly disabled.
   */
  @Test
  public void test_getRunningQueries_global_differentRegularUser()
  {
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(DIFFERENT_REGULAR_USER_NAME));

    // REGULAR_USER_NAME runs a query locally.
    final ControllerHolder holder = setUpMockRunningQuery(REGULAR_USER_NAME);

    // DIFFERENT_REGULAR_USER_NAME runs a query remotely.
    final DartQueryInfo remoteQueryInfo = new DartQueryInfo(
        "sid",
        "did2",
        "SELECT 2",
        "localhost:1002",
        AUTHENTICATOR_NAME,
        DIFFERENT_REGULAR_USER_NAME,
        DateTimes.of("2000"),
        ControllerHolder.State.RUNNING.toString()
    );
    Mockito.when(dartSqlClient.getRunningQueries(true))
           .thenReturn(Futures.immediateFuture(new GetQueriesResponse(Collections.singletonList(remoteQueryInfo))));

    // The endpoint returns only the query issued by DIFFERENT_REGULAR_USER_NAME.
    Assertions.assertEquals(
        new GetQueriesResponse(ImmutableList.of(remoteQueryInfo.withoutAuthenticationResult())),
        sqlResource.doGetRunningQueries(null, httpServletRequest)
    );

    controllerRegistry.deregister(holder);
  }

  @Test
  public void test_doPost_regularUser()
  {
    final MockAsyncContext asyncContext = new MockAsyncContext();
    final MockHttpServletResponse asyncResponse = new MockHttpServletResponse();
    asyncContext.response = asyncResponse;

    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));
    Mockito.when(httpServletRequest.startAsync())
           .thenReturn(asyncContext);

    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT 1 + 1",
        ResultFormat.ARRAY,
        false,
        false,
        false,
        Collections.emptyMap(),
        Collections.emptyList()
    );

    Assertions.assertNull(sqlResource.doPost(sqlQuery, httpServletRequest));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), asyncResponse.getStatus());
    Assertions.assertEquals("[[2]]\n", StringUtils.fromUtf8(asyncResponse.baos.toByteArray()));
  }

  @Test
  public void test_doPost_regularUser_forbidden()
  {
    final MockAsyncContext asyncContext = new MockAsyncContext();
    final MockHttpServletResponse asyncResponse = new MockHttpServletResponse();
    asyncContext.response = asyncResponse;

    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));
    Mockito.when(httpServletRequest.startAsync())
           .thenReturn(asyncContext);

    final SqlQuery sqlQuery = new SqlQuery(
        StringUtils.format("SELECT * FROM \"%s\"", CalciteTests.FORBIDDEN_DATASOURCE),
        ResultFormat.ARRAY,
        false,
        false,
        false,
        Collections.emptyMap(),
        Collections.emptyList()
    );

    Assertions.assertThrows(
        ForbiddenException.class,
        () -> sqlResource.doPost(sqlQuery, httpServletRequest)
    );
  }

  @Test
  public void test_doPost_regularUser_runtimeError() throws IOException
  {
    final MockAsyncContext asyncContext = new MockAsyncContext();
    final MockHttpServletResponse asyncResponse = new MockHttpServletResponse();
    asyncContext.response = asyncResponse;

    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));
    Mockito.when(httpServletRequest.startAsync())
           .thenReturn(asyncContext);

    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT U&'\\0000'",
        ResultFormat.ARRAY,
        false,
        false,
        false,
        Collections.emptyMap(),
        Collections.emptyList()
    );

    Assertions.assertNull(sqlResource.doPost(sqlQuery, httpServletRequest));
    Assertions.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), asyncResponse.getStatus());

    final Map<String, Object> e = objectMapper.readValue(
        asyncResponse.baos.toByteArray(),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assertions.assertEquals("InvalidNullByte", e.get("errorCode"));
    Assertions.assertEquals("RUNTIME_FAILURE", e.get("category"));
    assertThat((String) e.get("errorMessage"), CoreMatchers.startsWith("InvalidNullByte: "));
  }

  @Test
  public void test_doPost_regularUser_fullReport() throws Exception
  {
    final MockAsyncContext asyncContext = new MockAsyncContext();
    final MockHttpServletResponse asyncResponse = new MockHttpServletResponse();
    asyncContext.response = asyncResponse;

    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));
    Mockito.when(httpServletRequest.startAsync())
           .thenReturn(asyncContext);

    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT 1 + 1",
        ResultFormat.ARRAY,
        false,
        false,
        false,
        ImmutableMap.of(DartSqlEngine.CTX_FULL_REPORT, true),
        Collections.emptyList()
    );

    Assertions.assertNull(sqlResource.doPost(sqlQuery, httpServletRequest));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), asyncResponse.getStatus());

    final List<List<TaskReport.ReportMap>> reportMaps = objectMapper.readValue(
        asyncResponse.baos.toByteArray(),
        new TypeReference<>() {}
    );

    Assertions.assertEquals(1, reportMaps.size());
    final MSQTaskReport report =
        (MSQTaskReport) Iterables.getOnlyElement(Iterables.getOnlyElement(reportMaps)).get(MSQTaskReport.REPORT_KEY);
    final List<Object[]> results = report.getPayload().getResults().getResults();

    Assertions.assertEquals(1, results.size());
    Assertions.assertArrayEquals(new Object[]{2}, results.get(0));
  }

  @Test
  public void test_doPost_regularUser_runtimeError_fullReport() throws Exception
  {
    final MockAsyncContext asyncContext = new MockAsyncContext();
    final MockHttpServletResponse asyncResponse = new MockHttpServletResponse();
    asyncContext.response = asyncResponse;

    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));
    Mockito.when(httpServletRequest.startAsync())
           .thenReturn(asyncContext);

    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT U&'\\0000'",
        ResultFormat.ARRAY,
        false,
        false,
        false,
        ImmutableMap.of(DartSqlEngine.CTX_FULL_REPORT, true),
        Collections.emptyList()
    );

    Assertions.assertNull(sqlResource.doPost(sqlQuery, httpServletRequest));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), asyncResponse.getStatus());

    final List<List<TaskReport.ReportMap>> reportMaps = objectMapper.readValue(
        asyncResponse.baos.toByteArray(),
        new TypeReference<>() {}
    );

    Assertions.assertEquals(1, reportMaps.size());
    final MSQTaskReport report =
        (MSQTaskReport) Iterables.getOnlyElement(Iterables.getOnlyElement(reportMaps)).get(MSQTaskReport.REPORT_KEY);
    final MSQErrorReport errorReport = report.getPayload().getStatus().getErrorReport();
    Assertions.assertNotNull(errorReport);
    assertThat(errorReport.getFault(), CoreMatchers.instanceOf(InvalidNullByteFault.class));
  }

  @Test
  public void test_doPost_regularUser_thenCancelQuery() throws Exception
  {
    run_test_doPost_regularUser_fullReport_thenCancelQuery(false);
  }

  @Test
  public void test_doPost_regularUser_fullReport_thenCancelQuery() throws Exception
  {
    run_test_doPost_regularUser_fullReport_thenCancelQuery(true);
  }

  /**
   * Helper for {@link #test_doPost_regularUser_thenCancelQuery()} and
   * {@link #test_doPost_regularUser_fullReport_thenCancelQuery()}. We need to do cancellation tests with and
   * without the "fullReport" parameter, because {@link DartQueryMaker} has a separate pathway for each one.
   */
  private void run_test_doPost_regularUser_fullReport_thenCancelQuery(final boolean fullReport) throws Exception
  {
    final MockAsyncContext asyncContext = new MockAsyncContext();
    final MockHttpServletResponse asyncResponse = new MockHttpServletResponse();
    asyncContext.response = asyncResponse;

    // POST SQL query request.
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));
    Mockito.when(httpServletRequest.startAsync())
           .thenReturn(asyncContext);

    // Cancellation request.
    Mockito.when(httpServletRequest2.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));

    // Block up the controllerExecutor so the controller runs long enough to cancel it.
    final Future<?> sleepFuture = controllerExecutor.submit(() -> {
      try {
        Thread.sleep(3_600_000);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    final String sqlQueryId = UUID.randomUUID().toString();
    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT 1 + 1",
        ResultFormat.ARRAY,
        false,
        false,
        false,
        ImmutableMap.of(QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId, DartSqlEngine.CTX_FULL_REPORT, fullReport),
        Collections.emptyList()
    );

    final ExecutorService doPostExec = Execs.singleThreaded("do-post-exec-%s");
    final Future<Response> doPostFuture;
    try {
      // Run doPost in a separate thread. There are now three threads:
      // 1) The controllerExecutor thread, which is blocked up by sleepFuture.
      // 2) The doPostExec thread, which has a doPost in there, blocking on controllerExecutor.
      // 3) The current main test thread, which continues on and which will issue the cancellation request.
      doPostFuture = doPostExec.submit(() -> sqlResource.doPost(sqlQuery, httpServletRequest));
      controllerRegistered.await();

      // Issue cancellation request.
      final Response cancellationResponse = sqlResource.cancelQuery(sqlQueryId, httpServletRequest2);
      Assertions.assertEquals(Response.Status.ACCEPTED.getStatusCode(), cancellationResponse.getStatus());

      // Now that the cancellation request has been accepted, we can cancel the sleepFuture and allow the
      // controller to be canceled.
      sleepFuture.cancel(true);
      doPostExec.shutdown();
    }
    catch (Throwable e) {
      doPostExec.shutdownNow();
      throw e;
    }

    if (!doPostExec.awaitTermination(1, TimeUnit.MINUTES)) {
      throw new ISE("doPost timed out");
    }

    // Wait for the SQL POST to come back.
    Assertions.assertNull(doPostFuture.get());
    Assertions.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), asyncResponse.getStatus());

    // Ensure MSQ fault (CanceledFault) is properly translated to a DruidException and then properly serialized.
    final Map<String, Object> e = objectMapper.readValue(
        asyncResponse.baos.toByteArray(),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assertions.assertEquals("Canceled", e.get("errorCode"));
    Assertions.assertEquals("CANCELED", e.get("category"));
    Assertions.assertEquals(
        MSQFaultUtils.generateMessageWithErrorCode(CanceledFault.instance()),
        e.get("errorMessage")
    );
  }

  @Test
  public void test_cancelQuery_regularUser_unknownQuery()
  {
    Mockito.when(httpServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(makeAuthenticationResult(REGULAR_USER_NAME));

    final Response cancellationResponse = sqlResource.cancelQuery("nonexistent", httpServletRequest);
    Assertions.assertEquals(Response.Status.ACCEPTED.getStatusCode(), cancellationResponse.getStatus());
  }

  /**
   * Add a mock {@link ControllerHolder} to {@link #controllerRegistry}, with a query run by the given user.
   * Used by methods that test {@link DartSqlResource#doGetRunningQueries}.
   *
   * @return the mock holder
   */
  private ControllerHolder setUpMockRunningQuery(final String identity)
  {
    final Controller controller = Mockito.mock(Controller.class);
    Mockito.when(controller.queryId()).thenReturn("did_" + identity);

    final AuthenticationResult authenticationResult = makeAuthenticationResult(identity);
    final ControllerHolder holder = new ControllerHolder(
        controller,
        null,
        "sid",
        "SELECT 1",
        "localhost:1001",
        authenticationResult,
        DateTimes.of("2000")
    );

    controllerRegistry.register(holder);
    return holder;
  }

  /**
   * Create an {@link AuthenticationResult} with {@link AuthenticationResult#getAuthenticatedBy()} set to
   * {@link #AUTHENTICATOR_NAME}.
   */
  private static AuthenticationResult makeAuthenticationResult(final String identity)
  {
    return new AuthenticationResult(identity, null, AUTHENTICATOR_NAME, Collections.emptyMap());
  }
}
