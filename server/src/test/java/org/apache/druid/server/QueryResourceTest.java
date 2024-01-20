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

package org.apache.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BadJsonQueryException;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TruncatedResponseContextException;
import org.apache.druid.query.timeboundary.TimeBoundaryResultValue;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.mocks.ExceptionalInputStream;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.scheduling.NoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ThresholdBasedQueryPrioritizationStrategy;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.http.HttpStatus;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class QueryResourceTest
{
  private static final QueryToolChestWarehouse WAREHOUSE = new MapQueryToolChestWarehouse(ImmutableMap.of());
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("druid", "druid", null, null);

  private final MockHttpServletRequest testServletRequest = new MockHttpServletRequest();

  private static final QuerySegmentWalker TEST_SEGMENT_WALKER = new QuerySegmentWalker()
  {
    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
    {
      return (queryPlus, responseContext) -> Sequences.empty();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
    {
      return getQueryRunnerForIntervals(null, null);
    }
  };

  private static final String SIMPLE_TIMESERIES_QUERY =
      "{\n"
      + "    \"queryType\": \"timeseries\",\n"
      + "    \"dataSource\": \"mmx_metrics\",\n"
      + "    \"granularity\": \"hour\",\n"
      + "    \"intervals\": [\n"
      + "      \"2014-12-17/2015-12-30\"\n"
      + "    ],\n"
      + "    \"aggregations\": [\n"
      + "      {\n"
      + "        \"type\": \"count\",\n"
      + "        \"name\": \"rows\"\n"
      + "      }\n"
      + "    ]\n"
      + "}";

  private static final String SIMPLE_TIMESERIES_QUERY_SMALLISH_INTERVAL =
      "{\n"
      + "    \"queryType\": \"timeseries\",\n"
      + "    \"dataSource\": \"mmx_metrics\",\n"
      + "    \"granularity\": \"hour\",\n"
      + "    \"intervals\": [\n"
      + "      \"2014-12-17/2014-12-30\"\n"
      + "    ],\n"
      + "    \"aggregations\": [\n"
      + "      {\n"
      + "        \"type\": \"count\",\n"
      + "        \"name\": \"rows\"\n"
      + "      }\n"
      + "    ]\n"
      + "}";

  private static final String SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY =
      "{\n"
      + "    \"queryType\": \"timeseries\",\n"
      + "    \"dataSource\": \"mmx_metrics\",\n"
      + "    \"granularity\": \"hour\",\n"
      + "    \"intervals\": [\n"
      + "      \"2014-12-17/2015-12-30\"\n"
      + "    ],\n"
      + "    \"aggregations\": [\n"
      + "      {\n"
      + "        \"type\": \"count\",\n"
      + "        \"name\": \"rows\"\n"
      + "      }\n"
      + "    ],\n"
      + "    \"context\": { \"priority\": -1 }"
      + "}";


  private static final ServiceEmitter NOOP_SERVICE_EMITTER = new NoopServiceEmitter();
  private static final DruidNode DRUID_NODE = new DruidNode(
      "broker",
      "localhost",
      true,
      8082,
      null,
      true,
      false
  );

  private ObjectMapper jsonMapper;
  private ObjectMapper smileMapper;
  private QueryResource queryResource;
  private QueryScheduler queryScheduler;
  private TestRequestLogger testRequestLogger;

  @BeforeClass
  public static void staticSetup()
  {
    EmittingLogger.registerEmitter(NOOP_SERVICE_EMITTER);
  }

  @Before
  public void setup()
  {
    Injector injector = GuiceInjectors.makeStartupInjector();
    jsonMapper = injector.getInstance(ObjectMapper.class);
    smileMapper = injector.getInstance(Key.get(ObjectMapper.class, Smile.class));

    testServletRequest.contentType = MediaType.APPLICATION_JSON;
    testServletRequest.headers.put("Accept", MediaType.APPLICATION_JSON);
    testServletRequest.remoteAddr = "localhost";

    queryScheduler = QueryStackTests.DEFAULT_NOOP_SCHEDULER;
    testRequestLogger = new TestRequestLogger();
    queryResource = createQueryResource(ResponseContextConfig.newConfig(true));
  }

  private QueryResource createQueryResource(ResponseContextConfig responseContextConfig)
  {
    return new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
        ),
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        null,
        responseContextConfig,
        DRUID_NODE
    );
  }

  @Test
  public void testGoodQuery() throws IOException
  {
    expectPermissiveHappyPathAuth();

    Assert.assertEquals(200, expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY).getStatus());
  }

  @Test
  public void testGoodQueryWithQueryConfigOverrideDefault() throws IOException
  {
    String overrideConfigKey = "priority";
    String overrideConfigValue = "678";
    DefaultQueryConfig overrideConfig = new DefaultQueryConfig(ImmutableMap.of(overrideConfigKey, overrideConfigValue));
    queryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            Suppliers.ofInstance(overrideConfig)
        ),
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    expectPermissiveHappyPathAuth();

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    final List<Result<TimeBoundaryResultValue>> responses = jsonMapper.readValue(
        response.baos.toByteArray(),
        new TypeReference<List<Result<TimeBoundaryResultValue>>>()
        {
        }
    );

    Assert.assertEquals(0, responses.size());
    Assert.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assert.assertTrue(testRequestLogger.getNativeQuerylogs()
                                       .get(0)
                                       .getQuery()
                                       .getContext()
                                       .containsKey(overrideConfigKey));
    Assert.assertEquals(
        overrideConfigValue,
        testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey)
    );
  }

  @Test
  public void testGoodQueryThrowsDruidExceptionFromLifecycleExecute() throws IOException
  {
    String overrideConfigKey = "priority";
    String overrideConfigValue = "678";
    DefaultQueryConfig overrideConfig = new DefaultQueryConfig(ImmutableMap.of(overrideConfigKey, overrideConfigValue));
    queryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            new QuerySegmentWalker()
            {
              @Override
              public <T> QueryRunner<T> getQueryRunnerForIntervals(
                  Query<T> query,
                  Iterable<Interval> intervals
              )
              {
                throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                                    .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                    .build("failing for coverage!");
              }

              @Override
              public <T> QueryRunner<T> getQueryRunnerForSegments(
                  Query<T> query,
                  Iterable<SegmentDescriptor> specs
              )
              {
                throw new UnsupportedOperationException();
              }
            },
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            Suppliers.ofInstance(overrideConfig)
        ),
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    expectPermissiveHappyPathAuth();

    final Response response = expectSynchronousRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

    final ErrorResponse entity = (ErrorResponse) response.getEntity();
    MatcherAssert.assertThat(
        entity.getUnderlyingException(),
        new DruidExceptionMatcher(DruidException.Persona.OPERATOR, DruidException.Category.RUNTIME_FAILURE, "general")
            .expectMessageIs("failing for coverage!")
    );

    Assert.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assert.assertTrue(testRequestLogger.getNativeQuerylogs()
                                       .get(0)
                                       .getQuery()
                                       .getContext()
                                       .containsKey(overrideConfigKey));
    Assert.assertEquals(
        overrideConfigValue,
        testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey)
    );
  }


  @Test
  public void testQueryThrowsRuntimeExceptionFromLifecycleExecute() throws IOException
  {
    String embeddedExceptionMessage = "Embedded Exception Message!";
    String overrideConfigKey = "priority";
    String overrideConfigValue = "678";

    DefaultQueryConfig overrideConfig = new DefaultQueryConfig(ImmutableMap.of(overrideConfigKey, overrideConfigValue));
    QuerySegmentWalker querySegmentWalker = new QuerySegmentWalker()
    {
      @Override
      public <T> QueryRunner<T> getQueryRunnerForIntervals(
          Query<T> query,
          Iterable<Interval> intervals
      )
      {
        throw new RuntimeException("something", new RuntimeException(embeddedExceptionMessage));
      }

      @Override
      public <T> QueryRunner<T> getQueryRunnerForSegments(
          Query<T> query,
          Iterable<SegmentDescriptor> specs
      )
      {
        throw new UnsupportedOperationException();
      }
    };

    queryResource = new QueryResource(

        new QueryLifecycleFactory(null, null, null, null, null, null, null, Suppliers.ofInstance(overrideConfig))
        {
          @Override
          public QueryLifecycle factorize()
          {
            return new QueryLifecycle(
                WAREHOUSE,
                querySegmentWalker,
                new DefaultGenericQueryMetricsFactory(),
                new NoopServiceEmitter(),
                testRequestLogger,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                overrideConfig,
                new AuthConfig(),
                System.currentTimeMillis(),
                System.nanoTime())
            {
              @Override
              public void emitLogsAndMetrics(@Nullable Throwable e, @Nullable String remoteAddress, long bytesWritten)
              {
                Assert.assertTrue(Throwables.getStackTraceAsString(e).contains(embeddedExceptionMessage));
              }
            };
          }
        },
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    expectPermissiveHappyPathAuth();

    final Response response = expectSynchronousRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

    final ErrorResponse entity = (ErrorResponse) response.getEntity();
    MatcherAssert.assertThat(
        entity.getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.OPERATOR,
            DruidException.Category.RUNTIME_FAILURE, "legacyQueryException")
            .expectMessageIs("something")
    );
  }

  @Test
  public void testGoodQueryWithQueryConfigDoesNotOverrideQueryContext() throws IOException
  {
    String overrideConfigKey = "priority";
    String overrideConfigValue = "678";
    DefaultQueryConfig overrideConfig = new DefaultQueryConfig(ImmutableMap.of(overrideConfigKey, overrideConfigValue));
    queryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            Suppliers.ofInstance(overrideConfig)
        ),
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    expectPermissiveHappyPathAuth();

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY);

    final List<Result<TimeBoundaryResultValue>> responses = jsonMapper.readValue(
        response.baos.toByteArray(),
        new TypeReference<List<Result<TimeBoundaryResultValue>>>()
        {
        }
    );

    Assert.assertNotNull(response);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(0, responses.size());
    Assert.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assert.assertTrue(testRequestLogger.getNativeQuerylogs()
                                       .get(0)
                                       .getQuery()
                                       .getContext()
                                       .containsKey(overrideConfigKey));
    Assert.assertEquals(
        -1,
        testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey)
    );
  }

  @Test
  public void testTruncatedResponseContextShouldFail() throws IOException
  {
    expectPermissiveHappyPathAuth();

    final QueryResource queryResource = createQueryResource(ResponseContextConfig.forTest(true, 0));

    MockHttpServletResponse response = expectAsyncRequestFlow(
        testServletRequest,
        SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8),
        queryResource
    );
    Assert.assertEquals(1, queryResource.getInterruptedQueryCount());
    Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatus());
    final String expectedException = new QueryInterruptedException(
        new TruncatedResponseContextException("Serialized response context exceeds the max size[0]"),
        DRUID_NODE.getHostAndPortToUse()
    ).toString();
    Assert.assertEquals(
        expectedException,
        jsonMapper.readValue(response.baos.toByteArray(), QueryInterruptedException.class).toString()
    );
  }

  @Test
  public void testTruncatedResponseContextShouldSucceed() throws IOException
  {
    expectPermissiveHappyPathAuth();
    final QueryResource queryResource = createQueryResource(ResponseContextConfig.forTest(false, 0));

    final MockHttpServletResponse response = expectAsyncRequestFlow(
        testServletRequest,
        SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8),
        queryResource
    );
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());
  }

  @Test
  public void testGoodQueryWithNullAcceptHeader() throws IOException
  {
    testServletRequest.headers.remove("Accept");
    expectPermissiveHappyPathAuth();

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());
    //since accept header is null, the response content type should be same as the value of 'Content-Type' header
    Assert.assertEquals(MediaType.APPLICATION_JSON, response.getContentType());
  }

  @Test
  public void testGoodQueryWithEmptyAcceptHeader() throws IOException
  {
    expectPermissiveHappyPathAuth();
    testServletRequest.headers.put("Accept", "");

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);

    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());
    //since accept header is empty, the response content type should be same as the value of 'Content-Type' header
    Assert.assertEquals(MediaType.APPLICATION_JSON, response.getContentType());
  }

  @Test
  public void testGoodQueryWithJsonRequestAndSmileAcceptHeader() throws IOException
  {
    expectPermissiveHappyPathAuth();

    // Set Accept to Smile
    testServletRequest.headers.put("Accept", SmileMediaTypes.APPLICATION_JACKSON_SMILE);

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());

    // Content-Type in response should be Smile
    Assert.assertEquals(SmileMediaTypes.APPLICATION_JACKSON_SMILE, response.getContentType());
  }

  @Test
  public void testGoodQueryWithSmileRequestAndSmileAcceptHeader() throws IOException
  {
    testServletRequest.contentType = SmileMediaTypes.APPLICATION_JACKSON_SMILE;
    expectPermissiveHappyPathAuth();

    // Set Accept to Smile
    testServletRequest.headers.put("Accept", SmileMediaTypes.APPLICATION_JACKSON_SMILE);

    final MockHttpServletResponse response = expectAsyncRequestFlow(
        testServletRequest,
        smileMapper.writeValueAsBytes(jsonMapper.readTree(
            SIMPLE_TIMESERIES_QUERY))
    );
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());

    // Content-Type in response should be Smile
    Assert.assertEquals(SmileMediaTypes.APPLICATION_JACKSON_SMILE, response.getContentType());
  }

  @Test
  public void testGoodQueryWithSmileRequestNoSmileAcceptHeader() throws IOException
  {
    testServletRequest.contentType = SmileMediaTypes.APPLICATION_JACKSON_SMILE;
    expectPermissiveHappyPathAuth();

    // DO NOT set Accept to Smile, Content-Type in response will be default to Content-Type in request
    testServletRequest.headers.remove("Accept");

    final MockHttpServletResponse response = expectAsyncRequestFlow(
        testServletRequest,
        smileMapper.writeValueAsBytes(jsonMapper.readTree(SIMPLE_TIMESERIES_QUERY))
    );
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());

    // Content-Type in response should default to Content-Type from request
    Assert.assertEquals(SmileMediaTypes.APPLICATION_JACKSON_SMILE, response.getContentType());
  }

  @Test
  public void testBadQuery() throws IOException
  {
    Response response = queryResource.doPost(
        new ByteArrayInputStream("Meka Leka Hi Meka Hiney Ho".getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    QueryException e = jsonMapper.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(QueryException.JSON_PARSE_ERROR_CODE, e.getErrorCode());
    Assert.assertEquals(BadJsonQueryException.ERROR_CLASS, e.getErrorClass());
  }

  @Test
  public void testResourceLimitExceeded() throws IOException
  {
    Response response = queryResource.doPost(
        new ExceptionalInputStream(() -> new ResourceLimitExceededException("You require too much of something")),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    QueryException e = jsonMapper.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(QueryException.RESOURCE_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    Assert.assertEquals(ResourceLimitExceededException.class.getName(), e.getErrorClass());
  }

  @Test
  public void testUnsupportedQueryThrowsException() throws IOException
  {
    String errorMessage = "This will be support in Druid 9999";
    Response response = queryResource.doPost(
        new ExceptionalInputStream(() -> new QueryUnsupportedException(errorMessage)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(QueryUnsupportedException.STATUS_CODE, response.getStatus());
    QueryException ex = jsonMapper.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(errorMessage, ex.getMessage());
    Assert.assertEquals(QueryException.QUERY_UNSUPPORTED_ERROR_CODE, ex.getErrorCode());
  }

  @Test
  public void testSecuredQuery() throws Exception
  {
    expectPermissiveHappyPathAuth();

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            if (resource.getName().equals("allow")) {
              return new Access(true);
            } else {
              return new Access(false);
            }
          }

        };
      }
    };

    queryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            authMapper,
            Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
        ),
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        authMapper,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );


    try {
      queryResource.doPost(
          new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
          null /*pretty*/,
          testServletRequest.mimic()
      );
      Assert.fail("doPost did not throw ForbiddenException for an unauthorized query");
    }
    catch (ForbiddenException e) {
    }

    final MockHttpServletResponse response = expectAsyncRequestFlow(
        "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\"}",
        testServletRequest.mimic()
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    final List<Result<TimeBoundaryResultValue>> responses = jsonMapper.readValue(
        response.baos.toByteArray(),
        new TypeReference<List<Result<TimeBoundaryResultValue>>>()
        {
        }
    );

    Assert.assertEquals(0, responses.size());
    Assert.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assert.assertEquals(
        true,
        testRequestLogger.getNativeQuerylogs().get(0).getQueryStats().getStats().get("success")
    );
    Assert.assertEquals(
        "druid",
        testRequestLogger.getNativeQuerylogs().get(0).getQueryStats().getStats().get("identity")
    );
  }

  @Test
  public void testQueryTimeoutException() throws Exception
  {
    final QuerySegmentWalker timeoutSegmentWalker = new QuerySegmentWalker()
    {
      @Override
      public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
      {
        throw new QueryTimeoutException();
      }

      @Override
      public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
      {
        return getQueryRunnerForIntervals(null, null);
      }
    };

    final QueryResource timeoutQueryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            timeoutSegmentWalker,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
        ),
        jsonMapper,
        jsonMapper,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );
    expectPermissiveHappyPathAuth();

    final Response response = expectSynchronousRequestFlow(
        testServletRequest,
        SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8),
        timeoutQueryResource
    );
    Assert.assertEquals(QueryTimeoutException.STATUS_CODE, response.getStatus());

    ErrorResponse entity = (ErrorResponse) response.getEntity();
    MatcherAssert.assertThat(
        entity.getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.OPERATOR,
            DruidException.Category.TIMEOUT,
            "legacyQueryException"
        )
            .expectMessageIs(
                "Query did not complete within configured timeout period. You can increase query timeout or tune the performance of query.")
    );

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    jsonMapper.writeValue(baos, entity);
    QueryTimeoutException ex = jsonMapper.readValue(baos.toByteArray(), QueryTimeoutException.class);
    Assert.assertEquals("Query did not complete within configured timeout period. You can " +
                        "increase query timeout or tune the performance of query.", ex.getMessage());
    Assert.assertEquals(QueryException.QUERY_TIMEOUT_ERROR_CODE, ex.getErrorCode());
    Assert.assertEquals(1, timeoutQueryResource.getTimedOutQueryCount());

  }

  @Test(timeout = 60_000L)
  public void testSecuredCancelQuery() throws Exception
  {
    final CountDownLatch waitForCancellationLatch = new CountDownLatch(1);
    final CountDownLatch waitFinishLatch = new CountDownLatch(2);
    final CountDownLatch startAwaitLatch = new CountDownLatch(1);
    final CountDownLatch cancelledCountDownLatch = new CountDownLatch(1);

    expectPermissiveHappyPathAuth();

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            // READ action corresponds to the query
            // WRITE corresponds to cancellation of query
            if (action.equals(Action.READ)) {
              try {
                // Countdown startAwaitLatch as we want query cancellation to happen
                // after we enter isAuthorized method so that we can handle the
                // InterruptedException here because of query cancellation
                startAwaitLatch.countDown();
                waitForCancellationLatch.await();
              }
              catch (InterruptedException e) {
                // When the query is cancelled the control will reach here,
                // countdown the latch and rethrow the exception so that error response is returned for the query
                cancelledCountDownLatch.countDown();
                throw new QueryInterruptedException(e);
              }
              return new Access(true);
            } else {
              return new Access(true);
            }
          }

        };
      }
    };

    queryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            authMapper,
            Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
        ),
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        authMapper,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    final String queryString = "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\","
                               + "\"context\":{\"queryId\":\"id_1\"}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    Query<?> query = mapper.readValue(queryString, Query.class);

    AtomicReference<Response> responseFromEndpoint = new AtomicReference<>();

    // We expect this future to get canceled so we have to grab the exception somewhere else.
    ListenableFuture<Response> future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_resource_%s")
    ).submit(
        () -> {
          try {
            responseFromEndpoint.set(queryResource.doPost(
                new ByteArrayInputStream(queryString.getBytes(StandardCharsets.UTF_8)),
                null,
                testServletRequest
            ));
            return null;
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          finally {
            waitFinishLatch.countDown();
          }
        }
    );

    queryScheduler.registerQueryFuture(query, future);
    startAwaitLatch.await();

    Executors.newSingleThreadExecutor().submit(
        () -> {
          Response response = queryResource.cancelQuery("id_1", testServletRequest);
          Assert.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());
          waitForCancellationLatch.countDown();
          waitFinishLatch.countDown();
        }
    );
    waitFinishLatch.await();
    cancelledCountDownLatch.await();

    Assert.assertTrue(future.isCancelled());
    final Response response = responseFromEndpoint.get();
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @Test(timeout = 60_000L)
  public void testDenySecuredCancelQuery() throws Exception
  {
    final CountDownLatch waitForCancellationLatch = new CountDownLatch(1);
    final CountDownLatch waitFinishLatch = new CountDownLatch(2);
    final CountDownLatch startAwaitLatch = new CountDownLatch(1);

    expectPermissiveHappyPathAuth();

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            // READ action corresponds to the query
            // WRITE corresponds to cancellation of query
            if (action.equals(Action.READ)) {
              try {
                waitForCancellationLatch.await();
              }
              catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              return new Access(true);
            } else {
              // Deny access to cancel the query
              return new Access(false);
            }
          }

        };
      }
    };

    queryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            authMapper,
            Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
        ),
        jsonMapper,
        smileMapper,
        queryScheduler,
        new AuthConfig(),
        authMapper,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    final String queryString = "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\","
                               + "\"context\":{\"queryId\":\"id_1\"}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    Query<?> query = mapper.readValue(queryString, Query.class);

    ListenableFuture<HttpServletResponse> future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_resource_%s")
    ).submit(
        () -> {
          try {
            startAwaitLatch.countDown();
            final MockHttpServletRequest localRequest = testServletRequest.mimic();
            final MockHttpServletResponse retVal = MockHttpServletResponse.forRequest(localRequest);
            queryResource.doPost(
                new ByteArrayInputStream(queryString.getBytes(StandardCharsets.UTF_8)),
                null,
                localRequest
            );
            return retVal;
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          finally {
            waitFinishLatch.countDown();
          }
        }
    );

    queryScheduler.registerQueryFuture(query, future);
    startAwaitLatch.await();

    Executors.newSingleThreadExecutor().submit(
        () -> {
          try {
            queryResource.cancelQuery("id_1", testServletRequest.mimic());
          }
          catch (ForbiddenException e) {
            waitForCancellationLatch.countDown();
            waitFinishLatch.countDown();
          }
        }
    );
    waitFinishLatch.await();

    Assert.assertEquals(Response.Status.OK.getStatusCode(), future.get().getStatus());
  }

  @Test(timeout = 10_000L)
  public void testTooManyQuery() throws InterruptedException, ExecutionException
  {
    expectPermissiveHappyPathAuth();

    final CountDownLatch waitTwoScheduled = new CountDownLatch(2);
    final QueryScheduler laningScheduler = new QueryScheduler(
        2,
        ManualQueryPrioritizationStrategy.INSTANCE,
        NoQueryLaningStrategy.INSTANCE,
        // enable total laning
        new ServerConfig(false)
    );

    ArrayList<Future<Boolean>> back2 = new ArrayList<>();

    createScheduledQueryResource(laningScheduler, Collections.emptyList(), ImmutableList.of(waitTwoScheduled));
    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));
    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus())
    ));
    waitTwoScheduled.await();
    back2.add(eventuallyaAssertSynchronousResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> {
          Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;

          final ErrorResponse entity = (ErrorResponse) response.getEntity();
          MatcherAssert.assertThat(
              entity.getUnderlyingException(),
              new DruidExceptionMatcher(
                  DruidException.Persona.OPERATOR,
                  DruidException.Category.CAPACITY_EXCEEDED,
                  "legacyQueryException"
              )
                  .expectMessageIs(
                      "Too many concurrent queries, total query capacity of 2 exceeded. Please try your query again later.")
          );

          try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            jsonMapper.writeValue(baos, entity);

            // Here we are converting to a QueryCapacityExceededException.  This is just to validate legacy stuff.
            // When we delete the QueryException class, we can just rely on validating the DruidException instead
            ex = jsonMapper.readValue(baos.toByteArray(), QueryCapacityExceededException.class);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          Assert.assertEquals(QueryCapacityExceededException.makeTotalErrorMessage(2), ex.getMessage());
          Assert.assertEquals(QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE, ex.getErrorCode());
        }
    ));

    for (Future<Boolean> theFuture : back2) {
      Assert.assertTrue(theFuture.get());
    }
  }

  @Test(timeout = 10_000L)
  public void testTooManyQueryInLane() throws InterruptedException, ExecutionException
  {
    expectPermissiveHappyPathAuth();
    final CountDownLatch waitTwoStarted = new CountDownLatch(2);
    final CountDownLatch waitOneScheduled = new CountDownLatch(1);
    final QueryScheduler scheduler = new QueryScheduler(
        40,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new HiLoQueryLaningStrategy(2),
        new ServerConfig()
    );

    ArrayList<Future<Boolean>> back2 = new ArrayList<>();

    createScheduledQueryResource(scheduler, ImmutableList.of(waitTwoStarted), ImmutableList.of(waitOneScheduled));

    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));
    waitOneScheduled.await();
    back2.add(eventuallyaAssertSynchronousResponse(
        SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY,
        response -> {
          Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;

          final ErrorResponse entity = (ErrorResponse) response.getEntity();
          MatcherAssert.assertThat(
              entity.getUnderlyingException(),
              new DruidExceptionMatcher(
                  DruidException.Persona.OPERATOR,
                  DruidException.Category.CAPACITY_EXCEEDED,
                  "legacyQueryException"
              )
                  .expectMessageIs(
                      "Too many concurrent queries for lane 'low', query capacity of 1 exceeded. Please try your query again later.")
          );

          try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            jsonMapper.writeValue(baos, entity);

            // Here we are converting to a QueryCapacityExceededException.  This is just to validate legacy stuff.
            // When we delete the QueryException class, we can just rely on validating the DruidException instead
            ex = jsonMapper.readValue(baos.toByteArray(), QueryCapacityExceededException.class);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          Assert.assertEquals(
              QueryCapacityExceededException.makeLaneErrorMessage(HiLoQueryLaningStrategy.LOW, 1),
              ex.getMessage()
          );
          Assert.assertEquals(QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE, ex.getErrorCode());

        }
    ));
    waitTwoStarted.await();
    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));

    for (Future<Boolean> theFuture : back2) {
      Assert.assertTrue(theFuture.get());
    }
  }

  @Test(timeout = 10_000L)
  public void testTooManyQueryInLaneImplicitFromDurationThreshold() throws InterruptedException, ExecutionException
  {
    expectPermissiveHappyPathAuth();
    final CountDownLatch waitTwoStarted = new CountDownLatch(2);
    final CountDownLatch waitOneScheduled = new CountDownLatch(1);
    final QueryScheduler scheduler = new QueryScheduler(
        40,
        new ThresholdBasedQueryPrioritizationStrategy(null, "P90D", null, null),
        new HiLoQueryLaningStrategy(1),
        new ServerConfig()
    );

    ArrayList<Future<Boolean>> back2 = new ArrayList<>();
    createScheduledQueryResource(scheduler, ImmutableList.of(waitTwoStarted), ImmutableList.of(waitOneScheduled));

    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));
    waitOneScheduled.await();
    back2.add(eventuallyaAssertSynchronousResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> {
          Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;

          final ErrorResponse entity = (ErrorResponse) response.getEntity();
          MatcherAssert.assertThat(
              entity.getUnderlyingException(),
              new DruidExceptionMatcher(
                  DruidException.Persona.OPERATOR,
                  DruidException.Category.CAPACITY_EXCEEDED,
                  "legacyQueryException"
              )
                  .expectMessageIs(
                      "Too many concurrent queries for lane 'low', query capacity of 1 exceeded. Please try your query again later.")
          );

          try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            jsonMapper.writeValue(baos, entity);

            // Here we are converting to a QueryCapacityExceededException.  This is just to validate legacy stuff.
            // When we delete the QueryException class, we can just rely on validating the DruidException instead
            ex = jsonMapper.readValue(baos.toByteArray(), QueryCapacityExceededException.class);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          Assert.assertEquals(
              QueryCapacityExceededException.makeLaneErrorMessage(HiLoQueryLaningStrategy.LOW, 1),
              ex.getMessage()
          );
          Assert.assertEquals(QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE, ex.getErrorCode());
        }
    ));
    waitTwoStarted.await();
    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY_SMALLISH_INTERVAL,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));

    for (Future<Boolean> theFuture : back2) {
      Assert.assertTrue(theFuture.get());
    }
  }

  private void createScheduledQueryResource(
      QueryScheduler scheduler,
      Collection<CountDownLatch> beforeScheduler,
      Collection<CountDownLatch> inScheduler
  )
  {

    QuerySegmentWalker texasRanger = new QuerySegmentWalker()
    {
      @Override
      public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
      {
        return (queryPlus, responseContext) -> {
          beforeScheduler.forEach(CountDownLatch::countDown);

          return Sequences.simple(
              scheduler.run(
                  scheduler.prioritizeAndLaneQuery(queryPlus, ImmutableSet.of()),
                  new LazySequence<T>(() -> {
                    inScheduler.forEach(CountDownLatch::countDown);
                    try {
                      // pretend to be a query that is waiting on results
                      Thread.sleep(500);
                    }
                    catch (InterruptedException ignored) {
                    }
                    // all that waiting for nothing :(
                    return Sequences.empty();
                  })
              ).toList()
          );
        };
      }

      @Override
      public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
      {
        return getQueryRunnerForIntervals(null, null);
      }
    };

    queryResource = new QueryResource(
        new QueryLifecycleFactory(
            WAREHOUSE,
            texasRanger,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
        ),
        jsonMapper,
        smileMapper,
        scheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );
  }

  private Future<Boolean> eventuallyAssertAsyncResponse(
      String query,
      Consumer<MockHttpServletResponse> asserts
  )
  {
    return Executors.newSingleThreadExecutor().submit(() -> {
      try {
        asserts.accept(expectAsyncRequestFlow(query, testServletRequest.mimic()));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      return true;
    });
  }

  private void expectPermissiveHappyPathAuth()
  {
    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, AUTHENTICATION_RESULT);
  }

  @Nonnull
  private MockHttpServletResponse expectAsyncRequestFlow(String simpleTimeseriesQuery) throws IOException
  {
    return expectAsyncRequestFlow(
        simpleTimeseriesQuery,
        testServletRequest
    );
  }

  @Nonnull
  private MockHttpServletResponse expectAsyncRequestFlow(String query, MockHttpServletRequest req) throws IOException
  {
    return expectAsyncRequestFlow(req, query.getBytes(StandardCharsets.UTF_8));
  }

  @Nonnull
  private MockHttpServletResponse expectAsyncRequestFlow(
      MockHttpServletRequest req,
      byte[] queryBytes
  ) throws IOException
  {
    return expectAsyncRequestFlow(req, queryBytes, queryResource);
  }

  @Nonnull
  private MockHttpServletResponse expectAsyncRequestFlow(
      MockHttpServletRequest req,
      byte[] queryBytes,
      QueryResource queryResource
  ) throws IOException
  {
    final MockHttpServletResponse response = MockHttpServletResponse.forRequest(req);

    Assert.assertNull(queryResource.doPost(
        new ByteArrayInputStream(queryBytes),
        null /*pretty*/,
        req
    ));
    return response;
  }

  private Future<Boolean> eventuallyaAssertSynchronousResponse(
      String query,
      Consumer<Response> asserts
  )
  {
    return Executors.newSingleThreadExecutor().submit(() -> {
      try {
        asserts.accept(
            expectSynchronousRequestFlow(
                testServletRequest.mimic(),
                query.getBytes(StandardCharsets.UTF_8),
                queryResource
            )
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      return true;
    });
  }

  private Response expectSynchronousRequestFlow(String simpleTimeseriesQuery) throws IOException
  {
    return expectSynchronousRequestFlow(
        testServletRequest,
        simpleTimeseriesQuery.getBytes(StandardCharsets.UTF_8),
        queryResource
    );
  }

  private Response expectSynchronousRequestFlow(
      MockHttpServletRequest req,
      byte[] bytes,
      QueryResource queryResource
  ) throws IOException
  {
    return queryResource.doPost(new ByteArrayInputStream(bytes), null, req);
  }
}
