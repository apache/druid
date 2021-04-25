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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class QueryResourceTest
{
  private static final QueryToolChestWarehouse WAREHOUSE = new MapQueryToolChestWarehouse(ImmutableMap.of());
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("druid", "druid", null, null);

  private final HttpServletRequest testServletRequest = EasyMock.createMock(HttpServletRequest.class);

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
    EasyMock.expect(testServletRequest.getContentType()).andReturn(MediaType.APPLICATION_JSON).anyTimes();
    EasyMock.expect(testServletRequest.getHeader("Accept")).andReturn(MediaType.APPLICATION_JSON).anyTimes();
    EasyMock.expect(testServletRequest.getHeader(QueryResource.HEADER_IF_NONE_MATCH)).andReturn(null).anyTimes();
    EasyMock.expect(testServletRequest.getRemoteAddr()).andReturn("localhost").anyTimes();
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
        JSON_MAPPER,
        JSON_MAPPER,
        queryScheduler,
        new AuthConfig(),
        null,
        responseContextConfig,
        DRUID_NODE
    );
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(testServletRequest);
  }

  @Test
  public void testGoodQuery() throws IOException
  {
    expectPermissiveHappyPathAuth();

    Response response = queryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
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
        JSON_MAPPER,
        JSON_MAPPER,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    expectPermissiveHappyPathAuth();

    Response response = queryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ((StreamingOutput) response.getEntity()).write(baos);
    final List<Result<TimeBoundaryResultValue>> responses = JSON_MAPPER.readValue(
        baos.toByteArray(),
        new TypeReference<List<Result<TimeBoundaryResultValue>>>() {}
    );

    Assert.assertNotNull(response);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(0, responses.size());
    Assert.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assert.assertTrue(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().containsKey(overrideConfigKey));
    Assert.assertEquals(overrideConfigValue, testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey));
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
        JSON_MAPPER,
        JSON_MAPPER,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    expectPermissiveHappyPathAuth();

    Response response = queryResource.doPost(
        // SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY context has overrideConfigKey with value of -1
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ((StreamingOutput) response.getEntity()).write(baos);
    final List<Result<TimeBoundaryResultValue>> responses = JSON_MAPPER.readValue(
        baos.toByteArray(),
        new TypeReference<List<Result<TimeBoundaryResultValue>>>() {}
    );

    Assert.assertNotNull(response);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(0, responses.size());
    Assert.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assert.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assert.assertTrue(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().containsKey(overrideConfigKey));
    Assert.assertEquals(-1, testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey));
  }

  @Test
  public void testTruncatedResponseContextShouldFail() throws IOException
  {
    expectPermissiveHappyPathAuth();
    final QueryResource queryResource = createQueryResource(ResponseContextConfig.forTest(true, 0));

    Response response = queryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertEquals(1, queryResource.getInterruptedQueryCount());
    Assert.assertNotNull(response);
    Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatus());
    final String expectedException = new QueryInterruptedException(
        new TruncatedResponseContextException("Serialized response context exceeds the max size[0]"),
        DRUID_NODE.getHostAndPortToUse()
    ).toString();
    Assert.assertEquals(
        expectedException,
        JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryInterruptedException.class).toString()
    );
  }

  @Test
  public void testTruncatedResponseContextShouldSucceed() throws IOException
  {
    expectPermissiveHappyPathAuth();
    final QueryResource queryResource = createQueryResource(ResponseContextConfig.forTest(false, 0));

    Response response = queryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());
  }

  @Test
  public void testGoodQueryWithNullAcceptHeader() throws IOException
  {
    final String acceptHeader = null;
    final String contentTypeHeader = MediaType.APPLICATION_JSON;
    EasyMock.reset(testServletRequest);

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
        .andReturn(null)
        .anyTimes();
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(AUTHENTICATION_RESULT)
        .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    EasyMock.expect(testServletRequest.getHeader("Accept")).andReturn(acceptHeader).anyTimes();
    EasyMock.expect(testServletRequest.getContentType()).andReturn(contentTypeHeader).anyTimes();
    EasyMock.expect(testServletRequest.getHeader(QueryResource.HEADER_IF_NONE_MATCH)).andReturn(null).anyTimes();
    EasyMock.expect(testServletRequest.getRemoteAddr()).andReturn("localhost").anyTimes();

    EasyMock.replay(testServletRequest);
    Response response = queryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());
    //since accept header is null, the response content type should be same as the value of 'Content-Type' header
    Assert.assertEquals(contentTypeHeader, (response.getMetadata().get("Content-Type").get(0)).toString());
    Assert.assertNotNull(response);
  }

  @Test
  public void testGoodQueryWithEmptyAcceptHeader() throws IOException
  {
    final String acceptHeader = "";
    final String contentTypeHeader = MediaType.APPLICATION_JSON;
    EasyMock.reset(testServletRequest);

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
        .andReturn(null)
        .anyTimes();
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(AUTHENTICATION_RESULT)
        .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    EasyMock.expect(testServletRequest.getHeader("Accept")).andReturn(acceptHeader).anyTimes();
    EasyMock.expect(testServletRequest.getContentType()).andReturn(contentTypeHeader).anyTimes();
    EasyMock.expect(testServletRequest.getHeader(QueryResource.HEADER_IF_NONE_MATCH)).andReturn(null).anyTimes();
    EasyMock.expect(testServletRequest.getRemoteAddr()).andReturn("localhost").anyTimes();

    EasyMock.replay(testServletRequest);
    Response response = queryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());
    //since accept header is empty, the response content type should be same as the value of 'Content-Type' header
    Assert.assertEquals(contentTypeHeader, (response.getMetadata().get("Content-Type").get(0)).toString());
    Assert.assertNotNull(response);
  }

  @Test
  public void testGoodQueryWithSmileAcceptHeader() throws IOException
  {
    //Doing a replay of testServletRequest for teardown to succeed.
    //We dont use testServletRequest in this testcase
    EasyMock.replay(testServletRequest);

    //Creating our own Smile Servlet request, as to not disturb the remaining tests.
    // else refactoring required for this class. i know this kinda makes the class somewhat Dirty.
    final HttpServletRequest smileRequest = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(smileRequest.getContentType()).andReturn(MediaType.APPLICATION_JSON).anyTimes();

    EasyMock.expect(smileRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
        .andReturn(null)
        .anyTimes();
    EasyMock.expect(smileRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(smileRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(AUTHENTICATION_RESULT)
        .anyTimes();

    smileRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    EasyMock.expect(smileRequest.getHeader("Accept")).andReturn(SmileMediaTypes.APPLICATION_JACKSON_SMILE).anyTimes();
    EasyMock.expect(smileRequest.getHeader(QueryResource.HEADER_IF_NONE_MATCH)).andReturn(null).anyTimes();
    EasyMock.expect(smileRequest.getRemoteAddr()).andReturn("localhost").anyTimes();

    EasyMock.replay(smileRequest);
    Response response = queryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        smileRequest
    );
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatus());
    Assert.assertEquals(SmileMediaTypes.APPLICATION_JACKSON_SMILE, (response.getMetadata().get("Content-Type").get(0)).toString());
    Assert.assertNotNull(response);
    EasyMock.verify(smileRequest);
  }

  @Test
  public void testBadQuery() throws IOException
  {
    EasyMock.replay(testServletRequest);
    Response response = queryResource.doPost(
        new ByteArrayInputStream("Meka Leka Hi Meka Hiney Ho".getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    QueryException e = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(BadJsonQueryException.ERROR_CODE, e.getErrorCode());
    Assert.assertEquals(BadJsonQueryException.ERROR_CLASS, e.getErrorClass());
  }

  @Test
  public void testResourceLimitExceeded() throws IOException
  {
    ByteArrayInputStream badQuery = EasyMock.createMock(ByteArrayInputStream.class);
    EasyMock.expect(badQuery.read(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
            .andThrow(new ResourceLimitExceededException("You require too much of something"));
    EasyMock.replay(badQuery, testServletRequest);
    Response response = queryResource.doPost(
        badQuery,
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    QueryException e = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(ResourceLimitExceededException.ERROR_CODE, e.getErrorCode());
    Assert.assertEquals(ResourceLimitExceededException.class.getName(), e.getErrorClass());
  }

  @Test
  public void testUnsupportedQueryThrowsException() throws IOException
  {
    String errorMessage = "This will be support in Druid 9999";
    ByteArrayInputStream badQuery = EasyMock.createMock(ByteArrayInputStream.class);
    EasyMock.expect(badQuery.read(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt())).andThrow(
        new QueryUnsupportedException(errorMessage));
    EasyMock.replay(badQuery);
    EasyMock.replay(testServletRequest);
    Response response = queryResource.doPost(
        badQuery,
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(QueryUnsupportedException.STATUS_CODE, response.getStatus());
    QueryException ex = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryException.class);
    Assert.assertEquals(errorMessage, ex.getMessage());
    Assert.assertEquals(QueryUnsupportedException.ERROR_CODE, ex.getErrorCode());
  }

  @Test
  public void testSecuredQuery() throws Exception
  {
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AUTHENTICATION_RESULT)
            .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().times(1);

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(testServletRequest);

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
        JSON_MAPPER,
        JSON_MAPPER,
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
          testServletRequest
      );
      Assert.fail("doPost did not throw ForbiddenException for an unauthorized query");
    }
    catch (ForbiddenException e) {
    }

    Response response = queryResource.doPost(
        new ByteArrayInputStream("{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\"}".getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ((StreamingOutput) response.getEntity()).write(baos);
    final List<Result<TimeBoundaryResultValue>> responses = JSON_MAPPER.readValue(
        baos.toByteArray(),
        new TypeReference<List<Result<TimeBoundaryResultValue>>>() {}
    );

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
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
        JSON_MAPPER,
        JSON_MAPPER,
        queryScheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );
    expectPermissiveHappyPathAuth();
    Response response = timeoutQueryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(QueryTimeoutException.STATUS_CODE, response.getStatus());
    QueryTimeoutException ex;
    try {
      ex = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryTimeoutException.class);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    Assert.assertEquals("Query Timed Out!", ex.getMessage());
    Assert.assertEquals(QueryTimeoutException.ERROR_CODE, ex.getErrorCode());
    Assert.assertEquals(1, timeoutQueryResource.getTimedOutQueryCount());

  }

  @Test(timeout = 60_000L)
  public void testSecuredCancelQuery() throws Exception
  {
    final CountDownLatch waitForCancellationLatch = new CountDownLatch(1);
    final CountDownLatch waitFinishLatch = new CountDownLatch(2);
    final CountDownLatch startAwaitLatch = new CountDownLatch(1);
    final CountDownLatch cancelledCountDownLatch = new CountDownLatch(1);

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AUTHENTICATION_RESULT)
            .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(testServletRequest);

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
                throw new RuntimeException(e);
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
        JSON_MAPPER,
        JSON_MAPPER,
        queryScheduler,
        new AuthConfig(),
        authMapper,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    final String queryString = "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\","
                               + "\"context\":{\"queryId\":\"id_1\"}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    Query query = mapper.readValue(queryString, Query.class);

    ListenableFuture future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_resource_%s")
    ).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              Response response = queryResource.doPost(
                  new ByteArrayInputStream(queryString.getBytes(StandardCharsets.UTF_8)),
                  null,
                  testServletRequest
              );

              Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
            waitFinishLatch.countDown();
          }
        }
    );

    queryScheduler.registerQueryFuture(query, future);
    startAwaitLatch.await();

    Executors.newSingleThreadExecutor().submit(
        new Runnable() {
          @Override
          public void run()
          {
            Response response = queryResource.cancelQuery("id_1", testServletRequest);
            Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
            waitForCancellationLatch.countDown();
            waitFinishLatch.countDown();
          }
        }
    );
    waitFinishLatch.await();
    cancelledCountDownLatch.await();
  }

  @Test(timeout = 60_000L)
  public void testDenySecuredCancelQuery() throws Exception
  {
    final CountDownLatch waitForCancellationLatch = new CountDownLatch(1);
    final CountDownLatch waitFinishLatch = new CountDownLatch(2);
    final CountDownLatch startAwaitLatch = new CountDownLatch(1);

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(AUTHENTICATION_RESULT)
            .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(testServletRequest);

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
        JSON_MAPPER,
        JSON_MAPPER,
        queryScheduler,
        new AuthConfig(),
        authMapper,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );

    final String queryString = "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\","
                               + "\"context\":{\"queryId\":\"id_1\"}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    Query query = mapper.readValue(queryString, Query.class);

    ListenableFuture future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_resource_%s")
    ).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              startAwaitLatch.countDown();
              Response response = queryResource.doPost(
                  new ByteArrayInputStream(queryString.getBytes(StandardCharsets.UTF_8)),
                  null,
                  testServletRequest
              );
              Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
            waitFinishLatch.countDown();
          }
        }
    );

    queryScheduler.registerQueryFuture(query, future);
    startAwaitLatch.await();

    Executors.newSingleThreadExecutor().submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              queryResource.cancelQuery("id_1", testServletRequest);
            }
            catch (ForbiddenException e) {
              waitForCancellationLatch.countDown();
              waitFinishLatch.countDown();
            }
          }
        }
    );
    waitFinishLatch.await();
  }

  @Test(timeout = 10_000L)
  public void testTooManyQuery() throws InterruptedException
  {
    expectPermissiveHappyPathAuth();

    final CountDownLatch waitTwoScheduled = new CountDownLatch(2);
    final CountDownLatch waitAllFinished = new CountDownLatch(3);
    final QueryScheduler laningScheduler = new QueryScheduler(
        2,
        ManualQueryPrioritizationStrategy.INSTANCE,
        NoQueryLaningStrategy.INSTANCE,
        new ServerConfig()
    );

    createScheduledQueryResource(laningScheduler, Collections.emptyList(), ImmutableList.of(waitTwoScheduled));
    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY,
        waitAllFinished,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    );
    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY,
        waitAllFinished,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    );
    waitTwoScheduled.await();
    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY,
        waitAllFinished,
        response -> {
          Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;
          try {
            ex = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryCapacityExceededException.class);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          Assert.assertEquals(QueryCapacityExceededException.makeTotalErrorMessage(2), ex.getMessage());
          Assert.assertEquals(QueryCapacityExceededException.ERROR_CODE, ex.getErrorCode());
        }
    );
    waitAllFinished.await();
  }

  @Test(timeout = 10_000L)
  public void testTooManyQueryInLane() throws InterruptedException
  {
    expectPermissiveHappyPathAuth();
    final CountDownLatch waitTwoStarted = new CountDownLatch(2);
    final CountDownLatch waitOneScheduled = new CountDownLatch(1);
    final CountDownLatch waitAllFinished = new CountDownLatch(3);
    final QueryScheduler scheduler = new QueryScheduler(
        40,
        ManualQueryPrioritizationStrategy.INSTANCE,
        new HiLoQueryLaningStrategy(2),
        new ServerConfig()
    );

    createScheduledQueryResource(scheduler, ImmutableList.of(waitTwoStarted), ImmutableList.of(waitOneScheduled));

    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY,
        waitAllFinished,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    );
    waitOneScheduled.await();
    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY,
        waitAllFinished,
        response -> {
          Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;
          try {
            ex = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryCapacityExceededException.class);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          Assert.assertEquals(
              QueryCapacityExceededException.makeLaneErrorMessage(HiLoQueryLaningStrategy.LOW, 1),
              ex.getMessage()
          );
          Assert.assertEquals(QueryCapacityExceededException.ERROR_CODE, ex.getErrorCode());

        }
    );
    waitTwoStarted.await();
    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY,
        waitAllFinished,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    );

    waitAllFinished.await();
  }

  @Test(timeout = 10_000L)
  public void testTooManyQueryInLaneImplicitFromDurationThreshold() throws InterruptedException
  {
    expectPermissiveHappyPathAuth();
    final CountDownLatch waitTwoStarted = new CountDownLatch(2);
    final CountDownLatch waitOneScheduled = new CountDownLatch(1);
    final CountDownLatch waitAllFinished = new CountDownLatch(3);
    final QueryScheduler scheduler = new QueryScheduler(
        40,
        new ThresholdBasedQueryPrioritizationStrategy(null, "P90D", null, null),
        new HiLoQueryLaningStrategy(1),
        new ServerConfig()
    );

    createScheduledQueryResource(scheduler, ImmutableList.of(waitTwoStarted), ImmutableList.of(waitOneScheduled));

    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY,
        waitAllFinished,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    );
    waitOneScheduled.await();
    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY,
        waitAllFinished,
        response -> {
          Assert.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;
          try {
            ex = JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryCapacityExceededException.class);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          Assert.assertEquals(
              QueryCapacityExceededException.makeLaneErrorMessage(HiLoQueryLaningStrategy.LOW, 1),
              ex.getMessage()
          );
          Assert.assertEquals(QueryCapacityExceededException.ERROR_CODE, ex.getErrorCode());
        }
    );
    waitTwoStarted.await();
    assertResponseAndCountdownOrBlockForever(
        SIMPLE_TIMESERIES_QUERY_SMALLISH_INTERVAL,
        waitAllFinished,
        response -> Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    );

    waitAllFinished.await();
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

          return scheduler.run(
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
        JSON_MAPPER,
        JSON_MAPPER,
        scheduler,
        new AuthConfig(),
        null,
        ResponseContextConfig.newConfig(true),
        DRUID_NODE
    );
  }

  private void assertResponseAndCountdownOrBlockForever(String query, CountDownLatch done, Consumer<Response> asserts)
  {
    Executors.newSingleThreadExecutor().submit(() -> {
      try {
        Response response = queryResource.doPost(
            new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)),
            null,
            testServletRequest
        );
        asserts.accept(response);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      done.countDown();
    });
  }

  private void expectPermissiveHappyPathAuth()
  {
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AUTHENTICATION_RESULT)
            .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(testServletRequest);
  }
}
