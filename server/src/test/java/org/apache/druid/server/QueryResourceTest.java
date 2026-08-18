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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.client.BrokerViewOfBrokerConfig;
import org.apache.druid.common.exception.ErrorResponseTransformStrategy;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.BadJsonQueryException;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TruncatedResponseContextException;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.timeboundary.TimeBoundaryResultValue;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.broker.QueryConfigSnapshot;
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
import org.assertj.core.api.AssertionsForClassTypes;
import org.eclipse.jetty.http.HttpHeader;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryResourceTest
{
  private static final DefaultQueryRunnerFactoryConglomerate CONGLOMERATE = DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(
      ImmutableMap.of());
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

  private static final String SIMPLE_TIMESERIES_QUERY_WRITE_EXCEPTION_AS_ROW =
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
      + "    \"context\": { \"writeExceptionBodyAsResponseRow\": \"true\" }"
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
  private StubServiceEmitter emitter;

  @BeforeAll
  public static void staticSetup()
  {
    EmittingLogger.registerEmitter(NOOP_SERVICE_EMITTER);
  }

  @BeforeEach
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
    emitter = StubServiceEmitter.createStarted();
    queryResource = createQueryResource(ResponseContextConfig.newConfig(true));
  }

  private QueryLifecycleFactory createQueryLifecycleFactory()
  {
    return new QueryLifecycleFactory(
        CONGLOMERATE,
        TEST_SEGMENT_WALKER,
        new DefaultGenericQueryMetricsFactory(),
        emitter,
        testRequestLogger,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new DefaultQueryConfig(Map.of()),
        null
    );
  }

  private QueryResource createQueryResource(ResponseContextConfig responseContextConfig)
  {
    return createQueryResource(
        createQueryLifecycleFactory(),
        null,
        queryScheduler,
        responseContextConfig,
        smileMapper,
        new ServerConfig()
    );
  }

  private QueryResource createQueryResource(QueryLifecycleFactory queryLifecycleFactory)
  {
    return createQueryResource(
        queryLifecycleFactory,
        null,
        queryScheduler,
        ResponseContextConfig.newConfig(true),
        smileMapper,
        new ServerConfig()
    );
  }

  /**
   * Every {@link QueryResource} under test is built here, so a change to its constructor touches one call site.
   *
   * @param responseMapper mapper backing the response writer, i.e. the smile mapper unless the test needs json
   */
  private QueryResource createQueryResource(
      final QueryLifecycleFactory queryLifecycleFactory,
      @Nullable final AuthorizerMapper authorizerMapper,
      final QueryScheduler queryScheduler,
      final ResponseContextConfig responseContextConfig,
      final ObjectMapper responseMapper,
      final ServerConfig serverConfig
  )
  {
    return new QueryResource(
        queryLifecycleFactory,
        jsonMapper,
        queryScheduler,
        authorizerMapper,
        new QueryResourceQueryResultPusherFactory(jsonMapper, responseContextConfig, DRUID_NODE),
        new ResourceIOReaderWriterFactory(jsonMapper, responseMapper),
        serverConfig
    );
  }

  @Test
  public void testGoodQuery() throws IOException
  {
    expectPermissiveHappyPathAuth();

    HttpServletResponse servletResponse = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assertions.assertEquals(200, servletResponse.getStatus());
    Assertions.assertTrue(servletResponse.containsHeader(HttpHeader.TRAILER.toString()));

    final Map<String, String> fields = servletResponse.getTrailerFields().get();
    Assertions.assertFalse(fields.containsKey(QueryResource.ERROR_MESSAGE_TRAILER_HEADER));

    Assertions.assertTrue(fields.containsKey(QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER));
    Assertions.assertEquals(fields.get(QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER), "true");
  }

  @Test
  public void testGoodQueryWithQueryConfigOverrideDefault() throws IOException
  {
    final String overrideConfigKey = "priority";
    final String overrideConfigValue = "678";
    DefaultQueryConfig overrideConfig = new DefaultQueryConfig(ImmutableMap.of(overrideConfigKey, overrideConfigValue));
    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            overrideConfig,
            null
        )
    );

    expectPermissiveHappyPathAuth();

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    final List<Result<TimeBoundaryResultValue>> responses = jsonMapper.readValue(
        response.baos.toByteArray(),
        new TypeReference<>()
        {
        }
    );

    Assertions.assertEquals(0, responses.size());
    Assertions.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assertions.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assertions.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assertions.assertTrue(testRequestLogger.getNativeQuerylogs()
                                       .get(0)
                                       .getQuery()
                                       .getContext()
                                       .containsKey(overrideConfigKey));
    Assertions.assertEquals(
        overrideConfigValue,
        testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey)
    );
  }

  /**
   * A {@link QueryResource} whose walker throws once the query is already executing, so the failure surfaces through
   * {@link QueryResultPusher} rather than before it.
   */
  private QueryResource createQueryResourceFailingInExecute(final DefaultQueryConfig queryConfig)
  {
    return createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
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
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            queryConfig,
            null
        )
    );
  }

  @Test
  public void testGoodQueryThrowsDruidExceptionFromLifecycleExecute() throws IOException
  {
    String overrideConfigKey = "priority";
    String overrideConfigValue = "678";
    DefaultQueryConfig overrideConfig = new DefaultQueryConfig(ImmutableMap.of(overrideConfigKey, overrideConfigValue));
    queryResource = createQueryResourceFailingInExecute(overrideConfig);

    expectPermissiveHappyPathAuth();

    final Response response = expectSynchronousRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assertions.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(500, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));

    final ErrorResponse entity = (ErrorResponse) response.getEntity();
    DruidExceptionMatcher.assertThat(
        entity.getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.OPERATOR,
            DruidException.Category.RUNTIME_FAILURE,
            "general"
        ).expectMessageIs("failing for coverage!")
    );

    Assertions.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assertions.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assertions.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assertions.assertTrue(testRequestLogger.getNativeQuerylogs()
                                       .get(0)
                                       .getQuery()
                                       .getContext()
                                       .containsKey(overrideConfigKey));
    Assertions.assertEquals(
        overrideConfigValue,
        testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey)
    );
  }

  @Test
  public void testResponseWithIncludeTrailerHeader() throws IOException
  {
    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            new QuerySegmentWalker()
            {
              @Override
              public <T> QueryRunner<T> getQueryRunnerForIntervals(
                  Query<T> query,
                  Iterable<Interval> intervals
              )
              {
                return (queryPlus, responseContext) -> new Sequence<T>()
                {
                  @Override
                  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
                  {
                    if (accumulator instanceof QueryResultPusher.StreamingHttpResponseAccumulator) {
                      try {
                        ((QueryResultPusher.StreamingHttpResponseAccumulator) accumulator).flush(); // initialized
                      }
                      catch (IOException ignore) {
                      }
                    }

                    throw new QueryTimeoutException();
                  }

                  @Override
                  public <OutType> Yielder<OutType> toYielder(
                      OutType initValue,
                      YieldingAccumulator<OutType, T> accumulator
                  )
                  {
                    return Yielders.done(initValue, null);
                  }
                };
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
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new DefaultQueryConfig(Map.of()),
            null
        )
    );

    expectPermissiveHappyPathAuth();

    HttpServletResponse response = expectAsyncRequestFlow(testServletRequest, SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8), queryResource);

    Assertions.assertTrue(response.containsHeader(HttpHeader.TRAILER.toString()));
    Assertions.assertEquals(response.getHeader(HttpHeader.TRAILER.toString()), QueryResultPusher.RESULT_TRAILER_HEADERS);

    final Map<String, String> fields = response.getTrailerFields().get();
    Assertions.assertTrue(fields.containsKey(QueryResource.ERROR_MESSAGE_TRAILER_HEADER));
    Assertions.assertEquals(
        fields.get(QueryResource.ERROR_MESSAGE_TRAILER_HEADER),
        "Query did not complete within configured timeout period. You can increase query timeout or tune the performance of query."
    );

    Assertions.assertTrue(fields.containsKey(QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER));
    Assertions.assertEquals(fields.get(QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER), "false");

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(504, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testResponseWithMidFlightExceptions() throws IOException
  {
    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            new QuerySegmentWalker()
            {
              @Override
              public <T> QueryRunner<T> getQueryRunnerForIntervals(
                  Query<T> query,
                  Iterable<Interval> intervals
              )
              {
                return (queryPlus, responseContext) -> new Sequence<T>()
                {
                  @Override
                  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
                  {
                    accumulator.accumulate(null,
                                           (T) new TimeBoundaryResultValue(ImmutableMap.<String, Object>of("maxTime", DateTimes.of("2014-08-02")))
                    );
                    throw InvalidInput.exception("mid-flight exception");
                  }

                  @Override
                  public <OutType> Yielder<OutType> toYielder(
                      OutType initValue,
                      YieldingAccumulator<OutType, T> accumulator
                  )
                  {
                    throw new UnsupportedOperationException("not implemented");
                  }
                };
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
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new DefaultQueryConfig(Map.of()),
            null
        )
    );

    expectPermissiveHappyPathAuth();

    MockHttpServletResponse response = expectAsyncRequestFlow(testServletRequest,
                                                              SIMPLE_TIMESERIES_QUERY_WRITE_EXCEPTION_AS_ROW.getBytes(
                                                                  StandardCharsets.UTF_8),
                                                              queryResource
    );
    String actualOutput = response.baos.toString(Charset.defaultCharset());
    Assertions.assertEquals(
        "[{\"maxTime\":\"2014-08-02T00:00:00.000Z\"},{\"error\":\"druidException\","
        + "\"errorCode\":\"invalidInput\",\"persona\":\"USER\",\"category\":\"INVALID_INPUT\","
        + "\"errorMessage\":\"mid-flight exception\",\"context\":{}}]",
        actualOutput
    );
    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(400, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testResponseContextContainsMissingSegments_whenLastSegmentIsMissing() throws IOException
  {
    final SegmentDescriptor missingSegDesc = new SegmentDescriptor(
        Intervals.of("2025-01-01/P1D"), "0", 1
    );

    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            new QuerySegmentWalker()
            {
              @Override
              public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
              {
                return (queryPlus, responseContext) -> new BaseSequence<>(
                    new BaseSequence.IteratorMaker<T, Iterator<T>>() {
                      @Override
                      public Iterator<T> make()
                      {
                        List<T> data = Collections.singletonList((T) ImmutableMap.of("dummy", 1));
                        Iterator<T> realIterator = data.iterator();

                        return new Iterator<T>() {
                          private boolean done = false;

                          @Override
                          public boolean hasNext()
                          {
                            if (realIterator.hasNext()) {
                              return true;
                            } else if (!done) {
                              // Simulate a segment failure in the end after initialize() has run
                              responseContext.addMissingSegments(ImmutableList.of(missingSegDesc));
                              done = true;
                            }
                            return false;
                          }

                          @Override
                          public T next()
                          {
                            return realIterator.next();
                          }
                        };
                      }

                      @Override
                      public void cleanup(Iterator<T> iterFromMake)
                      {
                      }
                    }
                );
              }

              @Override
              public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
              {
                throw new UnsupportedOperationException();
              }
            },
            new DefaultGenericQueryMetricsFactory(),
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new DefaultQueryConfig(Map.of()),
            null
        )
    );

    expectPermissiveHappyPathAuth();

    HttpServletResponse response = expectAsyncRequestFlow(testServletRequest, SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8), queryResource);

    Assertions.assertTrue(response.containsHeader(HttpHeader.TRAILER.toString()));
    Assertions.assertEquals(QueryResultPusher.RESULT_TRAILER_HEADERS, response.getHeader(HttpHeader.TRAILER.toString()));

    final Map<String, String> fields = response.getTrailerFields().get();
    Assertions.assertTrue(response.containsHeader(QueryResource.HEADER_RESPONSE_CONTEXT));
    Assertions.assertEquals(
        jsonMapper.writeValueAsString(ImmutableMap.of("missingSegments", ImmutableList.of(missingSegDesc))),
        response.getHeader(QueryResource.HEADER_RESPONSE_CONTEXT)
    );

    Assertions.assertTrue(fields.containsKey(QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER));
    Assertions.assertEquals("true", fields.get(QueryResource.RESPONSE_COMPLETE_TRAILER_HEADER));

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
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

    queryResource = createQueryResource(

        new QueryLifecycleFactory(null, null, null, null, null, null, NoopPolicyEnforcer.instance(), null, overrideConfig, null)
        {
          @Override
          public QueryLifecycle factorize()
          {
            return new QueryLifecycle(
                CONGLOMERATE,
                querySegmentWalker,
                new DefaultGenericQueryMetricsFactory(),
                emitter,
                testRequestLogger,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                new AuthConfig(),
                NoopPolicyEnforcer.instance(),
                new QueryConfigSnapshot(overrideConfig.getContext(), null),
                System.currentTimeMillis(),
                System.nanoTime()
            )
            {
              @Override
              public void emitLogsAndMetrics(@Nullable Throwable e, @Nullable String remoteAddress, long bytesWritten)
              {
                super.emitLogsAndMetrics(e, remoteAddress, bytesWritten);
                Assertions.assertTrue(Throwables.getStackTraceAsString(e).contains(embeddedExceptionMessage));
              }
            };
          }
        }
    );

    expectPermissiveHappyPathAuth();

    final Response response = expectSynchronousRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assertions.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

    final ErrorResponse entity = (ErrorResponse) response.getEntity();
    DruidExceptionMatcher.assertThat(
        entity.getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.OPERATOR,
            DruidException.Category.RUNTIME_FAILURE,
            "legacyQueryException"
        ).expectMessageIs("something")
    );
    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(500, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testGoodQueryWithQueryConfigDoesNotOverrideQueryContext() throws IOException
  {
    String overrideConfigKey = "priority";
    String overrideConfigValue = "678";
    DefaultQueryConfig overrideConfig = new DefaultQueryConfig(ImmutableMap.of(overrideConfigKey, overrideConfigValue));
    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            overrideConfig,
            null
        )
    );

    expectPermissiveHappyPathAuth();

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY);

    final List<Result<TimeBoundaryResultValue>> responses = jsonMapper.readValue(
        response.baos.toByteArray(),
        new TypeReference<>()
        {
        }
    );

    Assertions.assertNotNull(response);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(0, responses.size());
    Assertions.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assertions.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery());
    Assertions.assertNotNull(testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext());
    Assertions.assertTrue(testRequestLogger.getNativeQuerylogs()
                                       .get(0)
                                       .getQuery()
                                       .getContext()
                                       .containsKey(overrideConfigKey));
    Assertions.assertEquals(
        -1,
        testRequestLogger.getNativeQuerylogs().get(0).getQuery().getContext().get(overrideConfigKey)
    );
    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
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
    Assertions.assertEquals(1, queryResource.getInterruptedQueryCount());
    Assertions.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatus());
    final String expectedException = new QueryInterruptedException(
        new TruncatedResponseContextException("Serialized response context exceeds the max size[0]"),
        DRUID_NODE.getHostAndPortToUse()
    ).toString();
    Assertions.assertEquals(
        expectedException,
        jsonMapper.readValue(response.baos.toByteArray(), QueryInterruptedException.class).toString()
    );
    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(500, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
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
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(1, queryResource.getSuccessfulQueryCount());
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testGoodQueryWithNullAcceptHeader() throws IOException
  {
    testServletRequest.headers.remove("Accept");
    expectPermissiveHappyPathAuth();

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());
    //since accept header is null, the response content type should be same as the value of 'Content-Type' header
    Assertions.assertEquals(MediaType.APPLICATION_JSON, response.getContentType());

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(1, queryResource.getSuccessfulQueryCount());
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testGoodQueryWithEmptyAcceptHeader() throws IOException
  {
    expectPermissiveHappyPathAuth();
    testServletRequest.headers.put("Accept", "");

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);

    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());
    //since accept header is empty, the response content type should be same as the value of 'Content-Type' header
    Assertions.assertEquals(MediaType.APPLICATION_JSON, response.getContentType());

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(1, queryResource.getSuccessfulQueryCount());
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testGoodQueryWithJsonRequestAndSmileAcceptHeader() throws IOException
  {
    expectPermissiveHappyPathAuth();

    // Set Accept to Smile
    testServletRequest.headers.put("Accept", SmileMediaTypes.APPLICATION_JACKSON_SMILE);

    final MockHttpServletResponse response = expectAsyncRequestFlow(SIMPLE_TIMESERIES_QUERY);
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());

    // Content-Type in response should be Smile
    Assertions.assertEquals(SmileMediaTypes.APPLICATION_JACKSON_SMILE, response.getContentType());

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(1, queryResource.getSuccessfulQueryCount());
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
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
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());

    // Content-Type in response should be Smile
    Assertions.assertEquals(SmileMediaTypes.APPLICATION_JACKSON_SMILE, response.getContentType());

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(1, queryResource.getSuccessfulQueryCount());
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
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
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());

    // Content-Type in response should default to Content-Type from request
    Assertions.assertEquals(SmileMediaTypes.APPLICATION_JACKSON_SMILE, response.getContentType());

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(1, queryResource.getSuccessfulQueryCount());
    Assertions.assertEquals(200, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testBadQuery() throws IOException
  {
    Response response = queryResource.doPost(
        new ByteArrayInputStream("Meka Leka Hi Meka Hiney Ho".getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );
    Assertions.assertNotNull(response);
    Assertions.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    QueryException e = jsonMapper.readValue((byte[]) response.getEntity(), QueryException.class);
    Assertions.assertEquals(QueryException.JSON_PARSE_ERROR_CODE, e.getErrorCode());
    Assertions.assertEquals(BadJsonQueryException.ERROR_CLASS, e.getErrorClass());
  }

  @Test
  public void testIncompleteQuery() throws IOException
  {
    final Response response = queryResource.doPost(
        new ByteArrayInputStream("{\"queryType\":\"scan\"}".getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );

    Assertions.assertNotNull(response);
    Assertions.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    final QueryException e = jsonMapper.readValue((byte[]) response.getEntity(), QueryException.class);
    Assertions.assertEquals(QueryException.JSON_PARSE_ERROR_CODE, e.getErrorCode());
    Assertions.assertEquals(ValueInstantiationException.class.getName(), e.getErrorClass());
    Assertions.assertEquals("Invalid native query: dataSource can't be null", e.getMessage());
  }

  @Test
  public void testResourceLimitExceeded() throws IOException
  {
    final Response response;
    try (final ExceptionalInputStream inputStream = new ExceptionalInputStream(
        () -> new ResourceLimitExceededException("You require too much of something")
    )) {
      response = queryResource.doPost(
          inputStream,
          null /*pretty*/,
          testServletRequest
      );
    }
    Assertions.assertNotNull(response);
    Assertions.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    QueryException e = jsonMapper.readValue((byte[]) response.getEntity(), QueryException.class);
    Assertions.assertEquals(QueryException.RESOURCE_LIMIT_EXCEEDED_ERROR_CODE, e.getErrorCode());
    Assertions.assertEquals(ResourceLimitExceededException.class.getName(), e.getErrorClass());
  }

  @Test
  public void testUnsupportedQueryThrowsException() throws IOException
  {
    String errorMessage = "This will be support in Druid 9999";
    final Response response;
    try (final ExceptionalInputStream inputStream = new ExceptionalInputStream(
        () -> new QueryUnsupportedException(errorMessage)
    )) {
      response = queryResource.doPost(
          inputStream,
          null /*pretty*/,
          testServletRequest
      );
    }
    Assertions.assertNotNull(response);
    Assertions.assertEquals(QueryUnsupportedException.STATUS_CODE, response.getStatus());
    QueryException ex = jsonMapper.readValue((byte[]) response.getEntity(), QueryException.class);
    Assertions.assertEquals(errorMessage, ex.getMessage());
    Assertions.assertEquals(QueryException.QUERY_UNSUPPORTED_ERROR_CODE, ex.getErrorCode());
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
              return Access.allowWithRestriction(RowFilterPolicy.from(new NullFilter("col", null)));
            } else {
              return new Access(false);
            }
          }

        };
      }
    };

    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            authMapper,
            new DefaultQueryConfig(Map.of()),
            null
        ),
        authMapper,
        queryScheduler,
        ResponseContextConfig.newConfig(true),
        smileMapper,
        new ServerConfig()
    );


    try {
      queryResource.doPost(
          new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
          null /*pretty*/,
          testServletRequest.mimic()
      );
      Assertions.fail("doPost did not throw ForbiddenException for an unauthorized query");
    }
    catch (ForbiddenException e) {
    }

    final MockHttpServletResponse response = expectAsyncRequestFlow(
        "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\"}",
        testServletRequest.mimic()
    );
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    final List<Result<TimeBoundaryResultValue>> responses = jsonMapper.readValue(
        response.baos.toByteArray(),
        new TypeReference<>()
        {
        }
    );

    Assertions.assertEquals(0, responses.size());
    Assertions.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assertions.assertEquals(
        true,
        testRequestLogger.getNativeQuerylogs().get(0).getQueryStats().getStats().get("success")
    );
    Assertions.assertEquals(
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

    final QueryResource timeoutQueryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            timeoutSegmentWalker,
            new DefaultGenericQueryMetricsFactory(),
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new DefaultQueryConfig(Map.of()),
            null
        ),
        null,
        queryScheduler,
        ResponseContextConfig.newConfig(true),
        jsonMapper,
        new ServerConfig()
    );
    expectPermissiveHappyPathAuth();

    final Response response = expectSynchronousRequestFlow(
        testServletRequest,
        SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8),
        timeoutQueryResource
    );
    Assertions.assertEquals(QueryTimeoutException.STATUS_CODE, response.getStatus());

    ErrorResponse entity = (ErrorResponse) response.getEntity();
    DruidExceptionMatcher.assertThat(
        entity.getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.OPERATOR,
            DruidException.Category.TIMEOUT,
            "legacyQueryException"
        ).expectMessageIs(
            "Query did not complete within configured timeout period. You can increase query timeout or tune the performance of query."
        )
    );

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    jsonMapper.writeValue(baos, entity);
    QueryTimeoutException ex = jsonMapper.readValue(baos.toByteArray(), QueryTimeoutException.class);
    Assertions.assertEquals("Query did not complete within configured timeout period. You can " +
                        "increase query timeout or tune the performance of query.", ex.getMessage());
    Assertions.assertEquals(QueryException.QUERY_TIMEOUT_ERROR_CODE, ex.getErrorCode());
    Assertions.assertEquals(1, timeoutQueryResource.getTimedOutQueryCount());

    emitter.verifyEmitted("query/time", 1);
    Assertions.assertEquals(504, emitter.getMetricEvents("query/time").get(0).toMap().get(DruidMetrics.STATUS_CODE));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
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

    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            authMapper,
            new DefaultQueryConfig(Map.of()),
            null
        ),
        authMapper,
        queryScheduler,
        ResponseContextConfig.newConfig(true),
        smileMapper,
        new ServerConfig()
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
          Assertions.assertEquals(Status.ACCEPTED.getStatusCode(), response.getStatus());
          waitForCancellationLatch.countDown();
          waitFinishLatch.countDown();
        }
    );
    waitFinishLatch.await();
    cancelledCountDownLatch.await();

    Assertions.assertTrue(future.isCancelled());
    final Response response = responseFromEndpoint.get();
    Assertions.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
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

    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            new NoopServiceEmitter(),
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            authMapper,
            new DefaultQueryConfig(Map.of()),
            null
        ),
        authMapper,
        queryScheduler,
        ResponseContextConfig.newConfig(true),
        smileMapper,
        new ServerConfig()
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

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), future.get().getStatus());
  }

  @Test
  @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
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
        response -> Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));
    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus())
    ));
    waitTwoScheduled.await();
    back2.add(eventuallyaAssertSynchronousResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> {
          Assertions.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;

          final ErrorResponse entity = (ErrorResponse) response.getEntity();
          DruidExceptionMatcher.assertThat(
              entity.getUnderlyingException(),
              new DruidExceptionMatcher(
                  DruidException.Persona.OPERATOR,
                  DruidException.Category.CAPACITY_EXCEEDED,
                  "legacyQueryException"
              ).expectMessageIs(
                  "Too many concurrent queries, total query capacity of 2 exceeded. Please try your query again later."
              )
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
          Assertions.assertEquals(QueryCapacityExceededException.makeTotalErrorMessage(2), ex.getMessage());
          Assertions.assertEquals(QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE, ex.getErrorCode());
        }
    ));

    for (Future<Boolean> theFuture : back2) {
      Assertions.assertTrue(theFuture.get());
    }
    Assertions.assertEquals(2, queryResource.getSuccessfulQueryCount());
    Assertions.assertEquals(1, queryResource.getFailedQueryCount());

    emitter.verifyEmitted("query/time", 3);
    Map<Integer, Long> codeFrequencies = emitter.getMetricEvents("query/time").stream()
                                                .map(ServiceMetricEvent::toMap)
                                                .map(map -> (int) map.get(DruidMetrics.STATUS_CODE))
                                                .collect(Collectors.groupingBy(
                                                    code -> code,
                                                    Collectors.counting()
                                                ));
    Assertions.assertEquals(Map.of(200, 2L, 429, 1L), codeFrequencies);
  }

  @Test
  @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
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
        response -> Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));
    waitOneScheduled.await();
    back2.add(eventuallyaAssertSynchronousResponse(
        SIMPLE_TIMESERIES_QUERY_LOW_PRIORITY,
        response -> {
          Assertions.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;

          final ErrorResponse entity = (ErrorResponse) response.getEntity();
          DruidExceptionMatcher.assertThat(
              entity.getUnderlyingException(),
              new DruidExceptionMatcher(
                  DruidException.Persona.OPERATOR,
                  DruidException.Category.CAPACITY_EXCEEDED,
                  "legacyQueryException"
              ).expectMessageIs(
                  "Too many concurrent queries for lane 'low', query capacity of 1 exceeded. Please try your query again later."
              )
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
          Assertions.assertEquals(
              QueryCapacityExceededException.makeLaneErrorMessage(HiLoQueryLaningStrategy.LOW, 1),
              ex.getMessage()
          );
          Assertions.assertEquals(QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE, ex.getErrorCode());

        }
    ));
    waitTwoStarted.await();
    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));

    for (Future<Boolean> theFuture : back2) {
      Assertions.assertTrue(theFuture.get());
    }

    emitter.verifyEmitted("query/time", 3);
    Map<Integer, Long> codeFrequencies = emitter.getMetricEvents("query/time").stream()
                                                .map(ServiceMetricEvent::toMap)
                                                .map(map -> (int) map.get(DruidMetrics.STATUS_CODE))
                                                .collect(Collectors.groupingBy(
                                                    code -> code,
                                                    Collectors.counting()
                                                ));
    Assertions.assertEquals(Map.of(200, 2L, 429, 1L), codeFrequencies);
  }

  @Test
  @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
  public void testTooManyQueryInLaneImplicitFromDurationThreshold() throws InterruptedException, ExecutionException
  {
    expectPermissiveHappyPathAuth();
    final CountDownLatch waitTwoStarted = new CountDownLatch(2);
    final CountDownLatch waitOneScheduled = new CountDownLatch(1);
    final QueryScheduler scheduler = new QueryScheduler(
        40,
        new ThresholdBasedQueryPrioritizationStrategy(null, "P90D", null, null, null),
        new HiLoQueryLaningStrategy(1),
        new ServerConfig()
    );

    ArrayList<Future<Boolean>> back2 = new ArrayList<>();
    createScheduledQueryResource(scheduler, ImmutableList.of(waitTwoStarted), ImmutableList.of(waitOneScheduled));

    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));
    waitOneScheduled.await();
    back2.add(eventuallyaAssertSynchronousResponse(
        SIMPLE_TIMESERIES_QUERY,
        response -> {
          Assertions.assertEquals(QueryCapacityExceededException.STATUS_CODE, response.getStatus());
          QueryCapacityExceededException ex;

          final ErrorResponse entity = (ErrorResponse) response.getEntity();
          DruidExceptionMatcher.assertThat(
              entity.getUnderlyingException(),
              new DruidExceptionMatcher(
                  DruidException.Persona.OPERATOR,
                  DruidException.Category.CAPACITY_EXCEEDED,
                  "legacyQueryException"
              ).expectMessageIs(
                  "Too many concurrent queries for lane 'low', query capacity of 1 exceeded. Please try your query again later."
              )
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
          Assertions.assertEquals(
              QueryCapacityExceededException.makeLaneErrorMessage(HiLoQueryLaningStrategy.LOW, 1),
              ex.getMessage()
          );
          Assertions.assertEquals(QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE, ex.getErrorCode());
        }
    ));
    waitTwoStarted.await();
    back2.add(eventuallyAssertAsyncResponse(
        SIMPLE_TIMESERIES_QUERY_SMALLISH_INTERVAL,
        response -> Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus())
    ));

    for (Future<Boolean> theFuture : back2) {
      Assertions.assertTrue(theFuture.get());
    }
    emitter.verifyEmitted("query/time", 3);
    Map<Integer, Long> codeFrequencies = emitter.getMetricEvents("query/time").stream()
                                                .map(ServiceMetricEvent::toMap)
                                                .map(map -> (int) map.get(DruidMetrics.STATUS_CODE))
                                                .collect(Collectors.groupingBy(
                                                    code -> code,
                                                    Collectors.counting()
                                                ));
    Assertions.assertEquals(Map.of(200, 2L, 429, 1L), codeFrequencies);
  }

  @Test
  public void testNativeQueryWriter_goodResponse() throws IOException
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final QueryResultPusher.Writer writer = new QueryResource.NativeQueryWriter(jsonMapper, baos);
    writer.writeResponseStart();
    writer.writeRow(Arrays.asList("foo", "bar"));
    writer.writeRow(Collections.singletonList("baz"));
    writer.writeResponseEnd();
    writer.close();

    Assertions.assertEquals(
        ImmutableList.of(
            ImmutableList.of("foo", "bar"),
            ImmutableList.of("baz")
        ),
        jsonMapper.readValue(baos.toByteArray(), Object.class)
    );
  }

  @Test
  public void testNativeQueryWriter_truncatedResponse() throws IOException
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final QueryResultPusher.Writer writer = new QueryResource.NativeQueryWriter(jsonMapper, baos);
    writer.writeResponseStart();
    writer.writeRow(Arrays.asList("foo", "bar"));
    writer.close(); // Simulate an error that occurs midstream; close writer without calling writeResponseEnd.

    final JsonProcessingException e = Assertions.assertThrows(
        JsonProcessingException.class,
        () -> jsonMapper.readValue(baos.toByteArray(), Object.class)
    );

    AssertionsForClassTypes.assertThat(e).hasMessageContaining("expected close marker for Array");
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

    queryResource = createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            texasRanger,
            new DefaultGenericQueryMetricsFactory(),
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new DefaultQueryConfig(Map.of()),
            null
        ),
        null,
        scheduler,
        ResponseContextConfig.newConfig(true),
        smileMapper,
        new ServerConfig()
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

  @Test
  public void testBlocklistedQueryReturnsForbidden() throws IOException
  {
    expectPermissiveHappyPathAuth();

    final QueryResource blockingQueryResource = createQueryResourceWithBlocklist(
        new ServerConfig(),
        new DefaultQueryBlocklistRule("block-mmx", ImmutableSet.of("mmx_metrics"), null, null)
    );

    final Response response = blockingQueryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );

    Assertions.assertNotNull(response);
    Assertions.assertEquals(Status.FORBIDDEN.getStatusCode(), response.getStatus());
    Assertions.assertNotNull(response.getMetadata().getFirst(QueryResource.QUERY_ID_RESPONSE_HEADER));

    DruidExceptionMatcher.assertThat(
        ((ErrorResponse) response.getEntity()).getUnderlyingException(),
        DruidExceptionMatcher.forbidden().expectMessageContains("blocked by rule[block-mmx]")
    );

    // Blocked queries are still recorded in metrics and the request log. FORBIDDEN maps to no query counter, the
    // same as when the exception surfaces through QueryResultPusher.
    Assertions.assertEquals(0, blockingQueryResource.getFailedQueryCount());
    Assertions.assertEquals(0, blockingQueryResource.getInterruptedQueryCount());
    Assertions.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    final Map<String, Object> stats = testRequestLogger.getNativeQuerylogs().get(0).getQueryStats().getStats();
    Assertions.assertEquals(false, stats.get("success"));
    // The blocklist throws mid-authorization, so the identity is only present if it was recorded before that point.
    Assertions.assertEquals(AUTHENTICATION_RESULT.getIdentity(), stats.get("identity"));
    Assertions.assertEquals(Status.FORBIDDEN.getStatusCode(), stats.get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testBlocklistedQueryIsSanitizedByErrorResponseTransformStrategy() throws IOException
  {
    expectPermissiveHappyPathAuth();

    // Always-transforming strategy, so the test does not depend on any particular strategy's persona rules.
    final ErrorResponseTransformStrategy strategy = new ErrorResponseTransformStrategy()
    {
      @Override
      public Optional<DruidException> maybeTransform(DruidException exception, Optional<String> errorId)
      {
        return Optional.of(
            DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("sanitized[%s]", errorId.orElse(null))
        );
      }

      @Override
      public Function<String, String> getErrorMessageTransformFunction()
      {
        throw new UnsupportedOperationException();
      }
    };

    final QueryResource blockingQueryResource = createQueryResourceWithBlocklist(
        new ServerConfig(strategy),
        new DefaultQueryBlocklistRule("block-mmx", ImmutableSet.of("mmx_metrics"), null, null)
    );

    final Response response = blockingQueryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );

    Assertions.assertNotNull(response);
    // The transformed exception's own category now drives the status code, not the original FORBIDDEN.
    Assertions.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

    final Object queryId = response.getMetadata().getFirst(QueryResource.QUERY_ID_RESPONSE_HEADER);
    Assertions.assertNotNull(queryId);
    DruidExceptionMatcher.assertThat(
        ((ErrorResponse) response.getEntity()).getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.RUNTIME_FAILURE,
            "general"
        ).expectMessageIs(StringUtils.format("sanitized[%s]", queryId))
    );

    // Sanitization applies to the client response only; the request log keeps the original 403.
    Assertions.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    Assertions.assertEquals(
        Status.FORBIDDEN.getStatusCode(),
        testRequestLogger.getNativeQuerylogs().get(0).getQueryStats().getStats().get(DruidMetrics.STATUS_CODE)
    );
  }

  @Test
  public void testDruidExceptionFromAuthorizeIsCountedByCategory() throws IOException
  {
    expectPermissiveHappyPathAuth();

    final QueryLifecycleFactory realFactory = createQueryLifecycleFactory();
    final QueryLifecycleFactory failingFactory = Mockito.mock(QueryLifecycleFactory.class);
    Mockito.when(failingFactory.factorize()).thenAnswer(invocation -> {
      final QueryLifecycle lifecycle = Mockito.spy(realFactory.factorize());
      // DEFENSIVE maps to the "failed" counter, unlike the FORBIDDEN thrown by the blocklist.
      Mockito.doThrow(DruidException.defensive("oh no"))
             .when(lifecycle)
             .authorize(ArgumentMatchers.any(HttpServletRequest.class));
      return lifecycle;
    });

    final QueryResource failingQueryResource = createQueryResource(failingFactory);

    final Response response = failingQueryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );

    Assertions.assertNotNull(response);
    Assertions.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    Assertions.assertEquals(1, failingQueryResource.getFailedQueryCount());
    Assertions.assertEquals(0, failingQueryResource.getInterruptedQueryCount());
  }

  @Test
  public void testQueryExceptionFromAuthorizeIsRecorded() throws IOException
  {
    expectPermissiveHappyPathAuth();

    final QueryLifecycleFactory realFactory = createQueryLifecycleFactory();
    final QueryLifecycleFactory failingFactory = Mockito.mock(QueryLifecycleFactory.class);
    Mockito.when(failingFactory.factorize()).thenAnswer(invocation -> {
      final QueryLifecycle lifecycle = Mockito.spy(realFactory.factorize());
      Mockito.doThrow(QueryCapacityExceededException.withErrorMessageAndResolvedHost("too busy"))
             .when(lifecycle)
             .authorize(ArgumentMatchers.any(HttpServletRequest.class));
      return lifecycle;
    });

    final QueryResource failingQueryResource = createQueryResource(failingFactory);

    final Response response = failingQueryResource.doPost(
        new ByteArrayInputStream(SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8)),
        null /*pretty*/,
        testServletRequest
    );

    // A QueryException keeps its own status and its legacy response body.
    Assertions.assertNotNull(response);
    Assertions.assertEquals(429, response.getStatus());
    AssertionsForClassTypes.assertThat(StringUtils.fromUtf8((byte[]) response.getEntity())).contains("too busy");

    // It now shares the DruidException path, so the failure is recorded server-side and not just returned.
    Assertions.assertEquals(1, failingQueryResource.getFailedQueryCount());
    Assertions.assertEquals(1, testRequestLogger.getNativeQuerylogs().size());
    final Map<String, Object> stats = testRequestLogger.getNativeQuerylogs().get(0).getQueryStats().getStats();
    Assertions.assertEquals(false, stats.get("success"));
    Assertions.assertEquals(429, stats.get(DruidMetrics.STATUS_CODE));
  }

  @Test
  public void testNonBlocklistedQueryIsNotAffectedByBlocklist() throws IOException
  {
    expectPermissiveHappyPathAuth();

    final QueryResource blockingQueryResource = createQueryResourceWithBlocklist(
        new ServerConfig(),
        new DefaultQueryBlocklistRule("block-other", ImmutableSet.of("some_other_datasource"), null, null)
    );

    final MockHttpServletResponse response = expectAsyncRequestFlow(
        testServletRequest,
        SIMPLE_TIMESERIES_QUERY.getBytes(StandardCharsets.UTF_8),
        blockingQueryResource
    );

    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  private QueryResource createQueryResourceWithBlocklist(ServerConfig serverConfig, QueryBlocklistRule... rules)
  {
    final BrokerDynamicConfig dynamicConfig =
        new BrokerDynamicConfig.Builder().withQueryBlocklist(Arrays.asList(rules)).build();
    final BrokerViewOfBrokerConfig brokerViewOfBrokerConfig = Mockito.mock(BrokerViewOfBrokerConfig.class);
    Mockito.when(brokerViewOfBrokerConfig.getDynamicConfig()).thenReturn(dynamicConfig);
    Mockito.when(brokerViewOfBrokerConfig.snapshotForQuery())
           .thenReturn(new QueryConfigSnapshot(Map.of(), dynamicConfig));

    return createQueryResource(
        new QueryLifecycleFactory(
            CONGLOMERATE,
            TEST_SEGMENT_WALKER,
            new DefaultGenericQueryMetricsFactory(),
            emitter,
            testRequestLogger,
            new AuthConfig(),
            NoopPolicyEnforcer.instance(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new DefaultQueryConfig(Map.of()),
            brokerViewOfBrokerConfig
        ),
        null,
        queryScheduler,
        ResponseContextConfig.newConfig(true),
        smileMapper,
        serverConfig
    );
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

    Assertions.assertNull(queryResource.doPost(
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
