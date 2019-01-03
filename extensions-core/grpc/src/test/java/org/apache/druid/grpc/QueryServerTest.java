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

package org.apache.druid.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.KnownLength;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryManager;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class QueryServerTest
{
  public static final QuerySegmentWalker testSegmentWalker = new QuerySegmentWalker()
  {
    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
    {
      return (query1, responseContext) -> Sequences.empty();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
    {
      return getQueryRunnerForIntervals(null, null);
    }
  };
  private static final QueryToolChestWarehouse WAREHOUSE = new MapQueryToolChestWarehouse(ImmutableMap.of());
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final GrpcConfig GRPC_CONFIG = new GrpcConfig();

  @BeforeClass
  public static void setUp() throws Exception
  {
    try (final ServerSocket ss = new ServerSocket(0)) {
      ss.setReuseAddress(true);
      GRPC_CONFIG.port = ss.getLocalPort();
    }
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  protected static QueryLifecycleFactory queryLifecycleFactory()
  {
    return new QueryLifecycleFactory(
        WAREHOUSE,
        testSegmentWalker,
        new DefaultGenericQueryMetricsFactory(MAPPER),
        new NoopServiceEmitter(),
        new TestRequestLogger(),
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
  }

  protected static QueryServer queryServer(
      QueryManager queryManager,
      QuerySegmentWalker texasRanger,
      GrpcConfig grpcConfig
  )
  {
    return new QueryServer(
        queryLifecycleFactory(),
        MAPPER,
        queryManager,
        texasRanger,
        grpcConfig,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new DefaultGenericQueryMetricsFactory(MAPPER)
    );
  }

  protected static QueryServer queryServer()
  {
    return queryServer(null, null, GRPC_CONFIG);
  }

  @Test
  public void TestBAIS()
  {
    final byte[] buff = new byte[]{1, 2, 3, 4};
    final QueryServer.ByteArrayInputStream bais = new QueryServer.ByteArrayInputStream(buff);
    Assert.assertEquals(buff.length, bais.available());
    Assert.assertEquals(buff[0], bais.read());
    Assert.assertEquals(buff.length - 1, bais.available());
    Assert.assertTrue(bais instanceof KnownLength);
  }

  @Test
  public void testQueryMarshall()
  {
    final QueryServer qs = queryServer(null, null, GRPC_CONFIG);
    final TopNQuery topNQuery = new TopNQueryBuilder()
        .dataSource("Test datasource")
        .intervals("2017-01-01/2018-01-01")
        .dimension("some dimension")
        .metric("some metric")
        .threshold(1)
        .aggregators(ImmutableList.of(new CountAggregatorFactory("some metric")))
        .build();
    Assert.assertEquals(
        topNQuery,
        qs.queryImpl.QUERY_MARSHALL.parse(qs.queryImpl.QUERY_MARSHALL.stream(() -> topNQuery)).get()
    );
  }

  @Test
  public void testResultMarshall()
  {
    final QueryServer qs = queryServer();
    final Result result = new Result(DateTimes.nowUtc(), ImmutableList.of(ImmutableMap.of("dim", 1)));
    Assert.assertEquals(result, qs.queryImpl.RESULT_MARSHALL.parse(qs.queryImpl.RESULT_MARSHALL.stream(result)));
  }

  @Test
  public void testCancel()
  {
    final String id = "some id";
    final QueryManager qm = EasyMock.createStrictMock(QueryManager.class);
    EasyMock.expect(qm.cancelQuery(EasyMock.eq(id))).andReturn(true).once();
    EasyMock.replay(qm);
    final QueryServer qs = queryServer(qm, null, GRPC_CONFIG);
    final ServerCall<Supplier<Query>, Result> call = EasyMock.createStrictMock(ServerCall.class);
    final Metadata metadata = new Metadata();
    metadata.put(qs.queryImpl.QUERY_ID_KEY, id);
    final ServerCall.Listener<Supplier<Query>> listener = qs.queryImpl.SERVICE_CALL_HANDLER.startCall(call, metadata);
    listener.onCancel();
    EasyMock.verify(qm);
  }

  @Test
  public void testQuery()
  {
    final Result<TopNResultValue> result = new Result<TopNResultValue>(
        DateTimes.nowUtc(),
        new TopNResultValue(ImmutableList.of(ImmutableMap.of(
            "dim",
            1
        )))
    );
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource("Test datasource")
        .intervals("2017-01-01/2018-01-01")
        .dimension("some dimension")
        .metric("some metric")
        .threshold(1)
        .aggregators(ImmutableList.of(new CountAggregatorFactory("some metric")))
        .build();
    final String id = "some id";
    final QuerySegmentWalker walker = EasyMock.createStrictMock(QuerySegmentWalker.class);
    EasyMock.expect(walker.getQueryRunnerForIntervals(
        EasyMock.anyObject(query.getClass()),
        EasyMock.eq(query.getIntervals())
    ))
            .andReturn((queryPlus, responseContext) -> Sequences.simple(ImmutableList.of(result)))
            .once();
    final QueryServer qs = new QueryServer(
        new QueryLifecycleFactory(
            WAREHOUSE,
            walker,
            new DefaultGenericQueryMetricsFactory(MAPPER),
            new NoopServiceEmitter(),
            new TestRequestLogger(),
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER
        ),
        MAPPER,
        null,
        walker,
        GRPC_CONFIG,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new DefaultGenericQueryMetricsFactory(MAPPER)
    );
    final ServerCall<Supplier<Query>, Result> call = EasyMock.createNiceMock(ServerCall.class);
    call.request(EasyMock.eq(1));
    EasyMock.expectLastCall().once();
    EasyMock.expect(call.isCancelled()).andReturn(false).once();
    call.sendMessage(EasyMock.eq(result));
    EasyMock.expectLastCall().once();
    final Metadata metadata = new Metadata();
    metadata.put(qs.queryImpl.QUERY_ID_KEY, id);

    EasyMock.replay(walker, call);
    final ServerCall.Listener<Supplier<Query>> listener = qs.queryImpl.SERVICE_CALL_HANDLER.startCall(call, metadata);
    listener.onMessage(() -> query);
    EasyMock.verify(walker, call);
  }

  @Test
  public void testStartStop()
  {
    final QueryServer qs = queryServer();
    qs.start();
    qs.stop();
  }

  @Test
  public void testStartStopStop()
  {
    final QueryServer qs = queryServer();
    qs.start();
    qs.stop();
    qs.stop();
  }

  @Test
  public void testStartStopStartStop()
  {
    final QueryServer qs = queryServer();
    qs.start();
    qs.stop();
    qs.start();
    qs.stop();
  }

  @Test(expected = ISE.class)
  public void testStartStart()
  {
    final QueryServer qs = queryServer();
    qs.start();
    try {
      qs.start();
    }
    finally {
      qs.stop();
    }
  }

  @Test
  public void testServiceIT() throws Exception
  {
    final Result<TopNResultValue> result = new Result<>(
        DateTimes.nowUtc(),
        new TopNResultValue(ImmutableList.of(ImmutableMap.of(
            "dim",
            1
        )))
    );
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource("Test datasource")
        .intervals("2017-01-01/2018-01-01")
        .dimension("some dimension")
        .metric("some metric")
        .threshold(1)
        .aggregators(ImmutableList.of(new CountAggregatorFactory("some metric")))
        .build();
    final String id = "some id";
    final QuerySegmentWalker walker = EasyMock.createStrictMock(QuerySegmentWalker.class);
    EasyMock.expect(walker.getQueryRunnerForIntervals(
        EasyMock.anyObject(query.getClass()),
        EasyMock.eq(query.getIntervals())
    ))
            .andReturn((queryPlus, responseContext) -> Sequences.simple(ImmutableList.of(result)))
            .once();


    final Result expected = MAPPER.readValue(MAPPER.writeValueAsBytes(result), Result.class);

    EasyMock.replay(walker);

    try (final Closer closer = Closer.create()) {
      final GrpcConfig config = new GrpcConfig();
      final QueryServer qs;
      try (final ServerSocket ss = new ServerSocket(0)) {
        ss.setReuseAddress(true);
        config.port = ss.getLocalPort();
        // I have no idea why this one needs its own config. But using the global config, even with a new port,
        // does *NOT* work
        qs = new QueryServer(
            new QueryLifecycleFactory(
                WAREHOUSE,
                walker,
                new DefaultGenericQueryMetricsFactory(MAPPER),
                new NoopServiceEmitter(),
                new TestRequestLogger(),
                new AuthConfig(),
                AuthTestUtils.TEST_AUTHORIZER_MAPPER
            ),
            MAPPER,
            null,
            walker,
            config,
            new AuthConfig(),
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            new DefaultGenericQueryMetricsFactory(MAPPER)
        );
        // Minimize time between freeing socket and trying to start service
      }
      qs.start();
      closer.register(qs::stop);

      final Metadata metadata = new Metadata();
      metadata.put(qs.queryImpl.QUERY_ID_KEY, id);
      final ManagedChannel channel = ManagedChannelBuilder
          .forAddress("127.0.0.1", config.port)
          .usePlaintext()
          .build();
      closer.register(channel::shutdownNow);
      final QueryStub queryStub = new QueryStub(
          channel,
          CallOptions.DEFAULT.withWaitForReady().withDeadlineAfter(5, TimeUnit.SECONDS),
          qs
      );
      Assert.assertEquals(expected, queryStub.issueQuery(() -> query));
    }
    catch (Exception e) {
      Throwables.throwIfInstanceOf(e, RuntimeException.class);
      throw new RE(e, "test failed");
    }
    EasyMock.verify(walker);
  }

  final class QueryStub extends AbstractStub<QueryStub>
  {
    private final QueryServer qs;

    QueryStub(Channel channel, CallOptions callOptions, QueryServer qs)
    {
      super(channel, callOptions);
      this.qs = qs;
    }

    @Override
    protected QueryStub build(Channel channel, CallOptions callOptions)
    {
      return new QueryStub(channel, callOptions, qs);
    }

    Result issueQuery(Supplier<Query> q)
    {
      return ClientCalls.blockingUnaryCall(getChannel().newCall(
          qs.queryImpl.METHOD_DESCRIPTOR,
          getCallOptions()
      ), q);
    }
  }
}
