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

package org.apache.druid.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.client.selector.ConnectionCountServerSelectorStrategy;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.ReflectionQueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DirectDruidClientTest
{
  private final String hostName = "localhost:8080";

  private final DataSegment dataSegment = new DataSegment(
      "test",
      Intervals.of("2013-01-01/2013-01-02"),
      DateTimes.of("2013-01-01").toString(),
      new HashMap<>(),
      new ArrayList<>(),
      new ArrayList<>(),
      NoneShardSpec.instance(),
      0,
      0L
  );
  private ServerSelector serverSelector;

  private HttpClient httpClient;
  private DirectDruidClient client;
  private QueryableDruidServer queryableDruidServer;
  private ScheduledExecutorService queryCancellationExecutor;

  @Before
  public void setup()
  {
    httpClient = EasyMock.createMock(HttpClient.class);
    serverSelector = new ServerSelector(
        dataSegment,
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy())
    );
    queryCancellationExecutor = Execs.scheduledSingleThreaded("query-cancellation-executor");
    client = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new DefaultObjectMapper(),
        httpClient,
        "http",
        hostName,
        new NoopServiceEmitter(),
        queryCancellationExecutor
    );
    queryableDruidServer = new QueryableDruidServer(
        new DruidServer(
            "test1",
            "localhost",
            null,
            0,
            ServerType.HISTORICAL,
            DruidServer.DEFAULT_TIER,
            0
        ),
        client
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer, serverSelector.getSegment());
  }

  @After
  public void teardown() throws InterruptedException
  {
    queryCancellationExecutor.shutdown();
    queryCancellationExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testRun() throws Exception
  {
    final URL url = new URL(StringUtils.format("http://%s/druid/v2/", hostName));

    SettableFuture<InputStream> futureResult = SettableFuture.create();
    Capture<Request> capturedRequest = EasyMock.newCapture();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject(),
            EasyMock.anyObject(Duration.class)
        )
    )
            .andReturn(futureResult)
            .times(1);

    SettableFuture futureException = SettableFuture.create();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject(),
            EasyMock.anyObject(Duration.class)
        )
    )
            .andReturn(futureException)
            .times(1);

    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject(),
            EasyMock.anyObject(Duration.class)
        )
    )
            .andReturn(SettableFuture.create())
            .atLeastOnce();

    EasyMock.replay(httpClient);

    DirectDruidClient client2 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new DefaultObjectMapper(),
        httpClient,
        "http",
        "foo2",
        new NoopServiceEmitter(),
        queryCancellationExecutor
    );

    QueryableDruidServer queryableDruidServer2 = new QueryableDruidServer(
        new DruidServer(
            "test1",
            "localhost",
            null,
            0,
            ServerType.HISTORICAL,
            DruidServer.DEFAULT_TIER,
            0
        ),
        client2
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer2, serverSelector.getSegment());

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    query = query.withOverriddenContext(ImmutableMap.of(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE));
    Sequence s1 = client.run(QueryPlus.wrap(query));
    Assert.assertTrue(capturedRequest.hasCaptured());
    Assert.assertEquals(url, capturedRequest.getValue().getUrl());
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(1, client.getNumOpenConnections());

    // simulate read timeout
    client.run(QueryPlus.wrap(query));
    Assert.assertEquals(2, client.getNumOpenConnections());
    futureException.setException(new ReadTimeoutException());
    Assert.assertEquals(1, client.getNumOpenConnections());

    // subsequent connections should work
    client.run(QueryPlus.wrap(query));
    client.run(QueryPlus.wrap(query));
    client.run(QueryPlus.wrap(query));

    Assert.assertTrue(client.getNumOpenConnections() == 4);

    // produce result for first connection
    futureResult.set(
        new ByteArrayInputStream(
            StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\", \"result\": 42.0}]")
        )
    );
    List<Result> results = s1.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(DateTimes.of("2014-01-01T01:02:03Z"), results.get(0).getTimestamp());
    Assert.assertEquals(3, client.getNumOpenConnections());

    client2.run(QueryPlus.wrap(query));
    client2.run(QueryPlus.wrap(query));

    Assert.assertEquals(2, client2.getNumOpenConnections());

    Assert.assertEquals(serverSelector.pick(null), queryableDruidServer2);

    EasyMock.verify(httpClient);
  }

  @Test
  public void testCancel()
  {
    Capture<Request> capturedRequest = EasyMock.newCapture();
    ListenableFuture<Object> cancelledFuture = Futures.immediateCancelledFuture();
    SettableFuture<Object> cancellationFuture = SettableFuture.create();

    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject(),
            EasyMock.anyObject(Duration.class)
        )
    )
            .andReturn(cancelledFuture)
            .once();

    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject(),
            EasyMock.anyObject(Duration.class)
        )
    )
            .andReturn(cancellationFuture)
            .anyTimes();

    EasyMock.replay(httpClient);


    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    query = query.withOverriddenContext(ImmutableMap.of(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE));
    cancellationFuture.set(new StatusResponseHolder(HttpResponseStatus.OK, new StringBuilder("cancelled")));
    Sequence results = client.run(QueryPlus.wrap(query));
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(0, client.getNumOpenConnections());


    QueryInterruptedException exception = null;
    try {
      results.toList();
    }
    catch (QueryInterruptedException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);

    EasyMock.verify(httpClient);
  }

  @Test
  public void testQueryInterruptionExceptionLogMessage()
  {
    SettableFuture<Object> interruptionFuture = SettableFuture.create();
    Capture<Request> capturedRequest = EasyMock.newCapture();
    final String hostName = "localhost:8080";
    EasyMock
        .expect(
            httpClient.go(
                EasyMock.capture(capturedRequest),
                EasyMock.<HttpResponseHandler>anyObject(),
                EasyMock.anyObject(Duration.class)
            )
        )
        .andReturn(interruptionFuture)
        .anyTimes();

    EasyMock.replay(httpClient);

    // test error
    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    query = query.withOverriddenContext(ImmutableMap.of(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE));
    interruptionFuture.set(
        new ByteArrayInputStream(
            StringUtils.toUtf8("{\"error\":\"testing1\",\"errorMessage\":\"testing2\"}")
        )
    );
    Sequence results = client.run(QueryPlus.wrap(query));

    QueryInterruptedException actualException = null;
    try {
      results.toList();
    }
    catch (QueryInterruptedException e) {
      actualException = e;
    }
    Assert.assertNotNull(actualException);
    Assert.assertEquals("testing1", actualException.getErrorCode());
    Assert.assertEquals("testing2", actualException.getMessage());
    Assert.assertEquals(hostName, actualException.getHost());
    EasyMock.verify(httpClient);
  }

  @Test
  public void testQueryTimeoutBeforeFuture() throws IOException, InterruptedException
  {
    SettableFuture<Object> timeoutFuture = SettableFuture.create();
    Capture<Request> capturedRequest = EasyMock.newCapture();
    final String queryId = "timeout-before-future";

    EasyMock
        .expect(
            httpClient.go(
                EasyMock.capture(capturedRequest),
                EasyMock.<HttpResponseHandler>anyObject(),
                EasyMock.anyObject(Duration.class)
            )
        )
        .andReturn(timeoutFuture)
        .anyTimes();

    EasyMock.replay(httpClient);

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    query = query.withOverriddenContext(
        ImmutableMap.of(DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 250, "queryId", queryId)
    );

    Sequence results = client.run(QueryPlus.wrap(query));

    // incomplete result set
    PipedInputStream in = new PipedInputStream();
    final PipedOutputStream out = new PipedOutputStream(in);
    timeoutFuture.set(
        in
    );

    QueryTimeoutException actualException = null;
    try {
      out.write(StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\"}"));
      Thread.sleep(250);
      out.write(StringUtils.toUtf8("]"));
      out.close();
      results.toList();
    }
    catch (QueryTimeoutException e) {
      actualException = e;
    }
    Assert.assertNotNull(actualException);
    Assert.assertEquals("Query timeout", actualException.getErrorCode());
    Assert.assertEquals("url[http://localhost:8080/druid/v2/] timed out", actualException.getMessage());
    Assert.assertEquals(hostName, actualException.getHost());
    EasyMock.verify(httpClient);
  }

  @Test
  public void testQueryTimeoutFromFuture()
  {
    SettableFuture<Object> noFuture = SettableFuture.create();
    Capture<Request> capturedRequest = EasyMock.newCapture();
    final String queryId = "never-ending-future";

    EasyMock
        .expect(
            httpClient.go(
                EasyMock.capture(capturedRequest),
                EasyMock.<HttpResponseHandler>anyObject(),
                EasyMock.anyObject(Duration.class)
            )
        )
        .andReturn(noFuture)
        .anyTimes();

    EasyMock.replay(httpClient);

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    query = query.withOverriddenContext(
        ImmutableMap.of(DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 500, "queryId", queryId)
    );

    Sequence results = client.run(QueryPlus.wrap(query));

    QueryTimeoutException actualException = null;
    try {
      results.toList();
    }
    catch (QueryTimeoutException e) {
      actualException = e;
    }
    Assert.assertNotNull(actualException);
    Assert.assertEquals("Query timeout", actualException.getErrorCode());
    Assert.assertEquals(StringUtils.format("Query [%s] timed out!", queryId), actualException.getMessage());
    Assert.assertEquals(hostName, actualException.getHost());
    EasyMock.verify(httpClient);
  }
}
