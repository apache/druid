/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.ConnectionCountServerSelectorStrategy;
import io.druid.client.selector.HighestPriorityTierSelectorStrategy;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.java.util.common.StringUtils;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.ReflectionQueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.server.coordination.ServerType;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DirectDruidClientTest
{
  private final Map<String, Object> defaultContext;

  public DirectDruidClientTest()
  {
    defaultContext = new HashMap<>();
    defaultContext.put(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE);
    defaultContext.put(DirectDruidClient.QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());
  }

  @Test
  public void testRun() throws Exception
  {
    HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    final URL url = new URL("http://foo/druid/v2/");

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

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            new Interval("2013-01-01/2013-01-02"),
            new DateTime("2013-01-01").toString(),
            Maps.<String, Object>newHashMap(),
            Lists.<String>newArrayList(),
            Lists.<String>newArrayList(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy())
    );

    DirectDruidClient client1 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new DefaultObjectMapper(),
        httpClient,
        "http",
        "foo",
        new NoopServiceEmitter()
    );
    DirectDruidClient client2 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new DefaultObjectMapper(),
        httpClient,
        "http",
        "foo2",
        new NoopServiceEmitter()
    );

    QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client1
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer1, serverSelector.getSegment());
    QueryableDruidServer queryableDruidServer2 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client2
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer2, serverSelector.getSegment());

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();

    Sequence s1 = client1.run(query, defaultContext);
    Assert.assertTrue(capturedRequest.hasCaptured());
    Assert.assertEquals(url, capturedRequest.getValue().getUrl());
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(1, client1.getNumOpenConnections());

    // simulate read timeout
    Sequence s2 = client1.run(query, defaultContext);
    Assert.assertEquals(2, client1.getNumOpenConnections());
    futureException.setException(new ReadTimeoutException());
    Assert.assertEquals(1, client1.getNumOpenConnections());

    // subsequent connections should work
    Sequence s3 = client1.run(query, defaultContext);
    Sequence s4 = client1.run(query, defaultContext);
    Sequence s5 = client1.run(query, defaultContext);

    Assert.assertTrue(client1.getNumOpenConnections() == 4);

    // produce result for first connection
    futureResult.set(
        new ByteArrayInputStream(
            StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\", \"result\": 42.0}]")
        )
    );
    List<Result> results = Sequences.toList(s1, Lists.<Result>newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(new DateTime("2014-01-01T01:02:03Z"), results.get(0).getTimestamp());
    Assert.assertEquals(3, client1.getNumOpenConnections());

    client2.run(query, defaultContext);
    client2.run(query, defaultContext);

    Assert.assertTrue(client2.getNumOpenConnections() == 2);

    Assert.assertTrue(serverSelector.pick() == queryableDruidServer2);

    EasyMock.verify(httpClient);
  }

  @Test
  public void testCancel() throws Exception
  {
    HttpClient httpClient = EasyMock.createStrictMock(HttpClient.class);

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
            .once();

    EasyMock.replay(httpClient);

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            new Interval("2013-01-01/2013-01-02"),
            new DateTime("2013-01-01").toString(),
            Maps.<String, Object>newHashMap(),
            Lists.<String>newArrayList(),
            Lists.<String>newArrayList(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy())
    );

    DirectDruidClient client1 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new DefaultObjectMapper(),
        httpClient,
        "http",
        "foo",
        new NoopServiceEmitter()
    );

    QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client1
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer1, serverSelector.getSegment());

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    cancellationFuture.set(new StatusResponseHolder(HttpResponseStatus.OK, new StringBuilder("cancelled")));
    Sequence results = client1.run(query, defaultContext);
    Assert.assertEquals(HttpMethod.DELETE, capturedRequest.getValue().getMethod());
    Assert.assertEquals(0, client1.getNumOpenConnections());


    QueryInterruptedException exception = null;
    try {
      Sequences.toList(results, Lists.newArrayList());
    }
    catch (QueryInterruptedException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);

    EasyMock.verify(httpClient);
  }

  @Test
  public void testQueryInterruptionExceptionLogMessage() throws JsonProcessingException
  {
    HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    SettableFuture<Object> interruptionFuture = SettableFuture.create();
    Capture<Request> capturedRequest = EasyMock.newCapture();
    String hostName = "localhost:8080";
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject(),
            EasyMock.anyObject(Duration.class)
        )
    )
            .andReturn(interruptionFuture)
            .anyTimes();

    EasyMock.replay(httpClient);

    DataSegment dataSegment = new DataSegment(
        "test",
        new Interval("2013-01-01/2013-01-02"),
        new DateTime("2013-01-01").toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        0L
    );
    final ServerSelector serverSelector = new ServerSelector( dataSegment
        ,
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy())
    );

    DirectDruidClient client1 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new DefaultObjectMapper(),
        httpClient,
        "http",
        hostName,
        new NoopServiceEmitter()
    );

    QueryableDruidServer queryableDruidServer = new QueryableDruidServer(
        new DruidServer("test1", hostName, null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client1
    );

    serverSelector.addServerAndUpdateSegment(queryableDruidServer, dataSegment);

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    interruptionFuture.set(
        new ByteArrayInputStream(
            StringUtils.toUtf8("{\"error\":\"testing1\",\"errorMessage\":\"testing2\"}")
        )
    );
    Sequence results = client1.run(query, defaultContext);

    QueryInterruptedException actualException = null;
    try {
      Sequences.toList(results, Lists.newArrayList());
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
}
