/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.RequestBuilder;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.ConnectionCountServerSelectorStrategy;
import io.druid.client.selector.HighestPriorityTierSelectorStrategy;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryWatcher;
import io.druid.query.ReflectionQueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

public class DirectDruidClientTest
{
  @Test
  public void testRun() throws Exception
  {
    HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    RequestBuilder requestBuilder = new RequestBuilder(httpClient, HttpMethod.POST, new URL("http://foo.com"));
    EasyMock.expect(httpClient.post(EasyMock.<URL>anyObject())).andReturn(requestBuilder).atLeastOnce();

    SettableFuture futureException = SettableFuture.create();

    SettableFuture<InputStream> futureResult = SettableFuture.create();
    EasyMock.expect(httpClient.go(EasyMock.<Request>anyObject())).andReturn(futureResult).times(1);
    EasyMock.expect(httpClient.go(EasyMock.<Request>anyObject())).andReturn(futureException).times(1);
    EasyMock.expect(httpClient.go(EasyMock.<Request>anyObject())).andReturn(SettableFuture.create()).atLeastOnce();
    EasyMock.replay(httpClient);

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            new Interval("2013-01-01/2013-01-02"),
            new DateTime("2013-01-01").toString(),
            Maps.<String, Object>newHashMap(),
            Lists.<String>newArrayList(),
            Lists.<String>newArrayList(),
            new NoneShardSpec(),
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
        "foo"
    );
    DirectDruidClient client2 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new DefaultObjectMapper(),
        httpClient,
        "foo2"
    );

    QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", 0, "historical", DruidServer.DEFAULT_TIER, 0),
        client1
    );
    serverSelector.addServer(queryableDruidServer1);
    QueryableDruidServer queryableDruidServer2 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", 0, "historical", DruidServer.DEFAULT_TIER, 0),
        client2
    );
    serverSelector.addServer(queryableDruidServer2);

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();

    Sequence s1 = client1.run(query);
    Assert.assertEquals(1, client1.getNumOpenConnections());

    // simulate read timeout
    Sequence s2 = client1.run(query);
    Assert.assertEquals(2, client1.getNumOpenConnections());
    futureException.setException(new ReadTimeoutException());
    Assert.assertEquals(1, client1.getNumOpenConnections());

    // subsequent connections should work
    Sequence s3 = client1.run(query);
    Sequence s4 = client1.run(query);
    Sequence s5 = client1.run(query);

    Assert.assertTrue(client1.getNumOpenConnections() == 4);

    // produce result for first connection
    futureResult.set(new ByteArrayInputStream("[{\"timestamp\":\"2014-01-01T01:02:03Z\", \"result\": 42.0}]".getBytes()));
    List<Result> results = Sequences.toList(s1, Lists.<Result>newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(new DateTime("2014-01-01T01:02:03Z"), results.get(0).getTimestamp());
    Assert.assertEquals(3, client1.getNumOpenConnections());

    client2.run(query);
    client2.run(query);

    Assert.assertTrue(client2.getNumOpenConnections() == 2);

    Assert.assertTrue(serverSelector.pick() == queryableDruidServer2);

    EasyMock.verify(httpClient);
  }

  @Test
  public void testCancel() throws Exception
  {
    HttpClient httpClient = EasyMock.createStrictMock(HttpClient.class);
        EasyMock.expect(httpClient.post(EasyMock.<URL>anyObject())).andReturn(
            new RequestBuilder(httpClient, HttpMethod.POST, new URL("http://foo.com"))
        ).once();

    ListenableFuture<Object> cancelledFuture = Futures.immediateCancelledFuture();
    EasyMock.expect(httpClient.go(EasyMock.<Request>anyObject())).andReturn(cancelledFuture).once();

    EasyMock.expect(httpClient.delete(EasyMock.<URL>anyObject()))
            .andReturn(new RequestBuilder(httpClient, HttpMethod.DELETE, new URL("http://foo.com/delete")))
            .once();
    SettableFuture<Object> cancellationFuture = SettableFuture.create();
    EasyMock.expect(httpClient.go(EasyMock.<Request>anyObject())).andReturn(cancellationFuture).once();

    EasyMock.replay(httpClient);

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            new Interval("2013-01-01/2013-01-02"),
            new DateTime("2013-01-01").toString(),
            Maps.<String, Object>newHashMap(),
            Lists.<String>newArrayList(),
            Lists.<String>newArrayList(),
            new NoneShardSpec(),
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
        "foo"
    );

    QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", 0, "historical", DruidServer.DEFAULT_TIER, 0),
        client1
    );
    serverSelector.addServer(queryableDruidServer1);

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();

    cancellationFuture.set(new StatusResponseHolder(HttpResponseStatus.OK, new StringBuilder("cancelled")));
    Sequence results = client1.run(query);
    Assert.assertEquals(0, client1.getNumOpenConnections());


    QueryInterruptedException exception = null;
    try {
      Sequences.toList(results, Lists.newArrayList());
    } catch(QueryInterruptedException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);

    EasyMock.verify(httpClient);
  }
}
