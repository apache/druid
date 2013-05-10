/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.client.selector;

import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.druid.Druids;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DirectDruidClient;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.query.ReflectionQueryToolChestWarehouse;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.RequestBuilder;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

/**
 */
public class ServerSelectorTest
{
  private HttpClient httpClient;

  @Before
  public void setUp() throws Exception
  {
    httpClient = EasyMock.createMock(HttpClient.class);
  }

  @Test
  public void testPick() throws Exception
  {
    RequestBuilder requestBuilder = new RequestBuilder(httpClient, HttpMethod.POST, new URL("http://foo.com"));
    EasyMock.expect(httpClient.post(EasyMock.<URL>anyObject())).andReturn(requestBuilder).atLeastOnce();
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
        )
    );

    DirectDruidClient client1 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        new DefaultObjectMapper(new SmileFactory()),
        httpClient,
        "foo"
    );
    DirectDruidClient client2 = new DirectDruidClient(
        new ReflectionQueryToolChestWarehouse(),
        new DefaultObjectMapper(new SmileFactory()),
        httpClient,
        "foo2"
    );

    QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
        null,
        client1
    );
    serverSelector.addServer(queryableDruidServer1);
    QueryableDruidServer queryableDruidServer2 = new QueryableDruidServer(
        null,
        client2
    );
    serverSelector.addServer(queryableDruidServer2);

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();

    client1.run(query);
    client1.run(query);
    client1.run(query);

    Assert.assertTrue(client1.getNumOpenConnections() == 3);

    client2.run(query);
    client2.run(query);

    Assert.assertTrue(client2.getNumOpenConnections() == 2);

    Assert.assertTrue(serverSelector.pick() == queryableDruidServer2);

    EasyMock.verify(httpClient);
  }
}
