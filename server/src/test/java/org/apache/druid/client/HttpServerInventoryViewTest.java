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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class HttpServerInventoryViewTest
{
  @Test(timeout = 60_000L)
  public void testSimple() throws Exception
  {
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    final DataSegment segment1 = new DataSegment(
        "test1", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment2 = new DataSegment(
        "test2", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment3 = new DataSegment(
        "test3", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment4 = new DataSegment(
        "test4", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment5 = new DataSegment(
        "non-loading-datasource", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    TestHttpClient httpClient = new TestHttpClient(
        ImmutableList.of(
            Futures.immediateFuture(
                new ByteArrayInputStream(
                    jsonMapper.writerWithType(HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF).writeValueAsBytes(
                        new ChangeRequestsSnapshot(
                            false,
                            null,
                            ChangeRequestHistory.Counter.ZERO,
                            ImmutableList.of(
                                new SegmentChangeRequestLoad(segment1)
                            )
                        )
                    )
                )
            ),
            Futures.immediateFuture(
                new ByteArrayInputStream(
                    jsonMapper.writerWithType(HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF).writeValueAsBytes(
                        new ChangeRequestsSnapshot(
                            false,
                            null,
                            ChangeRequestHistory.Counter.ZERO,
                            ImmutableList.of(
                                new SegmentChangeRequestDrop(segment1),
                                new SegmentChangeRequestLoad(segment2),
                                new SegmentChangeRequestLoad(segment3)
                            )
                        )
                    )
                )
            ),
            Futures.immediateFuture(
                new ByteArrayInputStream(
                    jsonMapper.writerWithType(HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF).writeValueAsBytes(
                        new ChangeRequestsSnapshot(
                            true,
                            "force reset counter",
                            ChangeRequestHistory.Counter.ZERO,
                            ImmutableList.of()
                        )
                    )
                )
            ),
            Futures.immediateFuture(
                new ByteArrayInputStream(
                    jsonMapper.writerWithType(HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF).writeValueAsBytes(
                        new ChangeRequestsSnapshot(
                            false,
                            null,
                            ChangeRequestHistory.Counter.ZERO,
                            ImmutableList.of(
                                new SegmentChangeRequestLoad(segment3),
                                new SegmentChangeRequestLoad(segment4),
                                new SegmentChangeRequestLoad(segment5)
                            )
                        )
                    )
                )
            )
        )
    );

    DiscoveryDruidNode druidNode = new DiscoveryDruidNode(
        new DruidNode("service", "host", false, 8080, null, true, false),
        NodeType.HISTORICAL,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0)
        )
    );

    HttpServerInventoryView httpServerInventoryView = new HttpServerInventoryView(
        jsonMapper,
        httpClient,
        druidNodeDiscoveryProvider,
        (pair) -> !pair.rhs.getDataSource().equals("non-loading-datasource"),
        new HttpServerInventoryViewConfig(null, null, null)
    );

    CountDownLatch initializeCallback1 = new CountDownLatch(1);

    Map<SegmentId, CountDownLatch> segmentAddLathces = ImmutableMap.of(
        segment1.getId(), new CountDownLatch(1),
        segment2.getId(), new CountDownLatch(1),
        segment3.getId(), new CountDownLatch(1),
        segment4.getId(), new CountDownLatch(1)
    );

    Map<SegmentId, CountDownLatch> segmentDropLatches = ImmutableMap.of(
        segment1.getId(), new CountDownLatch(1),
        segment2.getId(), new CountDownLatch(1)
    );

    httpServerInventoryView.registerSegmentCallback(
        Execs.directExecutor(),
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            segmentAddLathces.get(segment.getId()).countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            segmentDropLatches.get(segment.getId()).countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            initializeCallback1.countDown();
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    final CountDownLatch serverRemovedCalled = new CountDownLatch(1);
    httpServerInventoryView.registerServerRemovedCallback(
        Execs.directExecutor(),
        new ServerView.ServerRemovedCallback()
        {
          @Override
          public ServerView.CallbackAction serverRemoved(DruidServer server)
          {
            if (server.getName().equals("host:8080")) {
              serverRemovedCalled.countDown();
              return ServerView.CallbackAction.CONTINUE;
            } else {
              throw new RE("Unknown server [%s]", server.getName());
            }
          }
        }
    );

    httpServerInventoryView.start();

    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(druidNode));

    initializeCallback1.await();
    segmentAddLathces.get(segment1.getId()).await();
    segmentDropLatches.get(segment1.getId()).await();
    segmentAddLathces.get(segment2.getId()).await();
    segmentAddLathces.get(segment3.getId()).await();
    segmentAddLathces.get(segment4.getId()).await();
    segmentDropLatches.get(segment2.getId()).await();

    DruidServer druidServer = httpServerInventoryView.getInventoryValue("host:8080");
    Assert.assertEquals(
        ImmutableMap.of(segment3.getId(), segment3, segment4.getId(), segment4),
        Maps.uniqueIndex(druidServer.iterateAllSegments(), DataSegment::getId)
    );

    druidNodeDiscovery.listener.nodesRemoved(ImmutableList.of(druidNode));

    serverRemovedCalled.await();
    Assert.assertNull(httpServerInventoryView.getInventoryValue("host:8080"));

    EasyMock.verify(druidNodeDiscoveryProvider);

    httpServerInventoryView.stop();
  }

  private static class TestDruidNodeDiscovery implements DruidNodeDiscovery
  {
    Listener listener;

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public void registerListener(Listener listener)
    {
      listener.nodesAdded(ImmutableList.of());
      listener.nodeViewInitialized();
      this.listener = listener;
    }
  }

  private static class TestHttpClient implements HttpClient
  {
    BlockingQueue<ListenableFuture> results;
    AtomicInteger requestNum = new AtomicInteger(0);

    TestHttpClient(List<ListenableFuture> resultsList)
    {
      results = new LinkedBlockingQueue<>();
      results.addAll(resultsList);
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler
    )
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler,
        Duration duration
    )
    {
      if (requestNum.getAndIncrement() == 0) {
        //fail first request immediately
        throw new RuntimeException("simulating couldn't send request to server for some reason.");
      }

      if (requestNum.get() == 2) {
        //fail scenario where request is sent to server but we got an unexpected response.
        HttpResponse httpResponse = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.INTERNAL_SERVER_ERROR
        );
        httpResponse.setContent(ChannelBuffers.buffer(0));
        httpResponseHandler.handleResponse(httpResponse, null);
        return Futures.immediateFailedFuture(new RuntimeException("server error"));
      }

      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      httpResponse.setContent(ChannelBuffers.buffer(0));
      httpResponseHandler.handleResponse(httpResponse, null);
      try {
        return results.take();
      }
      catch (InterruptedException ex) {
        throw new RE(ex, "Interrupted.");
      }
    }
  }
}
