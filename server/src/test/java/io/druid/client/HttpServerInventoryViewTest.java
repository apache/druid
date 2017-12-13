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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.discovery.DataNodeService;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.RE;
import io.druid.segment.TestHelper;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestHistory;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.server.coordination.SegmentChangeRequestsSnapshot;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
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
 */
public class HttpServerInventoryViewTest
{
  @Test(timeout = 10000)
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

    TestHttpClient httpClient = new TestHttpClient(
        ImmutableList.of(
            Futures.immediateFuture(
                new ByteArrayInputStream(
                    jsonMapper.writeValueAsBytes(
                        new SegmentChangeRequestsSnapshot(
                            false,
                            null,
                            SegmentChangeRequestHistory.Counter.ZERO,
                            ImmutableList.of(
                                new SegmentChangeRequestLoad(segment1)
                            )
                        )
                    )
                )
            ),
            Futures.immediateFuture(
                new ByteArrayInputStream(
                    jsonMapper.writeValueAsBytes(
                        new SegmentChangeRequestsSnapshot(
                            false,
                            null,
                            SegmentChangeRequestHistory.Counter.ZERO,
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
                    jsonMapper.writeValueAsBytes(
                        new SegmentChangeRequestsSnapshot(
                            true,
                            "force reset counter",
                            SegmentChangeRequestHistory.Counter.ZERO,
                            ImmutableList.of()
                        )
                    )
                )
            ),
            Futures.immediateFuture(
                new ByteArrayInputStream(
                    jsonMapper.writeValueAsBytes(
                        new SegmentChangeRequestsSnapshot(
                            false,
                            null,
                            SegmentChangeRequestHistory.Counter.ZERO,
                            ImmutableList.of(
                                new SegmentChangeRequestLoad(segment3),
                                new SegmentChangeRequestLoad(segment4)
                            )
                        )
                    )
                )
            )
        )
    );

    DiscoveryDruidNode druidNode = new DiscoveryDruidNode(
        new DruidNode("service", "host", 8080, null, true, false),
        DruidNodeDiscoveryProvider.NODE_TYPE_HISTORICAL,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0)
        )
    );

    HttpServerInventoryView httpServerInventoryView = new HttpServerInventoryView(
        jsonMapper,
        httpClient,
        druidNodeDiscoveryProvider,
        Predicates.alwaysTrue(),
        new HttpServerInventoryViewConfig(null, null, null)
    );

    CountDownLatch initializeCallback1 = new CountDownLatch(1);

    Map<String, CountDownLatch> segmentAddLathces = ImmutableMap.of(
        segment1.getIdentifier(), new CountDownLatch(1),
        segment2.getIdentifier(), new CountDownLatch(1),
        segment3.getIdentifier(), new CountDownLatch(1),
        segment4.getIdentifier(), new CountDownLatch(1)
    );

    Map<String, CountDownLatch> segmentDropLatches = ImmutableMap.of(
        segment1.getIdentifier(), new CountDownLatch(1),
        segment2.getIdentifier(), new CountDownLatch(1)
    );

    httpServerInventoryView.registerSegmentCallback(
        MoreExecutors.sameThreadExecutor(),
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(
              DruidServerMetadata server, DataSegment segment
          )
          {
            segmentAddLathces.get(segment.getIdentifier()).countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(
              DruidServerMetadata server, DataSegment segment
          )
          {
            segmentDropLatches.get(segment.getIdentifier()).countDown();
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
        MoreExecutors.sameThreadExecutor(),
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
    segmentAddLathces.get(segment1.getIdentifier()).await();
    segmentDropLatches.get(segment1.getIdentifier()).await();
    segmentAddLathces.get(segment2.getIdentifier()).await();
    segmentAddLathces.get(segment3.getIdentifier()).await();
    segmentAddLathces.get(segment4.getIdentifier()).await();
    segmentDropLatches.get(segment2.getIdentifier()).await();

    DruidServer druidServer = httpServerInventoryView.getInventoryValue("host:8080");
    Assert.assertEquals(ImmutableMap.of(segment3.getIdentifier(), segment3, segment4.getIdentifier(), segment4),
                        druidServer.getSegments());

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
        Request request, HttpResponseHandler<Intermediate, Final> httpResponseHandler
    )
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request, HttpResponseHandler<Intermediate, Final> httpResponseHandler, Duration duration
    )
    {
      if (requestNum.getAndIncrement() == 0) {
        //fail first request immediately
        throw new RuntimeException("simulating couldn't send request to server for some reason.");
      }

      if (requestNum.get() == 2) {
        //fail scenario where request is sent to server but we got an unexpected response.
        HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        httpResponse.setContent(ChannelBuffers.buffer(0));
        httpResponseHandler.handleResponse(httpResponse);
        return Futures.immediateFailedFuture(new RuntimeException("server error"));
      }

      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      httpResponse.setContent(ChannelBuffers.buffer(0));
      httpResponseHandler.handleResponse(httpResponse);
      try {
        return results.take();
      }
      catch (InterruptedException ex) {
        throw new RE(ex, "Interrupted.");
      }
    }
  }
}
