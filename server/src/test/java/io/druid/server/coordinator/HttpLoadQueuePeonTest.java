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

package io.druid.server.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.HttpResponseHandler;
import io.druid.server.ServerTestHelper;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentLoadDropHandler;
import io.druid.timeline.DataSegment;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class HttpLoadQueuePeonTest
{
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

  final TestDruidCoordinatorConfig config = new TestDruidCoordinatorConfig(
      null,
      null,
      null,
      null,
      null,
      null,
      10,
      null,
      false,
      false,
      Duration.ZERO
  )
  {
    @Override
    public int getHttpLoadQueuePeonBatchSize()
    {
      return 2;
    }
  };

  @Test(timeout = 10000)
  public void testSimple() throws Exception
  {
    HttpLoadQueuePeon httpLoadQueuePeon = new HttpLoadQueuePeon(
        "http://dummy:4000",
        ServerTestHelper.MAPPER,
        new TestHttpClient(),
        config,
        Executors.newScheduledThreadPool(
            2,
            Execs.makeThreadFactory("HttpLoadQueuePeonTest-%s")
        ),
        Execs.singleThreaded("HttpLoadQueuePeonTest")
    );

    httpLoadQueuePeon.start();

    Map<String, CountDownLatch> latches = ImmutableMap.of(
        segment1.getIdentifier(), new CountDownLatch(1),
        segment2.getIdentifier(), new CountDownLatch(1),
        segment3.getIdentifier(), new CountDownLatch(1),
        segment4.getIdentifier(), new CountDownLatch(1)
    );

    httpLoadQueuePeon.dropSegment(segment1, () -> latches.get(segment1.getIdentifier()).countDown());
    httpLoadQueuePeon.loadSegment(segment2, () -> latches.get(segment2.getIdentifier()).countDown());
    httpLoadQueuePeon.dropSegment(segment3, () -> latches.get(segment3.getIdentifier()).countDown());
    httpLoadQueuePeon.loadSegment(segment4, () -> latches.get(segment4.getIdentifier()).countDown());

    latches.get(segment1.getIdentifier()).await();
    latches.get(segment2.getIdentifier()).await();
    latches.get(segment3.getIdentifier()).await();
    latches.get(segment4.getIdentifier()).await();

    httpLoadQueuePeon.stop();
  }

  @Test(timeout = 10000)
  public void testLoadDropAfterStop() throws Exception
  {
    HttpLoadQueuePeon httpLoadQueuePeon = new HttpLoadQueuePeon(
        "http://dummy:4000",
        ServerTestHelper.MAPPER,
        new TestHttpClient(),
        config,
        Executors.newScheduledThreadPool(
            2,
            Execs.makeThreadFactory("HttpLoadQueuePeonTest-%s")
        ),
        Execs.singleThreaded("HttpLoadQueuePeonTest")
    );

    httpLoadQueuePeon.start();

    Map<String, CountDownLatch> latches = ImmutableMap.of(
        segment1.getIdentifier(), new CountDownLatch(1),
        segment2.getIdentifier(), new CountDownLatch(1),
        segment3.getIdentifier(), new CountDownLatch(1),
        segment4.getIdentifier(), new CountDownLatch(1)
    );

    httpLoadQueuePeon.dropSegment(segment1, () -> latches.get(segment1.getIdentifier()).countDown());
    httpLoadQueuePeon.loadSegment(segment2, () -> latches.get(segment2.getIdentifier()).countDown());
    latches.get(segment1.getIdentifier()).await();
    latches.get(segment2.getIdentifier()).await();
    httpLoadQueuePeon.stop();
    httpLoadQueuePeon.dropSegment(segment3, () -> latches.get(segment3.getIdentifier()).countDown());
    httpLoadQueuePeon.loadSegment(segment4, () -> latches.get(segment4.getIdentifier()).countDown());
    latches.get(segment3.getIdentifier()).await();
    latches.get(segment4.getIdentifier()).await();

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
    AtomicInteger requestNum = new AtomicInteger(0);

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
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      httpResponse.setContent(ChannelBuffers.buffer(0));
      httpResponseHandler.handleResponse(httpResponse);
      try {
        List<DataSegmentChangeRequest> changeRequests = ServerTestHelper.MAPPER.readValue(
            request.getContent().array(), new TypeReference<List<DataSegmentChangeRequest>>()
            {
            }
        );

        List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> statuses = new ArrayList<>(changeRequests.size());
        for (DataSegmentChangeRequest cr : changeRequests) {
          statuses.add(new SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus(
              cr,
              SegmentLoadDropHandler.Status.SUCCESS
          ));
        }
        return (ListenableFuture) Futures.immediateFuture(
            new ByteArrayInputStream(
                ServerTestHelper.MAPPER
                    .writerWithType(HttpLoadQueuePeon.RESPONSE_ENTITY_TYPE_REF)
                    .writeValueAsBytes(statuses)
            )
        );
      }
      catch (Exception ex) {
        throw new RE(ex, "Unexpected exception.");
      }
    }
  }
}
